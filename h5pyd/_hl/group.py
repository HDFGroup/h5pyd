##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

from __future__ import absolute_import

import six
import os.path as op
import numpy
import collections

from .base import HLObject, MutableMappingHDF5, phil
from .objectid import TypeID, GroupID, DatasetID
from . import dataset
from .dataset import Dataset
from .datatype import Datatype
from . import h5type


class Group(HLObject, MutableMappingHDF5):

    """ Represents an HDF5 group.
    """

    def __init__(self, bind, **kwargs):
        #print "group init, bind:", bind

        """ Create a new Group object by binding to a low-level GroupID.
        """

        with phil:
            if not isinstance(bind, GroupID):
                raise ValueError("%s is not a GroupID" % bind)
            HLObject.__init__(self, bind, **kwargs)
            self._req_prefix = "/groups/" + self.id.uuid 
            self._link_db = {}  # cache for links

    def create_group(self, h5path):
        """ Create and return a new subgroup.

        Name may be absolute or relative.  Fails if the target name already
        exists.
        """
        if self.id.http_conn.mode == 'r':
            raise ValueError("Unable to create group (No write intent on file)")

        #if self.__contains__(name):
        #    raise ValueError("Unable to create link (Name alredy exists)")
        if h5path[-1] == '/':
            raise ValueError("Invalid path for create_group")
        
        parent_uuid = self.id.id
        if h5path[0] == '/':
            # absolute path         
            parent_uuid = self.file.id.id   # uuid of root group

        parent_path = op.dirname(h5path)
        title = op.basename(h5path)

        if parent_path:
            parent_uuid, link_json = self.get_link_json(parent_path)
            if link_json["class"] != 'H5L_TYPE_HARD':
                # TBD: get the referenced object for softlink?
                raise IOError("cannot create subgroup of softlink")
            parent_uuid = link_json["id"]

         
        body = {'link': { 'id': parent_uuid,
                          'name': title
               } }
         
        rsp = self.POST('/groups', body=body)

        group_json = rsp 
        groupId = GroupID(self, group_json)

        subGroup = Group(groupId)
        if self._name:
            if self._name[-1] == '/':
                subGroup._name = self._name + title
            else:
                subGroup._name = self._name + '/' + title
        return subGroup

    def create_dataset(self, name, shape=None, dtype=None, data=None, **kwds):
        """ Create a new HDF5 dataset

        name
            Name of the dataset (absolute or relative).  Provide None to make
            an anonymous dataset.
        shape
            Dataset shape.  Use "()" for scalar datasets.  Required if "data"
            isn't provided.
        dtype
            Numpy dtype or string.  If omitted, dtype('f') will be used.
            Required if "data" isn't provided; otherwise, overrides data
            array's dtype.
        data
            Provide data to initialize the dataset.  If used, you can omit
            shape and dtype arguments.

        Keyword-only arguments:

        chunks
            (Tuple) Chunk shape, or True to enable auto-chunking.
        maxshape
            (Tuple) Make the dataset resizable up to this shape.  Use None for
            axes you want to be unlimited.
        compression
            (String or int) Compression strategy.  Legal values are 'gzip',
            'szip', 'lzf'.  If an integer in range(10), this indicates gzip
            compression level. Otherwise, an integer indicates the number of a
            dynamically loaded compression filter.
        compression_opts
            Compression settings.  This is an integer for gzip, 2-tuple for
            szip, etc. If specifying a dynamically loaded compression filter
            number, this must be a tuple of values.
        scaleoffset
            (Integer) Enable scale/offset filter for (usually) lossy
            compression of integer or floating-point data. For integer
            data, the value of scaleoffset is the number of bits to
            retain (pass 0 to let HDF5 determine the minimum number of
            bits necessary for lossless compression). For floating point
            data, scaleoffset is the number of digits after the decimal
            place to retain; stored values thus have absolute error
            less than 0.5*10**(-scaleoffset).
        shuffle
            (T/F) Enable shuffle filter.
        fletcher32
            (T/F) Enable fletcher32 error detection. Not permitted in
            conjunction with the scale/offset filter.
        fillvalue
            (Scalar) Use this value for uninitialized parts of the dataset.
        track_times
            (T/F) Enable dataset creation timestamps.
        """

        if self.id.http_conn.mode == 'r':
            raise ValueError("Unable to create dataset (No write intent on file)")

        with phil:
            dsid = dataset.make_new_dset(self, shape, dtype, data, **kwds)
            dset = dataset.Dataset(dsid)

            if name is not None:
                items = name.split('/')
                path = []
                for item in items:
                    if len(item) > 0:
                        path.append(item)  # just get non-empty strings

                grp = self

                if len(path) == 0:
                    # no name, just return anonymous dataset
                    return dset

                dset_link = path[-1]
                dset._name = self._name
                if dset._name[-1] != '/':
                    dset._name += '/'
                if len(path) > 1:
                    grp_path = path[:-1]
                    # create any grps along the path that don't already exist
                    for item in grp_path:
                        if item not in grp:
                            grp = grp.create_group(item)
                        else:
                            grp = grp[item]

                        dset._name = dset._name + item + '/'

                dset._name += dset_link
                grp[dset_link] = dset

            return dset

    def require_dataset(self, name, shape, dtype, exact=False, **kwds):
        """ Open a dataset, creating it if it doesn't exist.

        If keyword "exact" is False (default), an existing dataset must have
        the same shape and a conversion-compatible dtype to be returned.  If
        True, the shape and dtype must match exactly.

        Other dataset keywords (see create_dataset) may be provided, but are
        only used if a new dataset is to be created.

        Raises TypeError if an incompatible object already exists, or if the
        shape or dtype don't match according to the above rules.
        """

        with phil:
            if not name in self:
                return self.create_dataset(name, *(shape, dtype), **kwds)

            dset = self[name]
            if not isinstance(dset, Dataset):
                raise TypeError("Incompatible object (%s) already exists" % dset.__class__.__name__)

            if not shape == dset.shape:
                raise TypeError("Shapes do not match (existing %s vs new %s)" % (dset.shape, shape))

            if exact:
                if not dtype == dset.dtype:
                    raise TypeError("Datatypes do not exactly match (existing %s vs new %s)" % (dset.dtype, dtype))
            elif not numpy.can_cast(dtype, dset.dtype):
                raise TypeError("Datatypes cannot be safely cast (existing %s vs new %s)" % (dset.dtype, dtype))

            return dset

    def require_group(self, name):
        """ Return a group, creating it if it doesn't exist.

        TypeError is raised if something with that name already exists that
        isn't a group.
        """

        with phil:
            if not name in self:
                return self.create_group(name)
            grp = self[name]
            if not isinstance(grp, Group):
                raise TypeError("Incompatible object (%s) already exists" % grp.__class__.__name__)
            return grp

    def get_link_json(self, h5path):
        """ Return parent_uuid and json description of link for given path """
        parent_uuid = self.id.uuid
        tgt_json = None
        if h5path.find('/') == -1:
            in_group = True   # link owned by this group
        else:
            in_group = False  # may belong to some other group


        if h5path[0] == '/':
            #abs path, start with root
            # get root_uuid
            parent_uuid = self.id.http_conn.root_uuid   
            # make a fake tgt_json to represent 'link' to root group
            tgt_json = {'collection': "groups", 'class': "H5L_TYPE_HARD", 'id': parent_uuid }
            if h5path == '/':
                # asking for the root, just return the root link
                return parent_uuid, tgt_json
        else:
            if in_group and h5path in self._link_db:
                # link belonging to this group, see if it's in the cache
                tgt_json = self._link_db[h5path]
                parent_uuid = self.id.id
                 
                return parent_uuid, tgt_json
                

        path = h5path.split('/')

        for name in path:
            if not name:
                continue

            if not parent_uuid:
                raise KeyError("Unable to open object (Component not found)") 

            req = "/groups/" + parent_uuid + "/links/" + name

            try:
                rsp_json = self.GET(req)
            except IOError:
                raise KeyError("Unable to open object (Component not found)")

            if "link" not in rsp_json:
                raise IOError("Unexpected Error")
            tgt_json = rsp_json['link']

            if in_group:
                # add to db to speed up future requests
                self._link_db[name] = tgt_json

            if tgt_json['class'] == 'H5L_TYPE_HARD':
                if tgt_json['collection'] == 'groups':
                    parent_uuid = tgt_json['id']
                else:
                    parent_uuid = None

        return parent_uuid, tgt_json

    def __getitem__(self, name):
        """ Open an object in the file """
         

        def getObjByUuid(collection_type, uuid):
            """ Utility method to get an obj based on collection type and uuid """
            if collection_type == 'groups':
                req = "/groups/" + uuid
                group_json = self.GET(req)
                tgt = Group(GroupID(self, group_json))
            elif link_json['collection'] == 'datatypes':
                req = "/datatypes/" + uuid
                datatype_json = self.GET(req)
                tgt = Datatype(TypeID(self, datatype_json))
            elif link_json['collection'] == 'datasets':
                req = "/datasets/" + uuid
                dataset_json = self.GET(req)
                tgt = Dataset(DatasetID(self, dataset_json))
            else:
                raise IOError("Unexpected Error - collection type: " + link_json['collection'])
            return tgt

        tgt = None

        if isinstance(name, h5type.Reference):
            tgt = name.objref()  # weak reference to ref object
            if tgt is not None:
                return tgt  # ref'd object has not been deleted
            if isinstance(name.id, GroupID):
                tgt = getObjByUuid('groups', name.id.uuid)
            elif isinstance(name.id, DatasetID):
                tgt = getObjByUuid('datasets', name.id.uuid)
            elif isinstance(name.id, TypeID):
                tgt = getObjByUuid('datatypes', name.id.uuid)
            else:
                raise IOError("Unexpected Error - ObjectID type: " + name.__class__.__name__)
            return tgt

        parent_uuid, link_json = self.get_link_json(name)
        link_class = link_json['class']

        if link_class == 'H5L_TYPE_HARD':
            tgt = getObjByUuid(link_json['collection'], link_json['id'])
        elif link_class == 'H5L_TYPE_SOFT':
            h5path = link_json['h5path']
            soft_parent_uuid, soft_json = self.get_link_json(h5path)
            tgt = getObjByUuid(soft_json['collection'], soft_json['id'])

        elif link_class == 'H5L_TYPE_EXTERNAL':
            # try to get a handle to the file and return the linked object...
            # Note: set use_session to false since file.close won't be called
            #  (and hince the httpconn socket won't be closed)
            from .files import File
            try:
                f = File(link_json['h5domain'], endpoint=self.id.http_conn.endpoint, mode='r', use_session=False)
            except IOError:
                # unable to find external link
                raise KeyError("Unable to open file: " + link_json['h5domain'])
            return f[link_json['h5path']]

        elif link_class == 'H5L_TYPE_USER_DEFINED':
            raise IOError("Unable to fetch user-defined link")
        else:
            raise IOError("Unexpected error, invalid link class:" + link_json['class'])

        # assign name
        if name[0] == '/':
            tgt._name = name
        else:
            if self.name:
                if self.name[-1] == '/':
                    tgt._name = self.name + name
                else:
                    tgt._name = self.name + '/' + name
            else:
                tgt._name = name
        return tgt

    def get(self, name, default=None, getclass=False, getlink=False):
        """ Retrieve an item or other information.

        "name" given only:
            Return the item, or "default" if it doesn't exist

        "getclass" is True:
            Return the class of object (Group, Dataset, etc.), or "default"
            if nothing with that name exists

        "getlink" is True:
            Return HardLink, SoftLink or ExternalLink instances.  Return
            "default" if nothing with that name exists.

        "getlink" and "getclass" are True:
            Return HardLink, SoftLink and ExternalLink classes.  Return
            "default" if nothing with that name exists.

        Example:

        >>> cls = group.get('foo', getclass=True)
        >>> if cls == SoftLink:
        ...     print '"foo" is a soft link!'
        """

        with phil:
            if not (getclass or getlink):
                try:
                    return self[name]
                except KeyError:
                    return default

            if not name in self:
                return default

            elif getclass and not getlink:
                obj = self.__getitem__(name)
                if obj is None:
                    return None
                if obj.id.__class__ is GroupID:
                    return Group
                elif obj.id.__class__ is DatasetID:
                    return Dataset
                elif obj.id.__class__ is TypeID:
                    return Datatype
                else:
                    raise TypeError("Unknown object type")

            elif getlink:
                parent_uuid, link_json = self.get_link_json(name)
                typecode = link_json['class']

                if typecode == 'H5L_TYPE_SOFT':
                    if getclass:
                        return SoftLink

                    return SoftLink(link_json['h5path'])
                elif typecode == 'H5L_TYPE_EXTERNAL':
                    if getclass:
                        return ExternalLink

                    return ExternalLink(link_json['h5domain'], link_json['h5path'])
                elif typecode == 'H5L_TYPE_HARD':
                    return HardLink if getclass else HardLink()
                else:
                    raise TypeError("Unknown link type")

    def __setitem__(self, name, obj):
        """ Add an object to the group.  The name must not already be in use.

        The action taken depends on the type of object assigned:

        Named HDF5 object (Dataset, Group, Datatype)
            A hard link is created at "name" which points to the
            given object.

        SoftLink or ExternalLink
            Create the corresponding link.

        Numpy ndarray
            The array is converted to a dataset object, with default
            settings (contiguous storage, etc.).

        Numpy dtype
            Commit a copy of the datatype as a named datatype in the file.

        Anything else
            Attempt to convert it to an ndarray and store it.  Scalar
            values are stored as scalar datasets. Raise ValueError if we
            can't understand the resulting array dtype.
        """

        #name, lcpl = self._e(name, lcpl=True)
        if name.find('/') != -1:
            parent_path = op.dirname(name)
            basename = op.basename(name)
            if not basename:
                raise KeyError("Group path can not end with '/'")
            parent_uuid, link_json = self.get_link_json(parent_path)
            if parent_uuid is None:
                raise KeyError("group path: {} not found".format(parent_path))
            req = "/groups/" + parent_uuid
            group_json = self.GET(req)
            tgt = Group(GroupID(self, group_json)) 
            tgt[basename] = obj   
             
        elif isinstance(obj, HLObject):
            body = {'id': obj.id.uuid }
            req = "/groups/" + self.id.uuid + "/links/" + name
            self.PUT(req, body=body)

        elif isinstance(obj, SoftLink):
            body = {'h5path': obj.path }
            req = "/groups/" + self.id.uuid + "/links/" + name
            self.PUT(req, body=body)
            #self.id.links.create_soft(name, self._e(obj.path),
            #              lcpl=lcpl, lapl=self._lapl)

        elif isinstance(obj, ExternalLink):
            body = {'h5path': obj.path,
                    'h5domain': obj.filename }
            req = "/groups/" + self.id.uuid + "/links/" + name
            self.PUT(req, body=body)
            #self.id.links.create_external(name, self._e(obj.filename),
            #              self._e(obj.path), lcpl=lcpl, lapl=self._lapl)

        elif isinstance(obj, numpy.dtype):
            # print "create named type"

            type_json = h5type.getTypeItem(obj)
            req = "/datatypes"

            body = {'type': type_json }
            rsp = self.POST(req, body=body)
            body['id'] = rsp['id']
            body['lastModified'] = rsp['lastModified']

            type_id = TypeID(self, body)
            req = "/groups/" + self.id.uuid + "/links/" + name
            body = {'id': type_id.uuid }
            self.PUT(req, body=body)

            #htype = h5t.py_create(obj)
            #htype.commit(self.id, name, lcpl=lcpl)

        else:
            pass #todo
            #ds = self.create_dataset(None, data=obj, dtype=base.guess_dtype(obj))
            #h5o.link(ds.id, self.id, name, lcpl=lcpl)

    def __delitem__(self, name):
        """ Delete (unlink) an item from this group. """
        req = "/groups/" + self.id.uuid + "/links/" + name
        self.DELETE(req)
        if name.find('/') == -1 and name in self._link_db:
            # remove from link cache
            del self._link_db[name]
        #self.id.unlink(self._e(name))

    def __len__(self):
        """ Number of members attached to this group """
        req = "/groups/" + self.id.uuid
        rsp_json = self.GET(req)
        return rsp_json['linkCount']
        #return self.id.get_num_objs()

    def __iter__(self):
        """ Iterate over member names """
        req = "/groups/" + self.id.uuid + "/links"
        rsp_json = self.GET(req)
        links = rsp_json['links']

        # reset the link cache
        self._link_db = {}
        for link in links:
            name = link["title"]
            self._link_db[name] = link

        for x in links:
            yield x['title']
        """
        for x in self.id.__iter__():
            yield self._d(x)
        """

    def __contains__(self, name):
        """ Test if a member name exists """
        found = False
        try:
            self.get_link_json(name)
            found = True
        except KeyError:
            pass  # not found
        return found

    def copy(self, source, dest, name=None,
             shallow=False, expand_soft=False, expand_external=False,
             expand_refs=False, without_attrs=False):
        """Copy an object or group.

        The source can be a path, Group, Dataset, or Datatype object.  The
        destination can be either a path or a Group object.  The source and
        destinations need not be in the same file.

        If the source is a Group object, all objects contained in that group
        will be copied recursively.

        When the destination is a Group object, by default the target will
        be created in that group with its current name (basename of obj.name).
        You can override that by setting "name" to a string.

        There are various options which all default to "False":

         - shallow: copy only immediate members of a group.

         - expand_soft: expand soft links into new objects.

         - expand_external: expand external links into new objects.

         - expand_refs: copy objects that are pointed to by references.

         - without_attrs: copy object without copying attributes.

       Example:

        >>> f = File('myfile.hdf5')
        >>> f.listnames()
        ['MyGroup']
        >>> f.copy('MyGroup', 'MyCopy')
        >>> f.listnames()
        ['MyGroup', 'MyCopy']

        """
        pass
        """
        with phil:
            if isinstance(source, HLObject):
                source_path = '.'
            else:
                # Interpret source as a path relative to this group
                source_path = source
                source = self

            if isinstance(dest, Group):
                if name is not None:
                    dest_path = name
                else:
                    # copy source into dest group: dest_name/source_name
                    dest_path = pp.basename(h5i.get_name(source[source_path].id))

            elif isinstance(dest, HLObject):
                raise TypeError("Destination must be path or Group object")
            else:
                # Interpret destination as a path relative to this group
                dest_path = dest
                dest = self

            flags = 0
            if shallow:
                flags |= h5o.COPY_SHALLOW_HIERARCHY_FLAG
            if expand_soft:
                flags |= h5o.COPY_EXPAND_SOFT_LINK_FLAG
            if expand_external:
                flags |= h5o.COPY_EXPAND_EXT_LINK_FLAG
            if expand_refs:
                flags |= h5o.COPY_EXPAND_REFERENCE_FLAG
            if without_attrs:
                flags |= h5o.COPY_WITHOUT_ATTR_FLAG
            if flags:
                copypl = h5p.create(h5p.OBJECT_COPY)
                copypl.set_copy_object(flags)
            else:
                copypl = None

            h5o.copy(source.id, self._e(source_path), dest.id, self._e(dest_path),
                     copypl, base.dlcpl)
        """

    def move(self, source, dest):
        """ Move a link to a new location in the file.

        If "source" is a hard link, this effectively renames the object.  If
        "source" is a soft or external link, the link itself is moved, with its
        value unmodified.
        """
        pass
        """
        with phil:
            if source == dest:
                return
            self.id.links.move(self._e(source), self.id, self._e(dest),
                               lapl=self._lapl, lcpl=self._lcpl)
        """

    def visit(self, func):
        """ Recursively visit all names in this group and subgroups (HDF5 1.8).

        You supply a callable (function, method or callable object); it
        will be called exactly once for each link in this group and every
        group below it. Your callable must conform to the signature:

            func(<member name>) => <None or return value>

        Returning None continues iteration, returning anything else stops
        and immediately returns that value from the visit method.  No
        particular order of iteration within groups is guranteed.

        Example:

        >>> # List the entire contents of the file
        >>> f = File("foo.hdf5")
        >>> list_of_names = []
        >>> f.visit(list_of_names.append)
        """
        self.visititems(func)
        """
        with phil:
            def proxy(name):
                return func(self._d(name))
            return h5o.visit(self.id, proxy)
        """

    def visititems(self, func):
        """ Recursively visit names and objects in this group (HDF5 1.8).

        You supply a callable (function, method or callable object); it
        will be called exactly once for each link in this group and every
        group below it. Your callable must conform to the signature:

            func(<member name>, <object>) => <None or return value>

        Returning None continues iteration, returning anything else stops
        and immediately returns that value from the visit method.  No
        particular order of iteration within groups is guranteed.

        Example:

        # Get a list of all datasets in the file
        >>> mylist = []
        >>> def func(name, obj):
        ...     if isinstance(obj, Dataset):
        ...         mylist.append(name)
        ...
        >>> f = File('foo.hdf5')
        >>> f.visititems(func)
        """
        visited = collections.OrderedDict()
        visited[self.id.uuid] = True
        tovisit = collections.OrderedDict()
        tovisit[self.id.uuid] = self

        if six.PY3:
            nargs = func.__code__.co_argcount
        else:
            nargs = func.func_code.co_argcount

        while len(tovisit) > 0:
            (parent_uuid, parent) = tovisit.popitem(last=True)
            if parent.name != '/':
                if nargs == 1:
                    retval = func(parent.name)
                else:
                    retval = func(parent.name, parent)
                if retval is not None:
                    # caller indicates to end iteration
                    break
            visited[self.id.uuid] = True
            if parent.id.__class__ is GroupID:
                req = "/groups/" + parent.id.uuid + "/links"
                rsp_json = self.GET(req)
                links = rsp_json['links']
                for link in links:
                    obj = None
                    if link['class'] == 'H5L_TYPE_SOFT':
                        #obj = SoftLink(link['h5path'])
                        pass  # don't visit soft links'
                    elif link['class'] == 'H5L_TYPE_EXTERNAL':
                        #obj = ExternalLink(link['h5domain'], link['h5path'])
                        pass # don't visit external links'
                    elif link['class'] ==  'H5L_TYPE_UDLINK':
                        obj = UserDefinedLink()
                    elif link['class'] == 'H5L_TYPE_HARD':
                        if link['id'] in visited:
                            continue  # already been there
                        obj = parent.__getitem__(link['title'])
                        tovisit[obj.id.uuid] = obj
                        obj = None
                    if obj is not None:
                        # call user func directly for non-hardlinks
                        link_name = parent.name + '/' + link['title']
                        if nargs == 1:
                            retval = func(link_name)
                        else:
                            retval = func(link_name, obj)
                        if retval is not None:
                            # caller indicates to end iteration
                            break

        """
        with phil:
            def proxy(name):
                name = self._d(name)
                return func(name, self[name])
            return h5o.visit(self.id, proxy)
        """

    def __repr__(self):
        if not self:
            r = six.u("<Closed HDF5 group>")
        else:
            namestr = (
                six.u('"%s"') % self.name
            ) if self.name is not None else six.u("(anonymous)")
            r = six.u('<HDF5 group %s (%d members)>') % (namestr, len(self))

        if six.PY3:
            return r
        return r.encode('utf8')


class HardLink(object):

    """
        Represents a hard link in an HDF5 file.  Provided only so that
        Group.get works in a sensible way.  Has no other function.
    """

    pass


#TODO: implement equality testing for these
class SoftLink(object):

    """
        Represents a symbolic ("soft") link in an HDF5 file.  The path
        may be absolute or relative.  No checking is performed to ensure
        that the target actually exists.
    """

    @property
    def path(self):
        return self._path

    def __init__(self, path):
        self._path = str(path)

    def __repr__(self):
        return '<SoftLink to "%s">' % self.path


class ExternalLink(object):

    """
        Represents an HDF5 external link.  Paths may be absolute or relative.
        No checking is performed to ensure either the target or file exists.
    """

    @property
    def path(self):
        return self._path

    @property
    def filename(self):
        return self._filename

    def __init__(self, filename, path):
        self._filename = str(filename)
        self._path = str(path)

    def __repr__(self):
        return '<ExternalLink to "%s" in file "%s">' % (self.path, self.filename)

class UserDefinedLink(object):

    """
        Represents a user-defined link
    """

    def __init__(self):
        pass

    def __repr__(self):
        return '<UDLink >'
