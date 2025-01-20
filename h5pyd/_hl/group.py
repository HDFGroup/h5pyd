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

import os.path as op
import numpy
import collections

from .base import HLObject, MutableMappingHDF5, guess_dtype
from .objectid import TypeID, GroupID, DatasetID, isUUID
from .h5type import special_dtype
from . import dataset
from .dataset import Dataset
from .table import Table
from .datatype import Datatype
from . import h5type


def _h5parent(path):
    """ Return parent of given path """
    parent_path = op.dirname(path)
    return parent_path


def _h5base(path):
    """ Return base name of the given path """
    # TBD - this needs to fixed up to work as h5py does
    # e.g. _h5base('x/y/') should return 'y'
    base_path = op.basename(path)
    return base_path


class Group(HLObject, MutableMappingHDF5):

    """ Represents an HDF5 group.
    """

    def __init__(self, bind, track_order=None, **kwargs):

        """ Create a new Group object by binding to a low-level GroupID.
        """

        if not isinstance(bind, GroupID):
            raise ValueError(f"{bind} is not a GroupID")
        super().__init__(bind, track_order=track_order, **kwargs)

    def _get_bypath(self, h5path, create=False, track_order=None):
        """ Return  object id at given path.
            If group_create, create any groups that don't already exists """

        self.log.info(f"_get_bypath: {h5path}")

        if h5path == "/":
            # return root group
            root_uuid = self.id.http_conn.root_uuid
            root_id = self.id.get(root_uuid)  # create a GroupID object
            root_grp = Group(root_id, track_order=track_order)
            return root_grp
        elif h5path[0] == '/':
            # absolute path - start with root
            root_uuid = self.id.http_conn.root_uuid
            parent_id = self.id.get(root_uuid)
            parent_name = "/"
        else:
            # relative path - start with this object
            parent_id = self.id
            parent_name = self._name

        links = h5path.split('/')
        for title in links:
            if not title:
                continue  # skip
            self.log.debug(f"_get_bypath - iterate for link: {title}")
            if parent_id.has_link(title):
                # the sub-group already exists, adjust parent and keep iterating
                sub_link_json = parent_id.get_link(title)
                link_class = sub_link_json['class']
                if link_class == 'H5L_TYPE_HARD':
                    parent_id = parent_id.get(sub_link_json['id'])
                elif link_class == 'H5L_TYPE_SOFT':
                    slink_path = sub_link_json.get('h5path')
                    if not slink_path:
                        raise IOError(f"invalid softlink: {title}")
                    obj = self._get_bypath(slink_path)  # recursive call
                    parent_id = obj.id
                elif link_class == 'H5L_TYPE_EXTERNAL':
                    external_path = sub_link_json.get('h5path')
                    external_domain = sub_link_json.get('h5domain')
                    if not external_path or not external_domain:
                        raise IOError(f"invalid extenallink: {title}")
                    # TBD: how to handle external links to other buckets?
                    from .files import File
                    if not external_domain.startswith("hdf5://") and not op.isabs(external_domain):
                        current_domain = self._id.http_conn.domain
                        external_domain = op.join(op.dirname(current_domain), external_domain)
                        external_domain = op.normpath(external_domain)
                    try:
                        f = File(external_domain, track_order=track_order)
                    except IOError:
                        # unable to find external link
                        raise KeyError(f"Unable to open domain: {external_domain}")
                    return f[external_path]
                else:
                    raise IOError(f"Unexpected link_class: {link_class}")
            elif create:
                # create the sub-group
                if self.id.http_conn.mode == 'r':
                    raise ValueError("Unable to create group (No write intent on file)")

                self.log.debug(f"_get_bypath - making subgroup: '{title}'")
                parent_id = parent_id.make_obj(title, track_order=track_order)
            else:
                self.log.warning(f"_get_bypath(h5path={h5path}, parent_id={parent_id}) not found")
                raise KeyError(f"object {h5path} does not exists")
            if parent_name:
                if parent_name[-1] == '/':
                    parent_name = parent_name + title
                else:
                    parent_name = f"{parent_name}/{title}"
                self.log.debug(f"_get_bypath - parent name: {parent_name}")

        if isinstance(parent_id, GroupID):
            tgt = Group(parent_id, track_order=track_order)
        elif isinstance(parent_id, TypeID):
            tgt = Datatype(parent_id, track_order=track_order)
        elif isinstance(parent_id, DatasetID):
            if parent_id.rank == 1 and parent_id.type_class == 'H5T_COMPOUND':
                tgt = Table(parent_id, track_order=track_order)
            else:
                tgt = Dataset(parent_id, track_order=track_order)
        else:
            raise TypeError(f"unexpected type: {type(parent_id)}")

        tgt._name = parent_name

        return tgt

    def create_group(self, h5path, track_order=None, ignore_exists=False):
        """ Create and return a new subgroup.

        Name may be absolute or relative.  Fails if the target name already
        exists.
        """

        if h5path is None:
            obj_id = self.id.make_obj(None, track_order=track_order)
            return Group(obj_id)

        if not ignore_exists:
            # verify an existing link is not already present
            h5parent = _h5parent(h5path)
            h5base = _h5base(h5path)
            if h5parent:
                parent_group = self._get_bypath(h5parent, create=True)
            else:
                parent_group = self
            if parent_group.id.has_link(h5base):
                self.log.warning("unable to create_group: {h5parent}, already exists")
                raise ValueError("Unable to synchronously create group (name already exists)")

        sub_group = self._get_bypath(h5path, track_order=track_order, create=True)

        return sub_group

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
        track_order
            (T/F) List attributes by creation_time if set
        initializer
            (String) chunk initializer function
        initializer_args
            (List) arguments to be passed to initializer
        """

        if self.id.http_conn.mode == 'r':
            raise ValueError("Unable to create dataset (No write intent on file)")

        if isinstance(name, bytes):
            # convert byte input to string
            name = name.decode("utf-8")

        if name:
            if name[-1] == '/':
                raise ValueError("Invalid path for create_dataset")
            h5path = _h5parent(name)
            if h5path:
                parent_grp = self._get_bypath(h5path, create=True)
            else:
                parent_grp = self
            base_name = _h5base(name)
        else:
            parent_grp = self
            base_name = None

        dset_id = dataset.make_new_dset(parent_grp, name=base_name, shape=shape, dtype=dtype, data=data, **kwds)
        if dset_id.rank == 1 and dset_id.type_class == 'H5T_COMPOUND':
            dset = Table(dset_id)
        else:
            dset = Dataset(dset_id)

        if base_name:
            dset._name = f"{self._name}/{base_name}"

        return dset

    def create_dataset_like(self, name, other, **kwupdate):
        """ Create a dataset similar to `other`.

        name
            Name of the dataset (absolute or relative).  Provide None to make
            an anonymous dataset.
        other
            The dataset which the new dataset should mimic. All properties, suchd
            as shape, dtype, chunking, ... will be taken from it, but no data
            or attributes are being copied.

        Any dataset keywords (see create_dataset) may be provided, including
        shape and dtype, in which case the provided values take precedence over
        those from `other`.
        """
        for k in ('shape', 'dtype', 'chunks', 'compression',
                  'compression_opts', 'scaleoffset', 'shuffle', 'fletcher32',
                  'fillvalue'):
            kwupdate.setdefault(k, getattr(other, k))
        # TODO: more elegant way to pass these (dcpl to create_dataset?)

        cpl = other.id.cpl
        track_order = None
        if "CreateOrder" in cpl:
            createOrder = cpl["CreateOrder"]
            if not createOrder or createOrder == "0":
                track_order = False
            else:
                track_order = True

        kwupdate.setdefault('track_order', track_order)

        # Special case: the maxshape property always exists, but if we pass it
        # to create_dataset, the new dataset will automatically get chunked
        # layout. So we copy it only if it is different from shape.
        if other.maxshape != other.shape:
            kwupdate.setdefault('maxshape', other.maxshape)

        return self.create_dataset(name, **kwupdate)

    def create_virtual_dataset(name, layout, fillvalue=None):
        """ Create a virtual dataset """
        # not currently supported
        raise IOError("Not supported")

    def create_table(self, name, numrows=None, dtype=None, data=None, **kwds):
        """ Create a new Table - a one dimensional HDF5 Dataset with a compound type

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
        """
        # Convert data to a C-contiguous ndarray
        shape = None
        if data is not None:
            if dtype is None:
                dtype = guess_dtype(data)
            if dtype is None:
                dtype = numpy.float32
            if not isinstance(data, numpy.ndarray) or dtype != data.dtype:
                data = numpy.asarray(data, order="C", dtype=dtype)
            self.log.info(f"data dtype: {data.dtype}")
            if len(data.shape) != 1:
                ValueError("Table must be one-dimensional")
            if numrows and numrows != data.shape[0]:
                ValueError("Data does not match numrows value")
            shape = data.shape
        elif numrows:
            shape = [numrows,]
        else:
            shape = [0,]

        if dtype is None:
            ValueError("dtype must be specified or data provided")
        if len(dtype) < 1:
            ValueError("dtype must be compound")
        kwds["maxshape"] = (0,)
        dset = self.create_dataset(name, shape=shape, dtype=dtype, data=data, **kwds)
        tbl = Table(dset.id)

        if name:
            tbl._name = f"{self._name}/{name}"

        return tbl

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

        if isinstance(name, bytes):
            name = name.decode('utf-8')

        if name not in self:
            return self.create_dataset(name, *(shape, dtype), **kwds)

        if isinstance(shape, int):
            shape = (shape,)

        dset = self[name]
        if not isinstance(dset, Dataset):
            raise TypeError(f"Incompatible object ({dset.__class__.__name__}) already exists")

        if not shape == dset.shape:
            raise TypeError(f"Shapes do not match (existing {dset.shape} vs new {shape})")

        if exact:
            if not dtype == dset.dtype:
                raise TypeError(f"Datatypes do not exactly match (existing {dset.dtype} vs new {dtype})")
        elif not numpy.can_cast(dtype, dset.dtype):
            raise TypeError(f"Datatypes cannot be safely cast (existing {dset.dtype} vs new {dtype})")

        return dset

    def require_group(self, name, track_order=None):
        """ Return a group, creating it if it doesn't exist.

        TypeError is raised if something with that name already exists that
        isn't a group.
        """
        grp = self._get_bypath(name, track_order=track_order, create=True)
        return grp

    def __getitem__(self, name, track_order=None):
        """ Open an object in the file """
        # convert bytes to str for PY3
        if isinstance(name, bytes):
            name = name.decode('utf-8')
        self.log.debug(f"group.__getitem__({name}, track_order={track_order})")

        tgt_uuid = None
        if isinstance(name, h5type.Reference):
            tgt = name.objref()  # weak reference to ref object
            if tgt is not None:
                return tgt  # ref'd object has not been deleted
            else:
                tgt_uuid = name.id.id
        elif isUUID(name):
            tgt_uuid = name
        elif name == "/":
            # return root group
            tgt_uuid = self.id.http_conn.root_uuid
        else:
            pass  # will do a path lookup

        if tgt_uuid:
            obj_id = self.id.get(tgt_uuid)
            if isinstance(obj_id, GroupID):
                tgt = Group(obj_id)
            elif isinstance(obj_id, DatasetID):
                if obj_id.rank == 1 and obj_id.type_class == 'H5T_COMPOUND':
                    tgt = Table(obj_id)
                else:
                    tgt = Dataset(obj_id)
            elif isinstance(obj_id, TypeID):
                tgt = Datatype(obj_id)
            else:
                raise IOError("Unexpected Error - ObjectID type: " + obj_id.__class__.__name__)
            return tgt

        # get item by h5path
        tgt = self._get_bypath(name, track_order=track_order)

        return tgt

    def get(self, name, default=None, getclass=False, getlink=False, track_order=None, **kwds):
        """ Retrieve an item or other information.

        "name" given only:
            Return the item with the given name, or "default" if nothing with that name exists

        "getclass" is True:
            Return the class of object (Group, Dataset, etc.), or "default"
            if nothing with that name exists

        "getlink" is True:
            Return HardLink, SoftLink or ExternalLink instances.  Return
            "default" if nothing with that name exists.

        "getlink" and "getclass" are True:
            Return HardLink, SoftLink and ExternalLink classes.  Return
            "default" if nothing with that name exists.

        "track_order" is (T/F):
            List links and attributes by creation order if True, alphanumerically if False.
            If None, the track_order used when creating the group will be used.


        Example:

        >>> cls = group.get('foo', getclass=True)
        >>> if cls == SoftLink:
        ...     print '"foo" is a soft link!'
        """
        kwd_args = ("limit", "marker", "pattern", "follow_links")
        for kwd in kwds:
            if kwd not in kwd_args:
                raise TypeError(f"group.get() unexpected keyword argument: {kwd}")

        if not name:
            raise TypeError("Argument 'path' must not be None")

        h5path = _h5parent(name)
        base_name = _h5base(name)
        if not base_name:
            # TBD: is this valid?
            raise IOError(f"invalid name: {name}")

        if not h5path:
            parent = self
        else:
            parent = self.__getitem__(h5path)
            if not isinstance(parent, Group):
                self.log.error(f"unexpected object: {type(parent)}")
                raise TypeError(name)

        if not isinstance(parent.id, GroupID):
            self.log.error(f"unexpected object: {type(parent)}")
            raise TypeError(name)

        if not parent.id.has_link(base_name):
            raise IOError(f"{name} not found")

        if getlink:
            link_json = parent.id.get_link(base_name)
            link_class = link_json['class']
            if link_class == 'H5L_TYPE_HARD':
                if getclass:
                    return HardLink
                else:
                    return HardLink()
            elif link_class == 'H5L_TYPE_SOFT':
                if getclass:
                    return SoftLink
                else:
                    soft_path = link_json["h5path"]
                    return SoftLink(soft_path)
            elif link_class == 'H5L_TYPE_EXTERNAL':
                if getclass:
                    return ExternalLink
                else:
                    ext_path = link_json['h5path']
                    domain_path = link_json['h5domain']
                    return ExternalLink(domain_path, ext_path)
            else:
                self.log.info(f"user-defined link class: {link_class}")
                if getclass:
                    return UserDefinedLink
                else:
                    return UserDefinedLink()

        else:
            tgt = self.__getitem__(name)

            if getclass:
                # return class of object that link is pointing too
                if isinstance(tgt.id, GroupID):
                    return Group
                elif isinstance(tgt.id, TypeID):
                    return Datatype
                elif isinstance(tgt, DatasetID):
                    return Dataset
                else:
                    raise TypeError("Unexpected id class: {type(tgt)}")
            else:
                # getclass and getlink ar false, return the object
                if track_order is not None:
                    tgt._track_order = track_order
                return tgt

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

        if not name:
            raise TypeError("Argument 'path', must not be none")
        if len(name) > 1 and name[-1] == '/':
            name = name[:-1]
        parent_path = _h5parent(name)
        basename = _h5base(name)

        if parent_path:
            grp = self._get_bypath(parent_path, create=True)
        else:
            grp = self

        self.log.debug(f"got parent group for set: {grp.name}")

        if basename in grp:
            self.log.warning(f"link with {basename} already exists")
            raise IOError("Unable to create link (name already exists)")

        if isinstance(obj, HLObject):
            # create a hardlink to the given object
            link_json = {'class': 'H5L_TYPE_HARD', 'id': obj.id.id}
            grp.id.set_link(basename, link_json)
        elif isinstance(obj, SoftLink):
            link_json = {'class': 'H5L_TYPE_SOFT', 'h5path': obj.path}
            grp.id.set_link(basename, link_json)
        elif isinstance(obj, ExternalLink):
            link_json = {'class': 'H5L_TYPE_EXTERNAL', 'h5path': obj.path}
            link_json['h5domain'] = obj.filename
            grp.id.set_link(basename, link_json)

        elif isinstance(obj, numpy.dtype):
            self.log.info("create named type")

            type_json = h5type.getTypeItem(obj)
            grp.id.make_obj(name, type_json=type_json)

        else:
            if isinstance(obj, numpy.ndarray):
                arr = obj
            elif isinstance(obj, str):
                dt = special_dtype(vlen=str)
                arr = numpy.array(obj, dtype=dt)
            else:
                dt = guess_dtype(obj)
                arr = numpy.array(obj, dtype=dt)
            grp.create_dataset(basename, shape=arr.shape, dtype=arr.dtype, data=arr[...])
            # link was created for us

    def __delitem__(self, name):
        """ Delete (unlink) an item from this group. """
        objdb = self.id.http_conn.objdb

        if isUUID(name):
            obj_id = op.basename(name)
            if obj_id in objdb:
                del objdb[obj_id]
            else:
                self.log.warning(f"expected to find obj_id: {obj_id} for delete")
        else:
            parent_path = _h5parent(name)
            basename = _h5base(name)
            if not basename:
                raise KeyError("Group path can not end with '/'")

            if parent_path:
                grp = self._get_bypath(parent_path)
            else:
                grp = self

            grp.id.del_link(basename)

    def __len__(self):
        """ Number of members attached to this group """
        num_links = self.id.link_count
        return num_links

    def __iter__(self):
        """ Iterate over member names """
        titles = self.id.get_link_titles(track_order=self.track_order)

        for title in titles:
            yield title

    def __reversed__(self):
        """ Iterate over member names in reverse order """
        titles = self.id.get_link_titles(track_order=self.track_order)

        for title in reversed(titles):
            yield title

    def __contains__(self, name):
        """ Test if a member name exists """
        if name == "/":
            return True
        parent_path = _h5parent(name)
        basename = _h5base(name)
        if not basename:
            raise KeyError("Group path can not end with '/'")

        if parent_path:
            grp = self._get_bypath(parent_path)
        else:
            grp = self

        if grp.id.has_link(basename):
            return True
        else:
            return False

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
        raise IOError("Not implemented")

    def move(self, source, dest):
        """ Move a link to a new location in the file.

        If "source" is a hard link, this effectively renames the object.  If
        "source" is a soft or external link, the link itself is moved, with its
        value unmodified.
        """
        raise IOError("Not supported")

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
        return self.visititems(func)

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
        retval = None

        nargs = func.__code__.co_argcount

        while len(tovisit) > 0:
            (parent_uuid, parent) = tovisit.popitem(last=True)
            if parent.name != '/':
                h5path = parent.name
                if h5path[0] == '/':
                    h5path = h5path[1:]
                if nargs == 1:
                    retval = func(h5path)
                else:
                    retval = func(h5path, parent)
                if retval is not None:
                    # caller indicates to end iteration
                    break
            visited[parent.id.uuid] = True
            if parent.id.__class__ is GroupID:
                # get group links
                for title in parent.id.get_link_titles():
                    link = parent.id.get_link(title)
                    obj = None
                    if link['class'] == 'H5L_TYPE_SOFT':
                        # obj = SoftLink(link['h5path'])
                        pass  # don't visit soft links'
                    elif link['class'] == 'H5L_TYPE_EXTERNAL':
                        # obj = ExternalLink(link['h5domain'], link['h5path'])
                        pass  # don't visit external links'
                    elif link['class'] == 'H5L_TYPE_UDLINK':
                        obj = UserDefinedLink()
                    elif link['class'] == 'H5L_TYPE_HARD':
                        if link['id'] in visited:
                            continue  # already been there
                        obj = parent.__getitem__(title)
                        tovisit[obj.id.uuid] = obj
                        obj = None
                    if obj is not None:
                        # call user func directly for non-hardlinks
                        link_name = parent.name + '/' + link['title']
                        if link_name[0] == '/':
                            # don't include the first slash
                            link_name = link_name[1:]
                        if nargs == 1:
                            retval = func(link_name)
                        else:
                            retval = func(link_name, obj)
                        if retval is not None:
                            # caller indicates to end iteration
                            break

        return retval

    def __repr__(self):
        if not self:
            r = "<Closed HDF5 group>"
        else:
            if self.name is None:
                namestr = "(anonymous)"
            else:
                namestr = f'"{self.name}"'
            r = f'<HDF5 group {namestr} ({len(self)} members)>'
        return r

    def refresh(self):
        """Refresh the group metadata by reloading from the file.
        """
        self.id.refresh()


class HardLink(object):

    """
        Represents a hard link in an HDF5 file.  Provided only so that
        Group.get works in a sensible way.  Has no other function.
    """
    @property
    # The uuid of the target object
    def id(self):
        return self._id

    def __init__(self, id=None):
        self._id = id

    def __repr__(self):
        return f'<HardLink to "{self.id}">'


# TODO: implement equality testing for these
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
        return f'<SoftLink to "{self.path}">'


class ExternalLink(object):

    """
        Represents an HDF5 external link.  Paths may be absolute or relative.
        No checking is performed to ensure either the target or the domain exists.
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
        return f'<ExternalLink to "{self.path}" in file "{self.filename}">'


class UserDefinedLink(object):

    """
        Represents a user-defined link
    """

    def __init__(self):
        pass

    def __repr__(self):
        return '<UDLink >'
