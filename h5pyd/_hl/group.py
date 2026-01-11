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
from h5json.objid import isValidUuid, getCollectionForId
from h5json.hdf5dtype import special_dtype, Reference, guess_dtype
from h5json.link_util import getLinkClass
from h5json.shape_util import getRank

from .base import HLObject, MutableMappingHDF5
from .objectid import TypeID, GroupID, DatasetID
from . import dataset
from .dataset import Dataset
from . import table
from .table import Table
from .datatype import Datatype
from .. import config


class Group(HLObject, MutableMappingHDF5):

    """ Represents an HDF5 group.
    """

    def __init__(self, bind, track_order=None, **kwargs):
        # print "group init, bind:", bind

        """ Create a new Group object by binding to a low-level GroupID.
        """
        if not isinstance(bind, GroupID):
            raise ValueError(f"{bind} is not a GroupID")
        HLObject.__init__(self, bind, track_order=track_order, **kwargs)

    def _get_link_json(self, h5path):
        """ Return parent_uuid and json description of link for given path """
        self.log.debug(f"__get_link_json({h5path})")
        parent_uuid = self.id.uuid
        tgt_json = None
        if isinstance(h5path, bytes):
            h5path = h5path.decode('utf-8')

        if h5path[0] == '/':
            parent_uuid = self.id.db.root_id
        else:
            parent_uuid = self.id.uuid

        if h5path.find('/') == -1:
            # no path to traverse, just return the link for this group (if it exists)
            tgt_json = self.id.db.getLink(self.id.uuid, h5path)
            if not tgt_json:
                raise KeyError("Unable to open object (Component not found)")
            return self.id.uuid, tgt_json

        if h5path == '/':
            # make a fake tgt_json to represent 'link' to root group
            tgt_json = {'collection': "groups", 'class': "H5L_TYPE_HARD", 'id': parent_uuid}
            if h5path == '/':
                # asking for the root, just return the root link
                return self.id.db.root_id, tgt_json

        # fake link to start the iteration
        tgt_json = {"class": "H5L_TYPE_HARD", "id": parent_uuid}
        path = h5path.split('/')
        for name in path:
            if not name:
                continue
            parent_uuid = tgt_json["id"]
            tgt_json = self.id.db.getLink(parent_uuid, name)
            if not tgt_json:
                raise KeyError("Unable to open object (Component not found)")
            link_class = getLinkClass(tgt_json)
            if link_class != "H5L_TYPE_HARD":
                raise IOError(f"Unable to follow link type: {link_class}")
        return parent_uuid, tgt_json

    def _make_group(self, parent_id=None, parent_name=None, link=None, track_order=None):
        """ helper function to make a group """

        cfg = config.get_config()

        if track_order or cfg.track_order:
            cpl = {"CreateOrder": 1}
        else:
            cpl = None

        grp_uuid = self.id.db.createGroup(cpl=cpl)
        group_json = self.id.db.getObjectById(grp_uuid)

        if parent_id and link:
            # create link from parent_id to grp_id
            self.id.db.createHardLink(parent_id, link, grp_uuid)

        groupId = GroupID(self, grp_uuid, obj_json=group_json)

        sub_group = Group(groupId, track_order=(track_order or cfg.track_order))

        if parent_name:
            if parent_name[-1] == '/':
                parent_name = parent_name + link
            else:
                parent_name = parent_name + '/' + link
            self.log.debug(f"create group - parent name: {parent_name}")
            sub_group._name = parent_name

        return sub_group

    def create_group(self, h5path, track_order=None):
        """ Create and return a new subgroup.

        Name may be absolute or relative.  Fails if the target name already
        exists.
        """

        if self.read_only:
            raise ValueError("No write intent")

        if track_order is None:
            cfg = config.get_config()
            if cfg.track_order:
                track_order = True
            else:
                track_order = None

        if isinstance(h5path, bytes):
            h5path = h5path.decode('utf-8')

        if h5path is None:
            # anonymous group
            sub_group = self._make_group(track_order=track_order)
            return sub_group

        if h5path[-1] == '/':
            raise ValueError("Invalid path for create_group")
        elif h5path[0] == '/':
            # absolute path
            parent_uuid = self.file.id.id   # uuid of root group
            parent_name = "/"
        else:
            parent_uuid = self.id.id
            parent_name = self._name

        self.log.info(f"create_group: {h5path}")

        links = h5path.split('/')
        sub_group = None  # the object we'll return
        for link in links:
            if not link:
                continue  # skip
            self.log.debug(f"create_group - iterate for link: {link}")
            link_json = self.id.db.getLink(parent_uuid, link)
            if link_json is None:
                # link not found, create a sub-group
                kwargs = {}
                kwargs["parent_id"] = parent_uuid
                kwargs["parent_name"] = parent_name
                kwargs["link"] = link
                kwargs["track_order"] = track_order
                sub_group = self._make_group(**kwargs)
                parent_uuid = sub_group.id.id
            else:
                # sub-group already exists
                self.log.debug(f"create group - found subgroup: {link}")
                if getLinkClass(link_json) != 'H5L_TYPE_HARD':
                    # TBD: get the referenced object for softlink?
                    raise IOError("cannot create subgroup of softlink")
                parent_uuid = link_json["id"]
                if parent_name:
                    if parent_name[-1] == '/':
                        parent_name = parent_name + link
                    else:
                        parent_name = parent_name + '/' + link
                    self.log.debug(f"create group - parent name: {parent_name}")

        if sub_group is None:
            # didn't actually create anything
            raise ValueError("name already exists")

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
        track_oder
            (T/F) List attributes by creation_time if set
        track_times
            (T/F) Enable dataset creation timestamps.
        initializer
            (String) chunk initializer function
        initializer_args
            (List) arguments to be passed to initializer
        """

        if isinstance(name, bytes):
            # convert byte input to string
            name = name.decode("utf-8")

        if "track_order" in kwds:
            track_order = kwds["track_order"]
        else:
            cfg = config.get_config()
            if cfg.track_order:
                track_order = True
            else:
                track_order = None

        datasetId = dataset.make_new_dset(self, shape=shape, dtype=dtype, data=data, **kwds)
        dset = Dataset(datasetId, track_order=track_order)

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
                # create any groups along the path that don't already exist
                for item in grp_path:
                    if item not in grp:
                        grp = grp.create_group(item)
                    else:
                        grp = grp[item]

                    dset._name = dset._name + item + '/'

            dset._name += dset_link
            grp[dset_link] = dset

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
        """
        dcpl_json = other.id.cpl_json
        track_order = None
        if "CreateOrder" in dcpl_json:
            createOrder = dcpl_json["CreateOrder"]
            if not createOrder or createOrder == "0":
                track_order = False
            else:
                track_order = True
        """
        track_order = other.track_order

        kwupdate.setdefault('track_order', track_order)

        # Special case: the maxshape property always exists, but if we pass it
        # to create_dataset, the new dataset will automatically get chunked
        # layout. So we copy it only if it is different from shape.
        if other.maxshape != other.shape:
            kwupdate.setdefault('maxshape', other.maxshape)

        return self.create_dataset(name, **kwupdate)

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
        obj = table.Table(dset.id)
        return obj

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

    def require_group(self, name):
        """ Return a group, creating it if it doesn't exist.

        TypeError is raised if something with that name already exists that
        isn't a group.
        """

        if name not in self:
            return self.create_group(name)
        grp = self[name]
        if not isinstance(grp, Group):
            raise TypeError(f"Incompatible object ({grp.__class__.__name__}) already exists")
        return grp

    def __getitem__(self, name, track_order=None):
        """ Open an object in the file """
        # convert bytes to str for PY3
        if isinstance(name, bytes):
            name = name.decode('utf-8')
        self.log.debug(f"group.__getitem__({name}, track_order={track_order})")

        tgt_id = None
        tgt_json = None
        is_anon = False
        if isinstance(name, Reference):
            tgt_id = str(name)
            tgt_json = self.id.db.getObjectById(tgt_id)
            if not tgt_json:
                raise IOError("reference not found")
            is_anon = True
        elif isValidUuid(name):
            # name should be a uuid in the format <collection>/<uuid>
            # just use the later part as the tgt_id
            collection = getCollectionForId(name)
            if not name.startswith(f"{collection}/"):
                raise IOError(f"Invalid object id for reference: {name}")
            parts = name.split("/")
            tgt_id = parts[1]
            tgt_json = self.id.db.getObjectById(tgt_id)
            if not tgt_json:
                raise IOError("object id not found")
            is_anon = True
        else:
            parent_uuid, link_json = self._get_link_json(name)
            link_class = link_json['class']

            if link_class == 'H5L_TYPE_HARD':
                tgt_id = link_json['id']
                tgt_json = self.id.db.getObjectById(tgt_id)
            elif link_class == 'H5L_TYPE_SOFT':
                h5path = link_json['h5path']
                soft_parent_uuid, soft_json = self._get_link_json(h5path)
                tgt_id = soft_json['id']
                tgt_json = self.id.db.getObjectById(tgt_id)
            elif link_class == 'H5L_TYPE_EXTERNAL':
                # try to get a handle to the file and return the linked object...
                # Note: set use_session to false since file.close won't be called
                #  (and hence the http conn socket won't be closed)
                from .files import File
                external_domain = link_json['file']
                reader = self.id.db.reader

                if not external_domain.startswith("hdf5://") and not op.isabs(external_domain):
                    current_domain = reader.filepath
                    external_domain = op.join(op.dirname(current_domain), external_domain)
                    external_domain = op.normpath(external_domain)
                try:
                    kwargs = {}
                    kwargs["endpoint"] = reader.http_conn.endpoint
                    kwargs["username"] = reader.http_conn.username
                    kwargs["password"] = reader.http_conn.password
                    kwargs["mode"] = 'r'
                    kwargs["track_order"] = track_order

                    f = File(external_domain, **kwargs)
                except IOError:
                    # unable to find external link
                    raise KeyError("Unable to open file: " + link_json['file'])
                return f[link_json['h5path']]
            elif link_class == 'H5L_TYPE_USER_DEFINED':
                raise IOError("Unable to fetch user-defined link")
            else:
                raise IOError("Unexpected error, invalid link class:" + link_json['class'])

        if tgt_id is not None:
            collection = getCollectionForId(tgt_id)
            if collection == 'groups':
                tgt = Group(GroupID(self, tgt_id), track_order=track_order)
            elif collection == 'datatypes':
                tgt = Datatype(TypeID(self, tgt_id), track_order=track_order)
            elif collection == 'datasets':
                # create a Table if the dataset is one dimensional and compound
                shape_json = tgt_json["shape"]
                dtype_json = tgt_json["type"]
                dset_id = DatasetID(self, tgt_id)
                if getRank(shape_json) == 1 and dtype_json["class"] == 'H5T_COMPOUND':
                    tgt = Table(dset_id, track_order=track_order)
                else:
                    tgt = Dataset(dset_id, track_order=track_order)
            else:
                raise IOError(f"Unexpected collection_type: {collection}")

            if is_anon:
                tgt._name = None
            else:
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
        else:
            tgt = None
        return tgt

    def _objectify_link_Json(self, link_json):
        if "id" in link_json:
            link_obj = HardLink(link_json["id"])
        elif "h5path" in link_json and "file" not in link_json:
            link_obj = SoftLink(link_json["h5path"])
        elif "h5path" in link_json and "file" in link_json:
            link_obj = ExternalLink(link_json["file"], link_json["h5path"])
        else:
            raise ValueError("Invalid link JSON")

        return link_obj

    def get(self, name, default=None, getclass=False, getlink=False, track_order=None):
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

        "track_order" is (T/F/None):
            If a group is returned, it's items will be listed by creation order if track_order
            is True and lexicographically if False.  If track_order is not set, the track_order
            used at group creation time will be used.


        Example:

        >>> cls = group.get('foo', getclass=True)
        >>> if cls == SoftLink:
        ...     print '"foo" is a soft link!'
        """
        if not (getclass or getlink):
            try:
                return self.__getitem__(name, track_order=track_order)
            except KeyError:
                return default

        if not name or name == '/':
            return default

        if name[-1] == '/':
            name = name[:-1]  # trailing slash not relevant

        parent_path = op.dirname(name)
        if name[0] == '/':
            parent = self.__getitem('/')
        elif not parent_path:
            parent = self
        else:
            try:
                parent = self.__getitem__(parent_path)
            except KeyError:
                return default

        link_name = op.basename(name)

        obj_id = parent.id.id
        link_json = self.id.db.getLink(obj_id, link_name)
        if not link_json:
            return default

        link_class = getLinkClass(link_json)

        if getclass and not getlink:
            obj = parent.__getitem__(name, track_order=track_order)
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
            if link_class == 'H5L_TYPE_SOFT':
                if getclass:
                    return SoftLink
                else:
                    return SoftLink(link_json['h5path'])
            elif link_class == 'H5L_TYPE_EXTERNAL':
                if getclass:
                    return ExternalLink
                else:
                    # earlier HSDS storage formats usd h5domain, rather than file
                    # so check if either is set
                    if "file" in link_json:
                        link_json_file = link_json["file"]
                    elif "h5domain" in link_json:
                        link_json_file = link_json["h5domain"]
                    else:
                        raise KeyError(f"Unexpected link format: {link_json}")
                    if "h5path" in link_json:
                        link_json_path = link_json["h5path"]
                    else:
                        raise KeyError(f"Unexpected link format: {link_json}")
                    return ExternalLink(link_json_file, link_json_path)
            elif link_class == 'H5L_TYPE_HARD':
                return HardLink if getclass else HardLink(link_json['id'])
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

        db = self.id.db

        if name.find('/') != -1:
            parent_path = op.dirname(name)
            basename = op.basename(name)
            if not basename:
                raise KeyError("Group path can not end with '/'")
            parent_uuid, link_json = self._get_link_json(parent_path)
            if parent_uuid is None:
                raise KeyError(f"group path: {parent_path} not found")
            if link_json["class"] != 'H5L_TYPE_HARD':
                raise IOError("cannot create subgroup of softlink")
            parent_uuid = link_json["id"]
            group_json = db.getObjectById(parent_uuid)
            tgt = Group(GroupID(self, parent_uuid, obj_json=group_json))
            tgt[basename] = obj

            return

        # create a direct link, but first check to see if there's already a link here
        if db.getLink(self.id.uuid, name):
            raise IOError("Unable to create link (name already exists)")

        if isinstance(obj, HLObject):
            db.createHardLink(self.id.uuid, name, obj.id.uuid)

        elif isinstance(obj, SoftLink):
            db.createSoftLink(self.id.uuid, name, obj.path)

        elif isinstance(obj, ExternalLink):
            db.createExternalLink(self.id.uuid, name, obj.path, obj.filename)

        elif isinstance(obj, numpy.dtype):
            ctype_id = db.createCommittedType(obj)
            db.createHardLink(self.id.uuid, name, ctype_id)

        else:
            if isinstance(obj, numpy.ndarray):
                arr = obj
            elif isinstance(obj, str):
                dt = special_dtype(vlen=str)
                arr = numpy.array(obj, dtype=dt)
            else:
                dt = guess_dtype(obj)
                arr = numpy.array(obj, dtype=dt)
                self.create_dataset(name, shape=arr.shape, dtype=arr.dtype, data=arr[...])

    def __delitem__(self, name):
        """ Delete (unlink) an item from this group. """

        if self.read_only:
            raise ValueError("No write intent")

        if isValidUuid(name):
            # delete the actual object
            # TBD: construct a rererence object, or at least consolidate the code
            # to extract the tgt_id with the code in __get__
            collection = getCollectionForId(name)
            if not name.startswith(f"{collection}/"):
                raise IOError(f"Invalid object id for reference: {name}")
            parts = name.split("/")
            tgt_id = parts[1]
            obj_json = self.id.db.getObjectById(tgt_id)
            if obj_json:
                # remove object
                self.id.db.deleteObject(tgt_id)
            else:
                raise IOError("Not found")

        else:
            # delete the link(s), not an object
            self.id.db.deleteLink(self.id.uuid, name)

    def __len__(self):
        """ Number of members attached to this group """
        # we can avoid a server request and just count the links in the obj json
        titles = self.id.db.getLinks(self.id.uuid)

        return len(titles)

    def __iter__(self):
        """ Iterate over member names """
        titles = self.id.db.getLinks(self.id.uuid)
        links = {}
        for title in titles:
            links[title] = self.id.db.getLink(self.id.uuid, title)

        track_order = None
        if self._track_order is not None:
            track_order = self._track_order
        elif self.id.create_order is not None:
            track_order = self.id.create_order
        else:
            track_order = False

        if track_order:
            links = sorted(links.items(), key=lambda x: x[1]['created'])
        else:
            links = sorted(links.items())

        ordered_links = {}
        for link in links:
            ordered_links[link[0]] = link[1]

        for name in ordered_links:
            yield name

    def __contains__(self, name):
        """ Test if a member name exists """
        found = False
        try:
            self._get_link_json(name)
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
                titles = self.id.db.getLinks(parent.id.uuid)

                for title in titles:
                    link = self.id.db.getLink(parent.id.uuid, title)
                    obj = None
                    if link['class'] == 'H5L_TYPE_SOFT':
                        # obj = SoftLink(link['h5path'])
                        pass  # don't visit soft links'
                    elif link['class'] == 'H5L_TYPE_EXTERNAL':
                        # obj = ExternalLink(link['file'], link['h5path'])
                        pass  # don't visit external links'
                    elif link['class'] == 'H5L_TYPE_UDLINK':
                        obj = UserDefinedLink()
                    elif link['class'] == 'H5L_TYPE_HARD':
                        if link['id'] in visited:
                            continue  # already been there
                        obj = parent.__getitem__(link['title'])
                        tovisit[obj.id.uuid] = obj
                        obj = None
                    if obj is not None:
                        # call user func directly for non-hardlinks
                        link_name = parent.name + '/' + title
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

    def __reversed__(self):
        """ Iterate over member names in reverse order """
        titles = self.id.db.getLinks(self.id.uuid)
        links = {}
        for title in titles:
            links[title] = self.id.db.getLink(self.id.uuid, title)

        if self.id.create_order:
            links = sorted(links.items(), key=lambda x: x[1]['created'])
        else:
            links = sorted(links.items())

        ordered_links = {}
        for link in links:
            ordered_links[link[0]] = link[1]

        for name in reversed(ordered_links):
            yield name

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
        return f'<ExternalLink to "{self.path}" in file "{self.filename}">'


class UserDefinedLink(object):

    """
        Represents a user-defined link
    """

    def __init__(self):
        pass

    def __repr__(self):
        return '<UDLink >'
