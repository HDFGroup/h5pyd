.. currentmodule:: h5pyd
.. _group:


Groups
======


Groups are the container mechanism by which HSDS domains (ans well 
as HDF5 files) are organized.  
From a Python perspective, they operate somewhat like dictionaries.  In this case
the "keys" are the names of group members, and the "values" are the members
themselves (:class:`Group` and :class:`Dataset`) objects.

Group objects also contain most of the machinery which makes HDF5 useful.
The :ref:`File object <file>` does double duty as the HDF5 *root group*, and
serves as your entry point into the domain:

    >>> f = h5py.File('/home/test_user1/test/foo.hdf5','w')
    >>> f.name
    '/'
    >>> list(f.keys())
    []

Names of all objects in the domain are all text strings (``str``).  
These will be encoded using UTF-8 for transmission in http requests and by
HSDS when written to the storage medium.

.. note::

    Objects can be retrieved using byte strings, but these will decoded using 
    UTF8 before sending the request to HSDS.


.. _group_create:

Creating groups
---------------

New groups are easy to create::

    >>> grp = f.create_group("bar")
    >>> grp.name
    '/bar'
    >>> subgrp = grp.create_group("baz")
    >>> subgrp.name
    '/bar/baz'

Multiple intermediate groups can also be created implicitly::

    >>> grp2 = f.create_group("/some/long/path")
    >>> grp2.name
    '/some/long/path'
    >>> grp3 = f['/some/long']
    >>> grp3.name
    '/some/long'

.. _group_anonymous:

Anonymous groups
----------------

An anonymous group (a group that nothing links to)
can be created by using ``None`` as the group name.
The new group can either be set as a link target later,
or kept as a "hidden" group of the domain.

An anonymous group can be accessed using it's low-level id as
in this example:

    >>> anon_grp = f.create_group(None)
    >>> grp = f.getObjByUuid(anon_grp.id.id)  # another reference to anon_grp
    >>> f["g1"] = anon_grp  # link the grpup as "g1" of the root group

.. note::

    Unlike with HDF5, anonymous objects won't be released when the file is closed.
    They will need to be explicitly deleted if they are desired to be
    used temporarily.  


.. _group_links:

Dict interface and links
------------------------

Groups implement a subset of the Python dictionary convention.  They have
methods like ``keys()``, ``values()`` and support iteration.  Most importantly,
they support the indexing syntax, and standard exceptions:

    >>> myds = subgrp["MyDS"]
    >>> missing = subgrp["missing"]
    KeyError: "Name doesn't exist (Symbol table: Object not found)"

.. note::
    The keys(), values() and items() methods
    will return view-like objects instead of lists.  These objects support
    membership testing and iteration, but can't be sliced like lists.

By default, objects inside a group are iterated in alphanumeric order.
However, if group is created with ``track_order=True``, the insertion
order for the group is remembered (tracked) in the domain, and group
contents are iterated in that order.  The latter is consistent with
Python 3.7+ dictionaries.

The default ``track_order`` for all new groups can be specified
globally with ``h5pyd.get_config().track_order``.

Links can be deleted from a group using the standard Python syntax::

    >>> del subgroup["MyDataset"]

.. note::

    Unlike with h5py and HDF5, in h5pyd deleting the last link to an object will
    not cause the target object to be deleted.  Instead the object needs to be
    explicitly deleted using its UUID rather than the link name.  
    See the example below.

To delete the object a link refers to, pass the UUID identifier of the 
object as the argument:

    >>> g1 = f.create_group('g1')  # create a new object
    >>> del f[g1.id.id]            # now delete the object
    >>> 'g1' in f                  # link "g1" still exists
    >>> del f['g1']                # delete the link

.. _group_hardlinks:

Hard links
~~~~~~~~~~

What happens when assigning an object to a name in the group?  It depends on
the type of object being assigned.  For NumPy arrays or other data, the default
is to create an :ref:`HDF5 datasets <dataset>`::

    >>> grp["name"] = 42
    >>> out = grp["name"]
    >>> out
    <HDF5 dataset "name": shape (), type "<i8">

If the object is a Numpy datatype, the default is to create a committed datatype object.

    >> dt = numpy.dtype("int32")
    >> grp["name"] = dt
    >> out = grp["name"]
    >> out
    <HDF5 named type "named_type" (dtype <i4)>

When the object being stored is an existing Group or Dataset, a new link is
made to the object::

    >>> grp["other name"] = out
    >>> grp["other name"]
    <HDF5 dataset "other name": shape (), type "<i8">

Note that this is `not` a copy of the dataset!  Like hard links in a UNIX file
system, objects in an HDF5 file can be stored in multiple groups::

    >>> grp["other name"] == grp["name"]
    True


.. _group_softlinks:

Soft links
~~~~~~~~~~

Also like a UNIX filesystem, HDF5 groups can contain "soft" or symbolic links,
which contain a text path instead of a pointer to the object itself.  You
can easily create these in h5pyd by using ``h5pyd.SoftLink``::

    >>> myfile = h5pyd.File('/home/test_user1/foo.hdf5','w')
    >>> group = myfile.create_group("somegroup")
    >>> myfile["alias"] = h5pyd.SoftLink('/somegroup')

If the target is removed, they will "dangle":

    >>> del myfile['somegroup']
    >>> print(myfile['alias'])
    KeyError: 'Component not found (Symbol table: Object not found)'


.. _group_extlinks:

External links
~~~~~~~~~~~~~~

External links are "soft links plus", which allow you to
specify the name of the file as well as the path to the desired object.  You
can refer to objects in any file you wish.  Use similar syntax as for soft
links:

    >>> f = h5pyd.File('/home/test_user1/foo.hdf5','w')
    >>> f['ext link'] = h5pyd.ExternalLink("/home/test_user1/otherfile.hdf5", "/path/to/resource")

When the link is accessed, the domain "/home/test_user1/otherfile.hdf5" is opened, and object at
"/path/to/resource" is returned.

Since the object retrieved is in a different domain, its ".file" and ".parent"
properties will refer to objects in that domain, *not* the domain in which the
link resides.

.. note::

    To specify an externlink to a domain in different bucket, pre-append the 
    target bucket name to the external path.  E.g. ``otherbucket/home/test_user1/otherfile.hdf5``


Reference
---------

.. class:: Group(identifier)

    Generally Group objects are created by opening objects in the domain, or
    by the method :meth:`Group.create_group`.   

    .. method:: __iter__()

        Iterate over the names of the links in the group.
        Use :meth:`Group.visit` or :meth:`Group.visititems` for recursive
        access to group members.

    .. method:: __contains__(name)

        Dict-like membership testing.  `name` may be a relative or absolute
        path.

    .. method:: __getitem__(name)

        Retrieve an object.  `name` may be a relative or absolute path, or
        an :ref:`object or region reference <refs>`. See :ref:`group_links`.

    .. method:: __setitem__(name, value)

        Create a new link, or automatically create a dataset.
        See :ref:`group_links`.

    .. method:: __bool__()

        Check that the group is accessible.
        Will always return True for a valid group reference

    .. method:: keys()

        Get the names of directly attached group members.
        Use :meth:`Group.visit` or :meth:`Group.visititems` for recursive
        access to group members.

       :return: set-like object.

    .. method:: values()

        Get the objects contained in the group (Group and Dataset instances).
        Broken soft or external links show up as None.

        :return: a collection or bag-like object.

    .. method:: items()

        Get ``(name, value)`` pairs for object directly attached to this group.
        Values for broken soft or external links show up as None.

        :return: a set-like object.

    .. method:: get(name, default=None, getclass=False, getlink=False)

        Retrieve an item, or information about an item.  `name` and `default`
        work like the standard Python ``dict.get``.

        :param name:    Name of the object to retrieve.  May be a relative or
                        absolute path.
        :param default: If the object isn't found, return this instead.
        :param getclass:    If True, return the class of object instead;
                            :class:`Group` or :class:`Dataset`.
        :param getlink: If true, return the type of link via a :class:`HardLink`,
                        :class:`SoftLink` or :class:`ExternalLink` instance.
                        If ``getclass`` is also True, returns the corresponding
                        Link class without instantiating it.

    .. method:: visit(callable)

        Recursively visit all objects in this group and subgroups.  You supply
        a callable with the signature::

            callable(name) -> None or return value

        `name` will be the name of the object relative to the current group.
        Return None to continue visiting until all objects are exhausted.
        Returning anything else will immediately stop visiting and return
        that value from ``visit``::

            >>> def find_foo(name):
            ...     """ Find first object with 'foo' anywhere in the name """
            ...     if 'foo' in name:
            ...         return name
            >>> group.visit(find_foo)
            'some/subgroup/foo'


    .. method:: visititems(callable)

        Recursively visit all objects in this group and subgroups.  Like
        :meth:`Group.visit`, except your callable should have the signature::

            callable(name, object) -> None or return value

        In this case `object` will be a :class:`Group` or :class:`Dataset`
        instance.

    .. method:: visit_links(callable)
                visititems_links(callable)

       These methods are like :meth:`visit` and :meth:`visititems`, but work on
       the links in groups, rather than the objects those links point to. So if
       you have two links pointing to the same object, these will 'see' both.
       They also see soft & external links, which :meth:`visit` and
       :meth:`visititems` ignore.

       The second argument to the callback for ``visititems_links`` is an
       instance of one of the :ref:`link classes <group_link_classes>`.

    .. method:: move(source, dest)

        Move an object or link in the file.  If `source` is a hard link, this
        effectively renames the object.  If a soft or external link, the
        link itself is moved.

        :param source:  Name of object or link to move.
        :type source:   String
        :param dest:    New location for object or link.
        :type dest:   String

        .. note::

            This method is not yet supported, and will raise an error if invoked.


    .. method:: copy(source, dest, name=None, shallow=False, expand_soft=False, expand_external=False, expand_refs=False, without_attrs=False)

        Copy an object or group.  The source can be a path, Group, Dataset, or
        Datatype object.  The destination can be either a path or a Group
        object.  The source and destination need not be in the same file.

        If the source is a Group object, by default all objects within that
        group will be copied recursively.

        When the destination is a Group object, by default the target will be
        created in that group with its current name (basename of obj.name). You
        can override that by setting "name" to a string.

        :param source:  What to copy.  May be a path in the file or a Group/Dataset object.
        :param dest:    Where to copy it.  May be a path or Group object.
        :param name:    If the destination is a Group object, use this for the
                        name of the copied object (default is basename).
        :param shallow: Only copy immediate members of a group.
        :param expand_soft: Expand soft links into new objects.
        :param expand_external: Expand external links into new objects.
        :param expand_refs: Copy objects which are pointed to by references.
        :param without_attrs:   Copy object(s) without copying HDF5 attributes.

        .. note::

            This method is not yet supported, and will raise an error if invoked.



    .. method:: create_group(name, track_order=None)

        Create and return a new group in the file.

        :param name:    Name of group to create.  May be an absolute
                        or relative path.  Provide None to create an anonymous
                        group, to be linked into the file later.
        :type name:     String or None
        :param track_order:  Track dataset/group/attribute creation order under
                        this group if ``True``.  Default is
                        ``h5.get_config().track_order``.

        :return:        The new :class:`Group` object.


    .. method:: require_group(name)

        Open a group in the file, creating it if it doesn't exist.
        TypeError is raised if a conflicting object already exists.
        Parameters as in :meth:`Group.create_group`.


    .. method:: create_dataset(name, shape=None, dtype=None, data=None, **kwds)

        Create a new dataset.  Options are explained in :ref:`dataset_create`.

        :param name:    Name of dataset to create.  May be an absolute
                        or relative path.  Provide None to create an anonymous
                        dataset, to be linked into the file later.

        :param shape:   Shape of new dataset (Tuple).

        :param dtype:   Data type for new dataset

        :param data:    Initialize dataset to this (NumPy array).

        :keyword chunks:    Chunk shape, or True for auto-chunking.

        :keyword maxshape:  Dataset will be resizable up to this shape (Tuple).
                            Automatically enables chunking.  Use None for the
                            axes you want to be unlimited.

        :keyword compression:   Compression strategy.  See :ref:`dataset_compression`.

        :keyword compression_opts:  Parameters for compression filter.

        :keyword scaleoffset:   See :ref:`dataset_scaleoffset`.

        :keyword shuffle:   Enable shuffle filter (T/**F**).  See :ref:`dataset_shuffle`.

        :keyword fletcher32: Enable Fletcher32 checksum (T/**F**).  See :ref:`dataset_fletcher32`.

        :keyword fillvalue: This value will be used when reading
                            uninitialized parts of the dataset.


        :keyword track_order: Track attribute creation order if
                        ``True``.  Default is
                        ``h5.get_config().track_order``.

   
        :keyword initializer: Dataset initializer method - a method that will be
                         invoked each time a dataset chunk is initialized.  methods
                         currently available: arange, or None for no initializer

        :keyword initializer_args: List of arguments for dataset initializer args


    .. method:: require_dataset(name, shape, dtype, exact=False, **kwds)

        Open a dataset, creating it if it doesn't exist.

        If keyword "exact" is False (default), an existing dataset must have
        the same shape and a conversion-compatible dtype to be returned.  If
        True, the shape and dtype must match exactly.

        If keyword "maxshape" is given, the maxshape and dtype must match
        instead.

        If any of the keywords "rdcc_nslots", "rdcc_nbytes", or "rdcc_w0" are
        given, they will be used to configure the dataset's chunk cache.

        Other dataset keywords (see create_dataset) may be provided, but are
        only used if a new dataset is to be created.

        Raises TypeError if an incompatible object already exists, or if the
        shape, maxshape or dtype don't match according to the above rules.

        :keyword exact:     Require shape and type to match exactly (T/**F**)


    .. method:: create_dataset_like(name, other, **kwds)

        Create a dataset similar to `other`, much like numpy's `_like` functions.

        :param name:
            Name of the dataset (absolute or relative).  Provide None to make
            an anonymous dataset.
        :param other:
            The dataset whom the new dataset should mimic. All properties, such
            as shape, dtype, chunking, ... will be taken from it, but no data
            or attributes are being copied.

        Any dataset keywords (see create_dataset) may be provided, including
        shape and dtype, in which case the provided values take precedence over
        those from `other`.

    .. method:: create_virtual_dataset(name, layout, fillvalue=None)

       Create a new virtual dataset in this group. See :doc:`/vds` for more
       details.

       :param str name:
           Name of the dataset (absolute or relative).
       :param VirtualLayout layout:
           Defines what source data fills which parts of the virtual dataset.
       :param fillvalue:
           The value to use where there is no data.

        .. note: 

            This is a place holder method until Virtual Datasets are supported.
            Invoking the method will raise an error

    .. method:: build_virtual_dataset()

       Assemble a virtual dataset in this group.

       This is used as a context manager::

           with f.build_virtual_dataset('virt', (10, 1000), np.uint32) as layout:
               layout[0] = h5py.VirtualSource('foo.h5', 'data', (1000,))

       Inside the context, you populate a :class:`VirtualLayout` object.
       The file is only modified when you leave the context, and if there's
       no error.

       :param str name: Name of the dataset (absolute or relative)
       :param tuple shape: Shape of the dataset
       :param dtype: A numpy dtype for data read from the virtual dataset
       :param tuple maxshape: Maximum dimensions if the dataset can grow
           (optional). Use None for unlimited dimensions.
       :param fillvalue: The value used where no data is available.

        .. note: 

            This is a place holder method until Virtual Datasets are supported.
            Invoking the method will raise an error

    .. attribute:: attrs

        :ref:`attributes` for this group.

    .. attribute:: id

        The groups's low-level identifier; an instance of
        :class:`GroupID <low:h5pyd.GroupID>`.

    .. attribute:: ref

        An HDF5 object reference pointing to this group.  See
        :ref:`refs_object`.

    .. attribute:: regionref

        A proxy object allowing you to interrogate region references.
        See :ref:`refs_region`.

    .. attribute:: name

        String giving the full path to this group.

    .. attribute:: file

        :class:`File` instance in which this group resides.

    .. attribute:: parent

        :class:`Group` instance containing this group.

.. _group_link_classes:

Link classes
------------

.. class:: HardLink()

    Exists only to support :meth:`Group.get`.  Has no state and provides no
    properties or methods.

.. class:: SoftLink(path)

    Exists to allow creation of soft links in the file.
    See :ref:`group_softlinks`.  These only serve as containers for a path;
    they are not related in any way to a particular file.

    :param path:    Value of the soft link.
    :type path:     String

    .. attribute:: path

        Value of the soft link

.. class:: ExternalLink(filename, path)

    Like :class:`SoftLink`, only they specify a filename in addition to a
    path.  See :ref:`group_extlinks`.

    :param filename:    Path to the domain to which the link points
    :type filename:     String

    :param path:        Path to the object in the external domain.
    :type path:         String

    .. attribute:: filename

        Path to a domain as a Unicode string

    .. attribute::  path

        Path to the object in the external file
