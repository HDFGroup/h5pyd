Configuring h5pyd
=================

Library configuration
---------------------

A few library options are available to change the behavior of the library.
You can get a reference to the global library configuration object via the
function :func:`h5py.get_config`.  This object supports the following
attributes:

    **complex_names**
        Set to a 2-tuple of strings (real, imag) to control how complex numbers
        are saved.  The default is ('r','i').

    **bool_names**
        Booleans are saved as HDF5 enums.  Set this to a 2-tuple of strings
        (false, true) to control the names used in the enum.  The default
        is ("FALSE", "TRUE").

    **track_order**
        Controls whether links and attributes should be listed by
        creation order (rather than alphanumerically).  If
        track_order is True when an object is created, its links and attributes
        are iterated in ascending creation order (consistent with
        ``dict`` in Python 3.7+); otherwise in ascending alphanumeric
        order.  Global configuration value can be overridden for
        particular container by specifying ``track_order`` argument to
        :class:`h5py.File`, :meth:`h5py.Group.create_group`,
        :meth:`h5py.Group.create_dataset`.  The default is ``False``.
