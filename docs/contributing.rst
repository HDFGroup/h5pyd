Bug Reports & Contributions
===========================

Contributions and bug reports are welcome from anyone!  

Since we use GitHub, the workflow will be familiar to many people.
If you have questions about the process or about the details of implementing
your feature, feel free to ask on Github itself, or on the h5py section of the
HDF5 forum:

    https://forum.hdfgroup.org/c/hdf5/h5pyd

Posting on this forum requires registering for a free account with HDF group.

Anyone can post to this list. Your first message will be approved by a
moderator, so don't worry if there's a brief delay.

This guide is divided into three sections.  The first describes how to file
a bug report.

The second describes the mechanics of
how to submit a contribution to the h5pys project; for example, how to
create a pull request, which branch to base your work on, etc.
We assume you're are familiar with Git, the version control system used by h5py.
If not, `here's a great place to start <https://git-scm.com/book>`_.

Finally, we describe the various subsystems inside h5py, and give
technical guidance as to how to implement your changes.


How to File a Bug Report
------------------------

Bug reports are always welcome!  The issue tracker is at:

    https://github.com/h5py/h5pyd/issues


If you're unsure whether you've found a bug
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Always feel free to ask on the forum.
Even if the issue you're having turns out not to be a bug in the end, other
people can benefit from a record of the conversation.

By the way, nobody will get mad if you file a bug and it turns out to be
something else.  That's just how software development goes.


What to include
~~~~~~~~~~~~~~~

When filing a bug, there are two things you should include.  The first is
the output of ``hsinfo``  This will show the HSDS and h5pyd version info.
d
The second is a detailed explanation of what went wrong.  Unless the bug
is really trivial, **include code if you can**, either via GitHub's
inline markup::

    ```
        import h5pyd
        h5pyd.explode()    # Destroyed my computer!
    ```

or by uploading a code sample to `Github Gist <http://gist.github.com>`_.

If you have access to the HSDS log output, searching that for relevant
log output can be helpful.  Often times an error on h5pyd will be related
to a WARN or ERROR line in the log output.  If you find something, include
that in your issue as well.

How to Get Your Code into h5pyd
-------------------------------

This section describes how to contribute changes to the h5pyd code base.
Before you start, be sure to read the h5pyd license and contributor
agreement in "license.txt".  You can find this in the source distribution,
or view it online at the main h5py repository at GitHub.

The basic workflow is to clone h5py with git, make your changes in a topic
branch, and then create a pull request at GitHub asking to merge the changes
into the main h5py project.

Here are some tips to getting your pull requests accepted:

1. Let people know you're working on something.  This could mean posting a
   comment in an open issue, or sending an email or posting to the forum.  There's
   nothing wrong with just opening a pull request, but it might save you time
   if you ask for advice first.
2. Keep your changes focused.  If you're fixing multiple issues, file multiple
   pull requests.  Try to keep the amount of reformatting clutter small so
   the maintainers can easily see what you've changed in a diff.
3. Test code is mandatory for new features.  This doesn't mean hundreds
   (or even dozens) of tests!  Just enough to make sure the feature works as
   advertised.  The maintainers will let you know if more are needed.
4. If your feature requires changes to HSDS, submit a PR to https://github.com/HDFGroup/HSDS
   first.  The h5pyd code for the feature should check that the HSDS instance supports
   the feature (typically by checking the HSDS version), and fail gracefully if
   the HSDS instance has not been updated yet.


.. _git_checkout:

Clone the h5pyd repository
~~~~~~~~~~~~~~~~~~~~~~~~~~

The best way to do this is by signing in to GitHub and cloning the
h5pyd project directly.  You'll end up with a new repository under your
account; for example, if your username is ``yourname``, the repository
would be at http://github.com/yourname/h5pyd.

Then, clone your new copy of h5pyd to your local machine::

    $ git clone http://github.com/yourname/h5pyd


Create a topic branch for your feature
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check out a new branch for the bugfix or feature you're writing::

    $ git checkout -b newfeature master

The exact name of the branch can be anything you want.  For bug fixes, one
approach is to put the issue number in the branch name.

We develop all changes against the *master* branch.
If we're making a bugfix release, a bot will backport merged pull requests.


Implement the feature!
~~~~~~~~~~~~~~~~~~~~~~

You can implement the feature as a number of small changes, or as one big
commit; there's no project policy.  Double-check to make sure you've
included all your files; run ``git status`` and check the output.

.. _contrib-run-tests:

Run the tests
~~~~~~~~~~~~~

The easiest way to run the tests is with
`$ python testall.py`.
If you've added a new test script, you'll needed to add to the list
of tests in testall.py.

Update the documentation
~~~~~~~~~~~~~~~~~~~~~~~~

New features (and sometimes bug fixes) will require edits to the h5pyd documentation.
Modify the appropriate rst in the docs directory and include these changes with your PR.


Push your changes back and open a pull request
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Push your topic branch back up to your GitHub clone::

    $ git push origin newfeature

Then, `create a pull request <https://help.github.com/articles/creating-a-pull-request>`_ based on your topic branch.


Work with the maintainers
~~~~~~~~~~~~~~~~~~~~~~~~~

Your pull request might be accepted right away.  More commonly, the maintainers
will post comments asking you to fix minor things, like add a few tests, clean
up the style to be PEP-8 compliant, etc.

The pull request page also shows the results of building and testing the
modified code in the Github Workflows.
Check back after about 30 minutes to see if the build succeeded,
and if not, try to modify your changes to make it work.

When making changes after creating your pull request, just add commits to
your topic branch and push them to your GitHub repository.  Don't try to
rebase or open a new pull request!  We don't mind having a few extra
commits in the history, and it's helpful to keep all the history together
in one place.
