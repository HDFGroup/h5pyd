from setuptools import setup
# run:
#   setup.py install
# or (if you'll be modifying the package):
#   setup.py develop
# To use a consistent encoding
# To upload to PyPI:
# python setup.py register sdist upload
#
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='h5pyd',
      version='0.2.2',
      description='h5py compatible client lib for HDF REST API',
      long_description=long_description,
      url='http://github.com/HDFGroup/h5pyd',
      author='John Readey',
      author_email='jreadey@hdfgrouup.org',
      license='BSD',
      packages=['h5pyd', 'h5pyd._hl', 'h5pyd._apps'],
      # requires=['h5py (>=2.5.0)', 'h5json>=1.0.2'],
      install_requires=['numpy >= 1.10.4', 'requests', 'six', 'pytz'],
      setup_requires=['pkgconfig', 'six'],
      zip_safe=False,
      entry_points={'console_scripts':
          ['hsinfo = h5pyd._apps.hsinfo:main', 
           'hsls = h5pyd._apps.hsls:main',
           'hstouch = h5pyd._apps.hstouch:main',
           'hsacl = h5pyd._apps.hsacl:main',
           'hsdel = h5pyd._apps.hsdel:main',
           'hsget = h5pyd._apps.hsget:main',
           'hsload = h5pyd._apps.hsload:main']
      },
)  
