from setuptools import setup
# run:
#   setup.py install
# or (if you'll be modifying the package):
#   setup.py develop
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()
    
setup(name='h5pyd',
      version='0.1.0',
      description='h5py compatible client lib for HDF REST API',
      long_description=long_description,
      url='http://github.com/HDFGroup/h5pyd',
      author='John Readey',
      author_email='jreadey@hdfgrouup.org',
      license='BSD',
      packages = ['h5pyd', 'h5pyd._hl'],
      #requires = ['h5py (>=2.5.0)', 'h5json>=1.0.2'],
      install_requires = ['h5py>=2.5.0',  'h5json>=1.0.2', 'six'],
      setup_requires = ['h5py>=2.5.0', 'pkgconfig', 'six'],
      zip_safe=False)
