FROM hdfgroup/h5py:2.7.0
MAINTAINER John Readey <jreadey@hdfgroup.org>
RUN mkdir /usr/local/src/hsds/ /usr/local/src/h5pyd/
COPY h5pyd /usr/local/src/h5pyd/h5pyd
COPY test /usr/local/src/h5pyd/test
COPY testall.py /usr/local/src/h5pyd/
COPY setup.py /usr/local/src/h5pyd/
COPY README.rst /usr/local/src/h5pyd/
RUN cd /usr/local/src/h5pyd                               ; \
     pip install requests                                 ; \
     python setup.py install 
WORKDIR /usr/local/src/h5pyd/apps