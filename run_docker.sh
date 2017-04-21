#!/bin/sh
if [ $# -eq 0 ]; then
   ARGS=/bin/bash
else
   ARGS=$*
fi
docker run --rm -it -v ${HOME}/.hscfg:/root/.hscfg hdfgroup/h5pyd $ARGS
