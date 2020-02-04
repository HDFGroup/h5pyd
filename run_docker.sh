#!/bin/sh
if [ $# -eq 0 ]; then
   ARGS=/bin/bash
else
   ARGS=$*
fi
docker run --rm -v ${HOME}/.hscfg:/root/.hscfg --env AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY --env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} -it hdfgroup/h5pyd:0.7.0  $ARGS
