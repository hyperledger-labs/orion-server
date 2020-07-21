#!/bin/bash

export WORKING_DIR=`pwd`
echo "> Working dir: $WORKING_DIR"
pushd .
cd ..
echo "> Getting library..."
git clone git@github.ibm.com:blockchaindb/library.git
echo "> Getting protos..."
git clone git@github.ibm.com:blockchaindb/protos-go.git

popd