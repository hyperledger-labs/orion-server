#!/bin/bash

set -eu -o pipefail

pushd .
cd api

repo="."

if [ ! -d "$repo" ]; then
  mkdir "$repo"
fi

for protos in $(find . -name '*.proto' -exec dirname {} \; | sort -u); do
  protoc "--go_out=plugins=grpc,paths=source_relative:$repo" "$protos"/*.proto
done

popd

chown -R --reference=api api/*

