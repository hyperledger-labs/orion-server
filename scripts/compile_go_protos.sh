#!/bin/bash

set -euo pipefail

cd protos
mkdir -p ../pkg/types

for protos in $(find . -name '*.proto' -exec dirname {} \; | sort -u); do
  protoc \
    --proto_path . \
    --go_out=../pkg/types \
    --go_opt=paths=source_relative \
    $protos/*.proto
done

cd ../internal/blockstore
 protoc  \
   --proto_path . \
   --proto_path ../../protos \
   --go_out=. \
   --go_opt=paths=source_relative \
   *.proto

