#!/usr/bin/env bash

# Copy rpc proto files from other locations in repository to a proto subdir
# of the moac. This is impotant when creating a nix and npm package of the
# moac. Before changing anything here make sure you understand the code in
# default.nix.

[ -n "$csiProto" ] || csiProto=../proto/csi.proto
[ -n "$mayastorProto" ] || mayastorProto=../../rpc/proto/mayastor.proto
[ -n "$mayastorServiceProto" ] || mayastorServiceProto=../../rpc/proto/mayastor_service.proto

echo "Adding RPC proto files to the bundle ..."
mkdir -p proto || exit 1
cp -f "$csiProto" proto/csi.proto || exit 1
cp -f "$mayastorProto" proto/mayastor.proto || exit 1
cp -f "$mayastorServiceProto" proto/mayastor_service.proto || exit 1
echo "Done"
