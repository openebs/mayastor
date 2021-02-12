#!/usr/bin/env bash

set -xe

rm -rf rsa/
mkdir -p rsa/

openssl req -nodes \
          -x509 \
          -days 3650 \
          -newkey rsa:4096 \
          -keyout rsa/ca.key \
          -out rsa/ca.cert \
          -sha256 \
          -batch \
          -subj "/CN=testserver RSA CA"

openssl req -nodes \
          -newkey rsa:2048 \
          -keyout rsa/user.key \
          -out rsa/user.req \
          -sha256 \
          -batch \
          -subj "/CN=testserver.com"

openssl rsa \
          -in rsa/user.key \
          -out rsa/user.rsa

openssl x509 -req \
          -in rsa/user.req \
          -out rsa/user.cert \
          -CA rsa/ca.cert \
          -CAkey rsa/ca.key \
          -sha256 \
          -days 3650 \
          -set_serial 123 \
          -extensions v3_user -extfile openssl.cnf

cat rsa/user.cert rsa/ca.cert > rsa/user.chain
