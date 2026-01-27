#!/bin/bash

make_node_cert () {
	NODE_NAME=$1
	HOST_NAME=$2
	openssl genrsa -out ${NODE_NAME}.key 2048
	openssl req -new -key ${NODE_NAME}.key -out ${NODE_NAME}.csr -subj "/CN=${HOST_NAME}"
	openssl x509 -req -days 365 -in ${NODE_NAME}.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out ${NODE_NAME}.crt
}

# Make CA key and cert.
openssl genrsa -out ca.key 2048
openssl req -new -x509 -days 365 -key ca.key -out ca.crt -subj "/CN=DemoCA"

# Make node keys and certs.
make_node_cert A localhost
make_node_cert B localhost
make_node_cert C localhost
make_node_cert D localhost
