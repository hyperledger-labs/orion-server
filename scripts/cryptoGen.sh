#! /bin/bash

BASE_DIR=$1

rm -rf "$BASE_DIR/pki"

echo "Creating PKIs folders"
mkdir -p "$BASE_DIR/pki/ca"

echo "Generate root CA private key"
openssl ecparam -name prime256v1 -genkey -noout -out "$BASE_DIR/pki/ca/rootCA.key"

echo "Generating self-signed root CA certificate"
openssl req -new -x509 -nodes -key "$BASE_DIR/pki/ca/rootCA.key" -sha256 -days 365 -out "$BASE_DIR/pki/ca/rootCA.cert" -subj "/C=IL/ST=Haifa/O=BCDB" -extensions v3_ca

for f in "node" "admin" "user"
do
    echo "Creating PKIs folders"
    mkdir -p "$BASE_DIR/pki/$f"

    echo "Generating private key for $f"
    openssl ecparam -name prime256v1 -genkey -noout -out "$BASE_DIR/pki/$f/$f.key"

    echo "Generate node CSR"
    openssl req -new -key "$BASE_DIR/pki/$f/$f.key" -out "$BASE_DIR/pki/$f/$f.csr" -subj "/C=IL/ST=Haifa/O=BCDB"

    echo "Generate node certificate"
    openssl x509 -req -in "$BASE_DIR/pki/$f/$f.csr" -CA "$BASE_DIR/pki/ca/rootCA.cert" -CAkey "$BASE_DIR/pki/ca/rootCA.key" -CAcreateserial -out "$BASE_DIR/pki/$f/$f.cert" -days 365 -sha256
done
