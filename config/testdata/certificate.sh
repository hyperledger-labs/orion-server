#!/bin/bash

echo "authorityKeyIdentifier=keyid,issuer" > v3.ext
echo "basicConstraints=CA:FALSE" >> v3.ext
echo "keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment" >> v3.ext 

echo "[req]
default_bits = 4096
prompt = no
default_md = sha384
req_extensions = req_ext
distinguished_name = dn
[dn]
C = US
ST = NJ
O = Test, Inc.
CN = localhost
[req_ext]
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
IP.1 = ::1
IP.2 = 127.0.0.1
" > certificate.conf

# CA key
openssl ecparam -genkey -name prime256v1 -out rootca.key
# CA certificate
openssl req -x509 -new -SHA256 -nodes -key rootca.key -days 3650 -subj "/C=US/ST=NJ/O=CA, Inc." -out rootca.cert

# Node key
openssl ecparam -genkey -name prime256v1 -out node.key
# Node cetificate signature request
openssl req -new -SHA256 -key node.key -nodes -config certificate.conf -out node.csr
# Node certificate
openssl x509 -req -SHA256 -extfile v3.ext -days 3650 -in node.csr -CA rootca.cert -CAkey rootca.key -CAcreateserial -out node.cert

# Admin key
openssl ecparam -genkey -name prime256v1 -out admin.key
# Admin cetificate signature request
openssl req -new -SHA256 -key admin.key -nodes -config certificate.conf -out admin.csr
# Admin certificate
openssl x509 -req -SHA256 -extfile v3.ext -days 3650 -in admin.csr -CA rootca.cert -CAkey rootca.key -CAcreateserial -out admin.cert


#Cleanup
rm v3.ext certificate.conf rootca.srl *.csr rootca.key admin.key
