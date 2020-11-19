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
openssl ecparam -genkey -name prime256v1 -out ca_$1.key
# CA certificate
openssl req -x509 -new -SHA256 -nodes -key ca_$1.key -days 3650 -subj "/C=US/ST=NJ/O=CA, Inc." -out ca_$1.cert
# Server key
openssl ecparam -genkey -name prime256v1 -out $1.key
#Server cetificate signature request
openssl req -new -SHA256 -key $1.key -nodes -config certificate.conf -out $1.csr
# Server certificate
openssl x509 -req -SHA256 -extfile v3.ext -days 3650 -in $1.csr -CA ca_$1.cert -CAkey ca_$1.key -CAcreateserial -out $1.pem
# Server no CA key 
openssl ecparam -name prime256v1 -genkey -out noca_$1.key
# Server no CA certificate
openssl req -x509 -new -SHA256 -nodes -key noca_$1.key -days 3650 -config certificate.conf -out noca_$1.pem

#Cleanup 
rm v3.ext
rm certificate.conf
rm ca_$1.srl
rm $1.csr
