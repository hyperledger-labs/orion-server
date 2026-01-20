#!/bin/bash

# Generate crypto materials for 3-node Orion cluster
# This creates CA, admin, and 3 server certificates

set -e

create_pki() {
        echo "Creating PKIs folders for $1"
        mkdir -p "$BASE_DIR/crypto/$1"

        echo "Generating private key for $1"
        docker run -it --rm -v $BASE_DIR/crypto:/export nginx openssl ecparam -name prime256v1 -genkey -noout -out "/export/$1/$1.key"
        docker run -it --rm -v $BASE_DIR/crypto:/export nginx chmod ga+r "/export/$1/$1.key"

        echo "Generate CSR for $1"
        docker run -it --rm -v $BASE_DIR/crypto:/export nginx openssl req -new -key "/export/$1/$1.key" -out "/export/$1/$1.csr" -subj "/C=US/ST=NY/O=Orion/CN=$1"

        echo "Generate certificate for $1"
        docker run -it --rm -v $BASE_DIR/crypto:/export nginx openssl x509 -req -in "/export/$1/$1.csr" -CA "/export/CA/CA.pem" -CAkey "/export/CA/CA.key" -CAcreateserial -out "/export/$1/$1.pem" -days 1825 -sha256
        
        echo "✅ Created crypto materials for $1"
        echo ""
}

if [ -z "$1" ]; then
    echo "Usage: cryptoGen3Nodes.sh <folder>"
    echo "Example: ./scripts/cryptoGen3Nodes.sh deployment"
    exit 1
fi

BASE_DIR=$1

# Remove existing crypto folder
echo "🗑️  Removing old crypto materials..."
rm -rf "$BASE_DIR/crypto"

# Handle absolute vs relative paths
if [[ "$BASE_DIR" = /* ]]; then
   : # Absolute path
else
   BASE_DIR=$(pwd)/$BASE_DIR
fi

echo "📁 Creating crypto materials in: $BASE_DIR/crypto"
echo ""

# Create CA
echo "Creating PKIs folders for CA"
mkdir -p "$BASE_DIR/crypto/CA"

echo "🔐 Generate root CA private key"
docker run -it --rm -v $BASE_DIR/crypto:/export nginx openssl ecparam -name prime256v1 -genkey -noout -out "/export/CA/CA.key"
docker run -it --rm -v $BASE_DIR/crypto:/export nginx chmod ga+r "/export/CA/CA.key"

echo "🔐 Generating self-signed root CA certificate"
docker run -it --rm -v $BASE_DIR/crypto:/export nginx openssl req -new -x509 -nodes -key "/export/CA/CA.key" -sha256 -days 1825 -out "/export/CA/CA.pem" -subj "/C=US/ST=NY/O=Orion/CN=RootCA" -extensions v3_ca
echo "✅ Created CA"
echo ""

# Create admin user
create_pki "admin"

# Create 3 server nodes
create_pki "server1"
create_pki "server2"
create_pki "server3"

echo "✅ All crypto materials generated successfully!"
echo ""
echo "Created certificates for:"
echo "  - CA (Root Certificate Authority)"
echo "  - admin (Cluster Administrator)"
echo "  - server1 (Orion Node 1)"
echo "  - server2 (Orion Node 2)"
echo "  - server3 (Orion Node 3)"
echo ""
echo "📂 Location: $BASE_DIR/crypto/"
