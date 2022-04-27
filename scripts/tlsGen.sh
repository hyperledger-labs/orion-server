create_pki() {
        echo "Creating PKIs folders"
        mkdir -p "$BASE_DIR/tls/$1"

        echo "Generating private key for $1"
        docker run -it --rm -v $BASE_DIR/tls:/export nginx openssl ecparam -name prime256v1 -genkey -noout -out "/export/$1/$1.key"
        docker run -it --rm -v $BASE_DIR/tls:/export nginx chmod ga+r "/export/$1/$1.key"

        echo "Generate node CSR"
        docker run -it --rm -v $BASE_DIR/tls:/export nginx openssl req -new -key "/export/$1/$1.key" -out "/export/$1/$1.csr" -subj "/C=IL/ST=Haifa/O=BCDB"

        echo "Generate node certificate"
        docker run -it --rm -v $BASE_DIR/tls:/export nginx openssl x509 -req -in "/export/$1/$1.csr" -CA "/export/CA/CA.pem" -CAkey "/export/CA/CA.key" -CAcreateserial -out "/export/$1/$1.pem" -days 365 -sha256
}

if [ -z "$1" ]
  then
    echo "tlsGen.sh folder [extra users]"
    exit 1
fi
BASE_DIR=$1

rm -rf "$BASE_DIR/crypto"

if [[ "$BASE_DIR" = /* ]]
then
   : # Absolute path
else
   BASE_DIR=$(pwd)/$BASE_DIR
fi

echo "Creating PKIs folders"
mkdir -p "$BASE_DIR/tls/CA"

echo "Generate root CA private key"
docker run -it --rm -v $BASE_DIR/tls:/export nginx openssl ecparam -name prime256v1 -genkey -noout -out "/export/CA/CA.key"
docker run -it --rm -v $BASE_DIR/tls:/export nginx chmod ga+r "/export/CA/CA.key"

echo "Generating self-signed root CA certificate"
docker run -it --rm -v $BASE_DIR/tls:/export nginx openssl req -new -x509 -nodes -key "/export/CA/CA.key" -sha256 -days 365 -out "/export/CA/CA.pem" -subj "/C=IL/ST=Haifa/O=BCDB" -extensions v3_ca

create_pki "server"

shift 1

if [ $# -ne 0 ]
  then
    for f in "$@"
    do
      create_pki "$f"
    done
fi

