# Quick reference

-	**Maintained by**:  
     [Hyperledger Orion](https://github.com/hyperledger-labs/orion-server/)

-	**Where to get help**:  
     [Project github](https://github.com/hyperledger-labs/orion-server/), [Project documentation](http://labs.hyperledger.org/orion-server/)

# Supported tags and respective `Dockerfile` links

- [`v0.2.0`, `latest`](https://github.com/hyperledger-labs/orion-server/blob/a29148fb3c9503140ede965c6af11c0fda8ff4ba/images/Dockerfile)


# What is Hyperledger Orion?

- Orion is a key-value/document database with certain blockchain properties.  

# How to use this image

## Start an Orion instance
```console
docker run -it --rm -v crypto-materials-location:/etc/orion-server/crypto -p 6001:6001 -p 7050:7050 orionbcdb/orion-server
```

Port `6001` is Orion REST API port and should be used to access Orion using its REST API or [Go SDK](https://github.com/hyperledger-labs/orion-sdk-go/) 

Port `7050` is cluster communication port, leave it as is for now.

`crypto-materials-location` is there all certificates and private keys needed to run Orion server are stored.
If you only plan to try Orion, you can use the pre-existing configuration and crypto materials stored in folder [`deployment/crypto`](https://github.com/hyperledger-labs/orion-server/tree/main/deployment/crypto).

# Caveats

## Where to store data 

The ledger & database data and the server configuration are stored inside the docker container, but it can be overwritten by mapping host folders to container volumes.

The image has three mappable volumes:
* `/etc/orion-server/config` - contains server configuration `yml` files. Mapping is optional, needed only if you want to use your own configuration. Sample configuration files are stored in `deployment/config-docker` folder.
* `/var/orion-server/ledger` - blockchain ledger storage folder. Mapping is optional, but without mapping termination of the docker container will erase all ledger data.
* `/etc/orion-server/crypto` - crypto materials folder. Should be mapped to host folder. Pre-existing crypto materials are stored in `deployment/crypto` folder in [Orion github](https://github.com/hyperledger-labs/orion-server/). Never use pre-existing crypto materials in production environment.

To invoke the orion server docker container with all mappings, you can run:
```console
docker run -it --rm -v crypto-materials-location:/etc/orion-server/crypto \
                    -v ledger-data-location:/var/orion-server/ledger \
                    -v orion-config-location:/etc/orion-server/config \
                    -p 6001:6001 -p 7050:7050 orionbcdb/orion-server
```

for example, running it for `orion-server` folder from [github](https://github.com/hyperledger-labs/orion-server), using configuration and crypro materials stored in `deployment` folder, will look like this:
```console
docker run -it --rm -v $(pwd)/deployment/crypto/:/etc/orion-server/crypto \
                    -v $(pwd)/ledger:/var/orion-server/ledger \
                    -v $(pwd)/deployment/config-docker:/etc/orion-server/config \
                    -p 6001:6001 -p 7050:7050 orionbcdb/orion-server
``` 
