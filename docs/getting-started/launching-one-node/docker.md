---
id: docker
title: Orion Docker
---

<!--
 Copyright IBM Corp. All Rights Reserved.

 SPDX-License-Identifier: CC-BY-4.0
 -->

### Prerequisites

To build a docker image, the following are the prerequisites which should be installed.

  - **Docker**: To generate cryptographic materials and build BCDB image.
  - **Git**: To clone the code repository.

### Build

To build the docker image, the following steps need to be executed:

  1. To clone the repository, first, create required directories using the command `mkdir -p github.com/hyperledger-labs`
  2. Change the current working directory to the above created folder by issing the command `cd github.com/hyperledger-labs`
  3. Clone this repository with `git clone https://github.com/hyperledger-labs/orion-server`
  4. Change the current working directory to the repository root directory by issuing `cd orion-server`
  5. Optional. Generate all cryptographic materials. For more info see [crypto-materials](crypto-materials).
  6. To build the docker image, issue the command `make docker` which will create a docker image named `orion-server`
  7. Optional. Versions of docker image stored once in while in docker hub docker repository as `orionbcdb/orion-server`

### Start Orion in a Docker Container
If you only plan to try Orion, you can use the pre-existing configuration and crypto materials stored in folder `deployment`.

To do that, you just have to run the following:
```
docker run -it --rm -v $(pwd)/deployment/crypto/:/etc/orion-server/crypto -p 6001:6001 -p 7050:7050 orion-server
``` 
This command will store all the database data inside the running container, so that container termination will remove all data.

If you plan more that just trying Orion, first you have to create __new__ and __unique__ cryptographic materials and optional, update Orion server configuration.   

To create a minimal set of cryptographic materials, execute the `cryptoGen.sh` script as shown below.
```
./scripts/cryptoGen.sh deployment
```
It will create a new set of crypto materials, including Certificate Authority certificate & key pair, _server_ certificate & key pair and certificate & key pairs for _admin_ and _user_ database users.

#### Existing volumes
As mentioned before, by default, the ledger & database data and the server configuration are stored inside the docker container, but it can be overwritten by mapping host folders to container volumes.

Newly created docker image has three mappable volumes:
* `/etc/orion-server/config` - contains server configuration `yml` files. Mapping is optional, needed only if you want to use your own configuration. Sample configuration files are stored in `deployment/config-docker` folder.
* `/var/orion-server/ledger` - blockchain ledger storage folder. Mapping is optional, but without mapping termination of the docker container will erase all ledger data.
* `/etc/orion-server/crypto` - crypto materials folder. Should be mapped to host folder. Pre-existing crypto materials are stored in `deployment/crypto` folder. Never use pre-existing crypto materials in production environment.  

To invoke the orion server docker container with all mappings, you can run:
```
docker run -it --rm -v $(pwd)/deployment/crypto/:/etc/orion-server/crypto -v $(pwd)/ledger:/etc/orion-server/ledger -v $(pwd)deployment/config-docker:/etc/orion-server/config -p 6001:6001 -p 7050:7050 orion-server
``` 
