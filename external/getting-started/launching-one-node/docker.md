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

To build the docer image, the following steps need to be executed:

  1. To clone the repository, first, create required directories using the command `mkdir -p github.com/hyperledger-labs`
  2. Change the current working directory to the above created folder by issing the command `cd github.com/hyperledger-labs`
  3. Clone this repository with `git clone https://github.com/hyperledger-labs/orion-server`
  4. Change the current working directory to the repository root directory by issuing `cd orion-server`
  5. To build the docker images, issue the command `make docker` which will create a docker image named `orion-server`

### Start Orion in a Docker Container
Before starting the container, we need to create cryptographic materials needed to run the Orion.  To create a minimal set of cryptographic materials, execute the `cryptoGen.sh` script as shown below.
```
./scripts/cryptoGen.sh sampleconfig
```

Please note that `samplconfig` folder already contains multiple configuration files used to build Orion docker images.

To start the orion server docker container, issue the following command:
```
docker run -it --rm -v $(pwd)/sampleconfig/:/etc/orion-server -p 6001:6001 -p 7050:7050 orion-server
``` 
