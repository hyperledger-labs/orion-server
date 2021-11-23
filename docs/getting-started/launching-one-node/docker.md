---
id: docker
title: Orion Docker
---

<!--
 Copyright IBM Corp. All Rights Reserved.

 SPDX-License-Identifier: CC-BY-4.0
 -->

## 1) Prerequisites

To build a docker image, the following are the prerequisites which should be installed.

  - **[Git](https://github.com/git-guides/install-git)**: To clone the code repository.
  - **[Docker](https://www.docker.com)**: To build the docker image and start the Orion in a docker container.

## 2) Orion Docker Image

Orion node can be launched by either
   1. Pulling the published docker image in the docker hub (or)
   2. Building the image from scratch

Next, we explain the both options.

### 2.1) Pull Orion Docker Image from Docker Hub

Orion docker image is available in docker hub repository. To pull the image, execute the following command:
```
docker pull orionbcdb/orion-server
```

### 2.2) Build Your Own Orion Docker Image

If you would like to build your own image rather than pulling it from the docker hub, execute the following steps:

  1. Create the required directory using the command `mkdir -p github.com/hyperledger-labs`
  2. Change the current working directory to the above created directory by issing the command `cd github.com/hyperledger-labs`
  3. Clone the server repository with `git clone https://github.com/hyperledger-labs/orion-server`
  4. Change the current working directory to the repository root directory by issuing `cd orion-server`
  5. To build the docker image, issue the command `make docker` which will create a docker image named `orionbcdb/orion-server`

## 3) Start Orion in a Docker Container

To start a server node, we need a certificate authority and crypto materials for the server node and admin users. To simplify this task,
we have provided sample crypto materials in `deployment/crypto/`.

Let's start a node with the sample crypto materials:

```docker
docker run -it --rm -v $(pwd)/deployment/crypto/:/etc/orion-server/crypto -p 6001:6001 \
    -p 7050:7050 orionbcdb/orion-server
```

Port `6001` is Orion REST API port and should be used to access Orion using its REST API or [Go SDK](https://github.com/hyperledger-labs/orion-sdk-go/)

Port `7050` is used between nodes in a cluster for performing replication.

:::info
To change the port `6001` and `7050`, we need to change the configuration file and map the host directory holding this configuration to `/etc/orion-server/config`
as explained next in [volumes](#31-volumes-for-persistant-storage-configuration-and-crypto-materials).
:::

When we start a docker container with the above parameters, all data associated with the orion-server will stay within the container itself.
When the container is stopped and removed, all data will be lost. For all examples used in this documentation, the container started with the
above command is sufficient.

For a persistant storage, we need to use docker volumes as shown next.

### 3.1) Volumes for Persistant Storage, Configuration, and Crypto Materials

Orion docker image has three mappable volumes:
  1. `/var/orion-server/ledger`
      - It holds all blockchain data maintained by the Orion node. For a persistant storage of blockchain data, we need to map a host directory to this volume while starting the container.
  2.  `/etc/orion-server/config`
      - It contains server configuration `yml` files. Mapping is optional, needed only when we want a custom configuration. For example, the default port on which the Orion node listens is `6001`. To change this, we need to update the configuration file. Sample configuration files are stored in `deployment/config-docker` folder.
  3. `/etc/orion-server/crypto`
      - It holds all required crypto materials. This volume must be mapped to a host folder. Sample crypto materials are stored in `deployment/crypto` folder. Never use pre-existing crypto materials in production environment.

To start the orion container with all three volumes, execute the following command:
```
docker run -it --rm -v $(pwd)/deployment/crypto/:/etc/orion-server/crypto \
                    -v $(pwd)/ledger:/etc/orion-server/ledger \
                    -v $(pwd)/deployment/config-docker:/etc/orion-server/config \
                    -p 6001:6001 -p 7050:7050 orionbcdb/orion-server
```
