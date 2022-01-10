---
id: binary
title: Orion Executable
---

<!--
 Copyright IBM Corp. All Rights Reserved.

 SPDX-License-Identifier: CC-BY-4.0
 -->

## 1) Prerequisites

To build an executable binary file, the following prerequisites should be installed on the platform on which Orion will be deployed.

  - **[Go Programming Language](https://golang.org/)**: The database uses the Go Programming Language for many of its components. Go version 1.16.x or higher is required.
  - **[Make](https://man7.org/linux/man-pages/man1/make.1.html)**: To build a binary file and execute a unit-test, the `make` utility is required.
  - **[Git](https://github.com/git-guides/install-git)**: Git is used to clone the code repository.

## 2) Build

To build the executable binary, the following steps must be executed:

  1. Create the required directory using the command `mkdir -p github.com/hyperledger-labs`.
  2. Change the current working directory to the above created directory by issuing the command `cd github.com/hyperledger-labs`.
  3. Clone the server repository with `git clone https://github.com/hyperledger-labs/orion-server`.
  4. Change the current working directory to the repository root directory by issuing `cd orion-server`.
  5. To build the binary executable file, issue the command `make binary`, which creates a `bin` folder in the current directory. The `bin` holds four executable
  files named `bdb`, `signer`, `encoder`, and `decoder`.
  6. Run the `bdb` executable by issuing the following command:

```shell
./bin/bdb
```

If the above command execution results in the following output, the build was successful.

```text
To start and interact with a blockchain database server.
Usage:
  bdb [command]
Available Commands:
  help        Help about any command
  start       Starts a blockchain database
  version     Print the version of the blockchain database server.
Flags:
  -h, --help   help for bdb
Use "bdb [command] --help" for more information about a command.
```

For additional health checks, we can run `make test` to ensure all tests pass.

## 3) Start

To start a node, we need a certificate authority and crypto materials for the node and admin users. To simplify this task, we have provided sample
crypto materials and configuration files in the `deployment/sample/` folder. In order to understand the configuration files and procedure for creating crypto
materials, refer to [crypto materials](crypto-materials).

Let's start a node with the sample configuration:
```shell
./bin/bdb start --configpath ./deployment/config-local/config.yml
```

The output of the above command would be something similar to the following:
```text
2021/06/16 18:06:34 Starting a blockchain database
2021-06-16T18:06:35.151+0530	INFO	bdb-node-1	blockprocessor/processor.go:263	Registering listener [transactionProcessor]
2021-06-16T18:06:35.151+0530	INFO	bdb-node-1	txreorderer/txreorderer.go:59	starting the transactions reorderer
2021-06-16T18:06:35.151+0530	INFO	bdb-node-1	blockcreator/blockcreator.go:74	starting the block creator
2021-06-16T18:06:35.151+0530	INFO	bdb-node-1	replication/blockreplicator.go:73	starting the block replicator
2021-06-16T18:06:35.153+0530	INFO	bdb-node-1	server/server.go:113	Starting to serve requests on: 127.0.0.1:6001
```

Congratulations! We have started a node successfully.