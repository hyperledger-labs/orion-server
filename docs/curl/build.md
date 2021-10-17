## Build and Start a Blockchain DB Node

Let's build an executable binary file and start a single node DB. Note that we can start a multi-node cluster as described in [setup a cluster]().

### Prerequisites

To build an executable binary file, the following are the prerequisites which should be installed on the platform on which
the blockchain DB will be deployed.

  - **[Go Programming Language](https://golang.org/)**: The database uses the Go Programming Language for many of its components. Hence, Go version 1.15.x is required.
  - **Make**: To build a binary file and execute unit-test, `make` utility is required.
  - **Git**: To clone the code repository.
  - **cURL**: To execute queries and transactions provided in the tutorial.

### Build

To build the executable binary, the following steps need to be executed

  1. To clone the repository, first, create required directories using the command `mkdir -p github.com/hyperledger-labs`
  2. Change the current working directory to the above created folder by issing the command `cd github.com/hyperledger-labs`
  3. Clone this repository with `git clone https://github.com/hyperledger-labs/orion-server`
  4. Change the current working directory to the repository root directory by issuing `cd orion-server`
  5. To build the binary executable file, issue the command `make binary` which will create a `bin` folder in the current directory. The `bin` would holds two executable
  files named `bdb` and `signer`.
  6. Run the `bdb` executable by issuing the command `./bin/bdb`. If the following output is printed, the build was successful.
  ```
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
For additional health check, we can run `make test` to ensure all tests pass.

### Start

To start a node, we need a certificate authority and crypto materials for the node and admin users. To simplify this task, we have provided sample
crypto materials and configuration files in `deployment/sample/`. In order to understand the configuration files and procedure to create crypto
materials, refer to [Configuration]() and [Generating Crypto Materials]().

Let's start a node with the sample configuration:
`
./bin/bdb start --configpath deployment/sample/config-sample.yml
`

The output of the above command would be something similar to the following
```
2021/06/16 18:06:34 Starting a blockchain database
2021-06-16T18:06:35.151+0530	INFO	bdb-node-1	blockprocessor/processor.go:263	Registering listener [transactionProcessor]
2021-06-16T18:06:35.151+0530	INFO	bdb-node-1	txreorderer/txreorderer.go:59	starting the transactions reorderer
2021-06-16T18:06:35.151+0530	INFO	bdb-node-1	blockcreator/blockcreator.go:74	starting the block creator
2021-06-16T18:06:35.151+0530	INFO	bdb-node-1	replication/blockreplicator.go:73	starting the block replicator
2021-06-16T18:06:35.153+0530	INFO	bdb-node-1	server/server.go:113	Starting to serve requests on: 127.0.0.1:6001
```

Congratulations! We have started a node successfully.

## Build and start Blockchain DB node inside Docker
### Prerequisites

To build an docker image, the following are the prerequisites which should be installed.

  - **Docker**: To generate cryptographic materials and build BCDB image.
  - **Git**: To clone the code repository.
  - **cURL**: To execute queries and transactions provided in the tutorial.

### Build

Build process includes two steps - crypto materials generation and docker image build

#### Generate cryptographic materials 
Minimal set of crypto materials includes 3 sets of certificates, totally 4 certificates

1. `admin` and `user` - for db users
2. `node` - for server node
3. `CA` - for Certificate Authority 
To create minimal set of cryptographic materials, you can run:
```
./scripts/cryptoGen.sh sampleconfig
```
Please note that `sampleconfig` folder already contains multiple configuration files used to build BCDB docker images.

If you plan to use more users, you can create extra certificates in addition to minimal set. To create crypto configuration for more users, you can run:
```
./scripts/cryptoGen.sh sampleconfig <extra users>
```
For example, in case you want to generate crypto materials for extra users used in car demo, you can run:
```
./scripts/cryptoGen.sh sampleconfig alice bob dmv dealer
```
You don't have to create certificates for all users using `cryptoGen.sh` script, but can create them manually after that. See [Generating Crypto Materials]()

#### Generate docker image
To generate docker image, after you generated crypto materials, run:
```
make docker
```
#### Start docker container
To invoke the orion server docker container, you can run:
```
docker run -it --rm -v $(pwd)/sampleconfig/:/etc/orion-server -p 6001:6001 -p 7050:7050 orion-server
``` 

## Build and Use Signer Utility

We need the `signer` utility to compute the required digital signature for each query and transaction.

### Build

Refer to these [six steps](#Build) to build the `signer` utilty. If you have already executed these steps, the `signer` executable
can be found in `./bin`

### Usage

```sh
⟩ ./bin/signer
```
would print the following
```
all the following two flags must be set. An example command is shown below:

  signer -data='{"userID":"admin"}" -privatekey=admin.key

  -data string
    	json data to be signed. Surround that data with single quotes. An example json data is '{"userID":"admin"}'
  -privatekey string
    	path to the private key to be used for adding a digital signature
```

The `signer` utility expects two arguments: (1) data; (2) privatekey; as shown above. The `data` argument must be a json data
on which the digital signature is put using the private key assigned to `privatekey` argument.

For example, in the following command, `admin` puts the digital signature on the json data `'{"user_id":"admin"}'`

```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin"}'
```
The above command would produce a digital signature and prints it as base64 encoded string as shown below
```
MEUCIQCMEdLgfFEOF+vgXLwbeOdUUWnGB5HH2ULkoz15jlk5DgIgbWXuoyqD4szob78hZYiau9LPdJLLqP3bAu7iV98BcW0=
```
