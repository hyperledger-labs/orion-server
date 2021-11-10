---
id: curl
title: Setting up cURL, jq, and signer utilities
---

<!--
 Copyright IBM Corp. All Rights Reserved.

 SPDX-License-Identifier: CC-BY-4.0
 -->

### Required Command Line Tools

We need the following two tools to successfully execute all example `cURL` commands:

 1. `cURL` command line tool to issue http request and receive http response.
 2. `jq` command line tool to pretty print JSON output from `cURL`
 3. `signer` utility to compute the required digital signature for each query and transaction.

### Install cURL

You need to install the `cURL` utility on your server or laptop or PC from which you are planning to issue transactions and queries. For your operating system, please use google to find how to install the `cURL` utility.

### Install jq

Instructions for install `jq` command can be found [here](https://stedolan.github.io/jq/download/)

### Build Signer

Refer to these [six steps](../launching-one-node/binary#build) to build the `signer` utilty. If you have already executed these steps, the `signer` executable
can be found in `./bin`

#### Usage

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
