---
id: curl
title: Setting Up Command Line Utilities
---

<!--
 Copyright IBM Corp. All Rights Reserved.

 SPDX-License-Identifier: CC-BY-4.0
 -->

We need the following five utilities to successfully execute all example `cURL` commands:

 1. `cURL` command line tool to issue http requests and receive http responses.
 2. `jq` command line tool to pretty print JSON output from `cURL`.
 3. `signer` utility to compute the required digital signature for each query and transaction.
 4. `encoder` utility to encode data to base64 encoding.
 5. `decoder` utility to decode the base64-encoded data present in the server responses.

## 1) Installing cURL

You need to install the `cURL` utility on the server, laptop or PC from which you are planning to issue transactions and queries. Search the web to find out how to install the `cURL` utility on your operating system.

## 2) Installing jq

Instructions for installing the `jq` command line tool can be found [here](https://stedolan.github.io/jq/download/).

## 3) Building signer, encoder, and decoder

Refer to these [six steps](../launching-one-node/binary#build) to build the `signer`, `encoder`, and `decoder` utilities. Once you execute these steps, all three
utilities can be found in `./bin`

### 3.1) Using signer

```sh
./bin/signer
```
prints the following
```
all the following two flags must be set. An example command is shown below:

  signer -data='{"userID":"admin"}" -privatekey=admin.key

  -data string
    	JSON data to be signed. Surround that data with single quotes. An example of JSON data is '{"userID":"admin"}'
  -privatekey string
    	path to the private key to be used for adding a digital signature
```

The `signer` utility expects two arguments: (1) data and (2) privatekey, as shown above. The `data` argument must be JSON data
on which the digital signature is put using the private key assigned to `privatekey` argument.

For example, in the following command, `admin` puts the digital signature on the JSON data `'{"user_id":"admin"}'`

```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin"}'
```
The above command produces a digital signature and prints it as base64-encoded string as shown below
```
MEUCIQCMEdLgfFEOF+vgXLwbeOdUUWnGB5HH2ULkoz15jlk5DgIgbWXuoyqD4szob78hZYiau9LPdJLLqP3bAu7iV98BcW0=
```

### 3.2) Using encoder

```shell
./bin/encoder
```
prints the following
```
The data field accepts a JSON or string value. THE data flag must be set. Two example commands are shown below:

  encoder -data='{"userID":"admin"}'
  encoder -data='value'

  -data string
    	JSON or string data to be encoded. Surround the JSON data with single quotes. An example of JSON data is '{"userID":"admin"}'
```

The `encoder` utility expects only one argument, i.e., data, as shown above. The `data` argument can be a JSON or any other string that needs to be encoded using base64 encoding.

For example, in the following command, `encoder` outputs the base64-encoded string of `{"userID":"admin"}`

```sh
./bin/encoder -data='{"user_id":"admin"}'
```
The above command produces the base64-encoded string
```
eyJ1c2VyX2lkIjoiYWRtaW4ifQ==
```

### 3.2) Using decoder

```shell
./bin/decoder
```
prints the following
```
The decoder decodes the base64-encoded value field in the GetDataResponseEnvelope. Pass the JSON output of GetDataResponseEnvelope to the `-getresponse` flag
  -getresponse string
    	JSON output of GetDataResponseEnvelope. The value field in the JSON output will be decoded. Surround the JSON data with single quotes.
```

The `decoder` utility expects only one argument, i.e., GetDataResponseEnvelope, as shown above. The `getresponse` argument must be a JSON representation of
GetDataResponseEnvelope.

For example, in the following command, `decoder` decodes the base64-encoded value filed in 
```webmanifest
{
  "response": {
    "header": {
      "node_id": "orion-server1"
    },
    "value": "eyJuYW1lIjoiYWJjIiwiYWdlIjozMSwiZ3JhZHVhdGVkIjp0cnVlfQ==",
    "metadata": {
      "version": {
        "block_num": 4
      },
      "access_control": {
        "read_users": {
          "alice": true,
          "bob": true
        },
        "read_write_users": {
          "alice": true
        }
      }
    }
  },
  "signature": "MEQCIFK/bLBAu2mzH0DmRt9SPyTDfxG5qwPHTe05C4uKjGr1AiALnlwpVQ0spTzUMxgJOEHO8Li+P/7uACGXMIQK32O6WQ=="
}
```

```sh
./bin/decoder -getresponse='{"response":{"header":{"node_id":"orion-server1"},"value":"eyJuYW1lIjoiYWJjIiwiYWdlIjozMSwiZ3JhZHVhdGVkIjp0cnVlfQ==","metadata":{"version":{"block_num":4},"access_control":{"read_users":{"alice":true,"bob":true},"read_write_users":{"alice":true}}}},"signature":"MEUCIHMs2zPzybWNY52JqiD+mYPNgof6/Kg/cj4KsoJPu5IXAiEAtRprbynYqFCUyU+hOzsXaQVg/iBM2CZatLr1Fo+XJcA="}' | jq .
```
The above command produces the base64-encoded string
```webmanifest
{
  "response": {
    "header": {
      "node_id": "orion-server1"
    },
    "value": "{\"name\":\"abc\",\"age\":31,\"graduated\":true}",
    "metadata": {
      "version": {
        "block_num": 4
      },
      "access_control": {
        "read_users": {
          "alice": true,
          "bob": true
        },
        "read_write_users": {
          "alice": true
        }
      }
    }
  },
  "signature": "MEUCIHMs2zPzybWNY52JqiD+mYPNgof6/Kg/cj4KsoJPu5IXAiEAtRprbynYqFCUyU+hOzsXaQVg/iBM2CZatLr1Fo+XJcA="
}

```

It can be seen that the `"value":"eyJuYW1lIjoiYWJjIiwiYWdlIjozMSwiZ3JhZHVhdGVkIjp0cnVlfQ=="` has been decoded to `"value": "{\"name\":\"abc\",\"age\":31,\"graduated\":true}"`.
