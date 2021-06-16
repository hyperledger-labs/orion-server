## Querying the Cluster Configuration

The cluster configuration includes node, admin, and consensus configuration (used for replication).
When the `bdb` server bootup for the first time, it reads nodes, admins, and consensus configuration
present in the configuration file `config.yml` and creates a genesis block. The user can query
the current cluster configuration by issing a `GET` request on `/config/tx` endpoint.

The REST endpoint for querying the configuration is `/config/tx` and it does not require any
inputs or additional paramters from the user. Hence, the user need to sign only their user id and
set the signature in the `Signature` header.

Specifically, the user needs to sign the following JSON data `{"user_id":"<userID>"}` where `<userID`
is the ID of the submitting user who is registered in the blockchain database node. In our example,
`admin` user is the one who submits request to the server. Hence, we need to use the admin's private
key to sign the `{"user_id":"admin"}` as shown below.

```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin"}'
```
The above command would produce a digital signature and prints it as base64 encoded string as shown below
```
MEUCIQCMEdLgfFEOF+vgXLwbeOdUUWnGB5HH2ULkoz15jlk5DgIgbWXuoyqD4szob78hZYiau9LPdJLLqP3bAu7iV98BcW0=
```

Once the signature is computed, we can issue a `GET` request using the following `cURL` command
by setting the above signature string in the `Signature` header.
```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: MEUCIQCMEdLgfFEOF+vgXLwbeOdUUWnGB5HH2ULkoz15jlk5DgIgbWXuoyqD4szob78hZYiau9LPdJLLqP3bAu7iV98BcW0=" \
   -X GET http://127.0.0.1:6001/config/tx | jq .
   ```
A sample output of above command is shown below. The actual content might change depending on the configuration specified in `config.yml`.
For now, all users in the cluster can query the cluster configuration to identity the set of nodes, IP address of each node
along with the listening port number, certificate of the node, etc...

```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "config": {
      "nodes": [
        {
          "id": "bdb-node-1",
          "address": "127.0.0.1",
          "port": 6001,
          "certificate": "MIIBsjCCAVigAwIBAgIQYy4vf2+6qRtczxIkNb9fxjAKBggqhkjOPQQDAjAeMRwwGgYDVQQDExNDYXIgcmVnaXN0cnkgUm9vdENBMB4XDTIxMDYxNjExMTMyN1oXDTIyMDYxNjExMTgyN1owJTEjMCEGA1UEAxMaQ2FyIHJlZ2lzdHJ5IENsaWVudCBzZXJ2ZXIwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARWWd8GA/dbkxTiNP7x/LoyAc85sfsIxcmiX+4nzzB3s4SXA+N8YMSiKpsi6gSOVkxKLu43lla2ajyL0Z4WqWvYo3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiEAuyIC0jlV/KcyB8Cz2p3W4aojh+fDCeeRenMwvyP+EcACIDOjDiObMnb/2q2ceAKROr/rzJmakdjkNmw8A0bYL6Pb"
        }
      ],
      "admins": [
        {
          "id": "admin",
          "certificate": "MIIBsTCCAVigAwIBAgIRANDg6u+E+sdowdPBGXyoD9swCgYIKoZIzj0EAwIwHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTAeFw0yMTA2MTYxMTEzMjdaFw0yMjA2MTYxMTE4MjdaMCQxIjAgBgNVBAMTGUNhciByZWdpc3RyeSBDbGllbnQgYWRtaW4wWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAT9GYTj7ka7se8bEqclDdulRJnpay+EU/b4QUMiLl/9cB1iBgKegVXztZCcQs3S+XR1Y2b/1xSqMbPwHOV5kuUDo3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNHADBEAiB09qz5bAyguEEI4HMUPIBqRTF3RBMjfTKrpIjBh1ai9gIgWLL0SNsD/5a4xjp+fol42npUY13mIpByYg56IW0TsBI="
        }
      ],
      "cert_auth_config": {
        "roots": [
          "MIIBrDCCAVKgAwIBAgIQR6JzR6pbStB4cjbEtVAQrzAKBggqhkjOPQQDAjAeMRwwGgYDVQQDExNDYXIgcmVnaXN0cnkgUm9vdENBMB4XDTIxMDYxNjExMTMyN1oXDTIyMDYxNjExMTgyN1owHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABNttDXwc6IkY7tS+jmTl1z7i6Aq+zhVnTrbhVpKPHReZDSgIyLOzRMvRMVkmyNq+K0Tqi83R/0gNRpVQVOVgUIejcjBwMA4GA1UdDwEB/wQEAwICpDAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiEA/faZ857y9K/pCRHId67fC4ZBZJ+vs7vUkbLJrTPC+L4CIEVGUVou5T7Lnc+J3pHU15yDgr42/APzhRrBiRAa9FMC"
        ]
      },
      "consensus_config": {
        "algorithm": "raft",
        "members": [
          {
            "node_id": "bdb-node-1",
            "raft_id": 1,
            "peer_host": "127.0.0.1",
            "peer_port": 7050
          }
        ],
        "raft_config": {
          "tick_interval": "100ms",
          "election_ticks": 50,
          "heartbeat_ticks": 5
        }
      }
    },
    "metadata": {
      "version": {
        "block_num": 1
      }
    }
  },
  "signature": "MEQCIFE0hfiVoVnP/9m+hwgeApmO5OJncl6GEJk3f95pd5ROAiBB/tBhi0NT+ccE3lrXclwEct1X/vtO84IIwuSD/rBeSA=="
}
```

### Querying a Particular Node's Configuration

While `GET /config/tx` returns the complete configuration, we can use `GET /config/node/{nodeid}` to fetch the configuration of a particular node.
Here, the submitting user needs to sign `{"user_id":"<userid>","node_id":"<nodeid>"}` where `<userid>` denotes the id of the submitting user and
`<nodeid>` denotes the id of the blockchain database for which the user tries to fetch the configuration.

In our example, the JSON data to be signed is `{"user_id":"admin","node_id":"bdb-node-1"}` because the `admin` is submitting a request to fetch
the configuration of the node with id `bdb-node-1`

```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","node_id":"bdb-node-1"}'
```
The above command would produce a digital signature and prints it as base64 encoded string as shown below
```
MEQCIEAxqk1RnWBvCt9OV7zycK3U5itkEumSpEXivv/2R030AiA5mZMgvIDEgPP2J1TGRPsgyXZY6VrAcWTyFAzlLgKpPg==
```

Once the signature is computed, we can issue a `GET` request using the following `cURL` command
by setting the above signature string in the `Signature` header.
```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: MEQCIEAxqk1RnWBvCt9OV7zycK3U5itkEumSpEXivv/2R030AiA5mZMgvIDEgPP2J1TGRPsgyXZY6VrAcWTyFAzlLgKpPg==" \
   -X GET http://127.0.0.1:6001/config/node/bdb-node-1 | jq .
   ```
A sample output of above command is shown below. The actual content might change depending on the configuration specified in `config.yml`
for the node `bdb-node-1`. The output would only contain the configuration of the node `bdb-node-1`.

```sh
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "node_config": {
      "id": "bdb-node-1",
      "address": "127.0.0.1",
      "port": 6001,
      "certificate": "MIIBsjCCAVigAwIBAgIQYy4vf2+6qRtczxIkNb9fxjAKBggqhkjOPQQDAjAeMRwwGgYDVQQDExNDYXIgcmVnaXN0cnkgUm9vdENBMB4XDTIxMDYxNjExMTMyN1oXDTIyMDYxNjExMTgyN1owJTEjMCEGA1UEAxMaQ2FyIHJlZ2lzdHJ5IENsaWVudCBzZXJ2ZXIwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARWWd8GA/dbkxTiNP7x/LoyAc85sfsIxcmiX+4nzzB3s4SXA+N8YMSiKpsi6gSOVkxKLu43lla2ajyL0Z4WqWvYo3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiEAuyIC0jlV/KcyB8Cz2p3W4aojh+fDCeeRenMwvyP+EcACIDOjDiObMnb/2q2ceAKROr/rzJmakdjkNmw8A0bYL6Pb"
    }
  },
  "signature": "MEUCIB0gWj3xTZ4TkH/FcLhy8X3gVy5n5nOOIz1949HtPqhbAiEAxRgdd8z2LML0zk++rVNldJO+VpVH6E2j6lQrM6lboME="
}
```

## Querying the User Information

One user can query information about another user or self. If the access control is defined for the user entry, it would be enforced during the query.
A user information can be retrieved by issuing a `GET` request on `/user/{userid}` endpoint where `{userid}` should be replaced with the ID of the user
whom information needs to be fetched.

Here, the submitting user needs to sign the `{"user_id":"<submitting_user_id>","target_user_id":"<target_user_id"}` where `<submitting_user_id>` denotes the
ID of the user who is submitting the query and `<target_user_id>` denotes the ID of the user whom information needs to be fetched.

In our `config.yml`, we have an admin entry with ID `admin`. Hence, we do a self query.
In our example, the submitting user `admin` wants to fetch its own information and hence, the following data needs to be signer
`{"user_id":"admin","target_user_id":"admin"}` as follows:

```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","target_user_id":"admin"}'
```
The above command would produce a digital signature and prints it as base64 encoded string as shown below:
```sh
MEQCIB6NmOvgUvYwRLBAFz/OovrGRfhnwbxMwFb4+Dniano6AiBbObFcC6Cv/wcBLXzDf3pG0zybHTSA5ksMGCusfAridw==
```
Once the signature is computed, we can issue a `GET` request using the following `cURL` command
by setting the above signature string in the `Signature` header.
```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: MEQCIB6NmOvgUvYwRLBAFz/OovrGRfhnwbxMwFb4+Dniano6AiBbObFcC6Cv/wcBLXzDf3pG0zybHTSA5ksMGCusfAridw==" \
   -X GET http://127.0.0.1:6001/user/admin | jq .
```
A sample output of the above command is shown below. 
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "user": {
      "id": "admin",
      "certificate": "MIIBsTCCAVigAwIBAgIRANDg6u+E+sdowdPBGXyoD9swCgYIKoZIzj0EAwIwHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTAeFw0yMTA2MTYxMTEzMjdaFw0yMjA2MTYxMTE4MjdaMCQxIjAgBgNVBAMTGUNhciByZWdpc3RyeSBDbGllbnQgYWRtaW4wWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAT9GYTj7ka7se8bEqclDdulRJnpay+EU/b4QUMiLl/9cB1iBgKegVXztZCcQs3S+XR1Y2b/1xSqMbPwHOV5kuUDo3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNHADBEAiB09qz5bAyguEEI4HMUPIBqRTF3RBMjfTKrpIjBh1ai9gIgWLL0SNsD/5a4xjp+fol42npUY13mIpByYg56IW0TsBI=",
      "privilege": {
        "admin": true
      }
    },
    "metadata": {
      "version": {
        "block_num": 1
      }
    }
  },
  "signature": "MEUCIQDN7DAo/2drpnQYB9lonKcqNQRjRooARhw1D+aSZgOhyAIgbAbofC016zyBcccu6BRfwSrYDbQnekbSu9lJVR1FCOw="
}
```

## Checking the Database Existance

To check whether a database exist/created, the user can issue a GET request on `/db/{dbname}` endpoint where `{dbname}` should be replaced with
the `dbname` for which the user needs to perform this check.

For this query, the submitting user needs to sign `{"user_id":"<userid","db_name":"<dbname>"}` where `userid` denotes the submitting user and the
`<dbname>` denotes the name of the database for which the user performs the existance check. 

When the BDB server bootups, it creates a default database called `bdb` in the cluster. Hence, we can check its existance. For this case, the
submitting user `admin` needs to sign `{"user_id":"admin","db_name":"bdb"}` as shown below:

```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","db_name":"bdb"}'
```
The above command would produce a digital signature and prints it as base64 encoded string as shown below:
```sh
MEUCIBzH0qIz88jKdHsJvmQsNNuK3Cf0G+7LDWSiwv6yjba0AiEAgb/hBFZrr3w64M0Q6LmZjQ0i/sjYr27K1DJSlXHWfRU=
```

Once the signature is computed, we can issue a `GET` request using the following `cURL` command
by setting the above signature string in the `Signature` header.
```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: abcd" \
   -X GET http://127.0.0.1:6001/db/bdb | jq .
```
The above command results in the following output:
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
. 

},
    "exist": true
  },
  "signature": "MEQCIAhSD5eQ+lBCaN7C/fILXcHADekGi+1RteDLmBbgHS4sAiAU+h/uwp/CrKmRdgHeAN7wOArRj5BdPC4Qp8Mzw4uIaQ=="
}
```

## Querying a Block Header

A block is a collection of ordererd transactions in blokchain database. The header object within a block holds the block number,
root hash of the transaction merkle tree, root hash of the state merkle tree, and validation information. The root hash of
merkle tree helps in proving immutability and more detail can be found [here]().

To query a block header of a given block, the user can issue a GET request on `/ledger/block/{blocknumber}` endpoint where 
`{blocknumber}` denotes the block whose header needs to be fetched.

As genesis block is the first block in the blockchain database, we can query the same in this example. The submitting user needs
to sign `{"user_id":"<userid>","block_number":<blocknumber>}` where the `<userid>` denotes the id of the user who is submitting
the query and `<blocknumber>` denotes the number of the block whose header needs to be fetched. For our example, the admin user
needs to sign `{"user_id":"admin","block_number":1}` using the following command:

```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","block_number":1}'
```

The above command would produce a digital signature and prints it as base64 encoded string as shown below:
```sh
MEUCIGtzF3TUtJXzsBiQmT16o8wG6nOicJf64yIZWyPhZgrdAiEArbq6sxy4AfBbq3cHXayASvs+FwIaQlUVBm5HZvXfuoc=
```
Once the signature is computed, we can issue a `GET` request using the following `cURL` command
by setting the above signature string in the `Signature` header.
```sh
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: abcd" \
   -X GET http://127.0.0.1:6001/ledger/block/1 | jq .
```
The above command results in the following output:
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "block_header": {
      "base_header": {
        "number": 1
      },
      "tx_merkel_tree_root_hash": "5ebw3HNhBF14XFKXwYOAvUwaiX05cj6F2oTsZBPdL4E=",
      "state_merkel_tree_root_hash": "ut1aKeqCNbfXJad7kUUAwGaqZT5brjZVsPzt74t2/QU=",
      "validation_info": [
        {}
      ]
    }
  },
  "signature": "MEQCIHR2UGiculjlGTioiaxi+v9mciG2UJGQ4GWaiAMTs2W/AiBiwa/BjAKuwxq+cSA8ACznSRZ/37Ta4zEyjVrhkLcTTA=="
}
```
