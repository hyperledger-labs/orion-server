---
id: node-config
title: Query a Node's Configuration
---
### Querying a Particular Node's Configuration

While `GET /config/tx` returns the complete configuration, we can use `GET /config/node/{nodeid}` to fetch the configuration of a particular node.
Here, the submitting user needs to sign `{"user_id":"<userid>","node_id":"<nodeid>"}` where `<userid>` denotes the id of the submitting user and
`<nodeid>` denotes the id of the blockchain database for which the user tries to fetch the configuration.

In our example, the JSON data to be signed is `{"user_id":"admin","node_id":"bdb-node-1"}` because the `admin` is submitting a request to fetch
the configuration of the node with id `bdb-node-1`

```shell
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","node_id":"bdb-node-1"}'
```
The above command would produce a digital signature and prints it as base64 encoded string as shown below
```
MEQCIEAxqk1RnWBvCt9OV7zycK3U5itkEumSpEXivv/2R030AiA5mZMgvIDEgPP2J1TGRPsgyXZY6VrAcWTyFAzlLgKpPg==
```

Once the signature is computed, we can issue a `GET` request using the following `cURL` command
by setting the above signature string in the `Signature` header.
```shell
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: MEQCIEAxqk1RnWBvCt9OV7zycK3U5itkEumSpEXivv/2R030AiA5mZMgvIDEgPP2J1TGRPsgyXZY6VrAcWTyFAzlLgKpPg==" \
   -X GET http://127.0.0.1:6001/config/node/bdb-node-1 | jq .
   ```
A sample output of above command is shown below. The actual content might change depending on the configuration specified in `config.yml`
for the node `bdb-node-1`. The output would only contain the configuration of the node `bdb-node-1`.

```webmanifest
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
