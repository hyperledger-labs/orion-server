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
