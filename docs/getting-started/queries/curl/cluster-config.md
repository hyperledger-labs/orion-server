---
id: cluster-config
title: Query the Cluster Configuration
---

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

```shell
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin"}'
```
The above command would produce a digital signature and prints it as base64 encoded string as shown below
```
MEUCIQCMEdLgfFEOF+vgXLwbeOdUUWnGB5HH2ULkoz15jlk5DgIgbWXuoyqD4szob78hZYiau9LPdJLLqP3bAu7iV98BcW0=
```

Once the signature is computed, we can issue a `GET` request using the following `cURL` command
by setting the above signature string in the `Signature` header.
```shell
curl \
   -H "Content-Type: application/json" \
   -H "UserID: admin" \
   -H "Signature: MEUCIQCMEdLgfFEOF+vgXLwbeOdUUWnGB5HH2ULkoz15jlk5DgIgbWXuoyqD4szob78hZYiau9LPdJLLqP3bAu7iV98BcW0=" \
   -X GET http://127.0.0.1:6001/config/tx | jq .
   ```
A sample output of above command is shown below. The actual content might change depending on the configuration specified in `config.yml`.
For now, all users in the cluster can query the cluster configuration to identity the set of nodes, IP address of each node
along with the listening port number, certificate of the node, etc...

```webmanifest
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
          "heartbeat_ticks": 5,
          "max_inflight_blocks": 50,
          "snapshot_interval_size": 1000000000000
        }
      }
    },
    "metadata": {
      "version": {
        "block_num": 1
      }
    }
  },
  "signature": "MEUCIGaeLwhfXBb/s1k8sIzqCPy+OqkiNJS9EgeSZGlHqoHaAiEAwzyj0h4QHfwmuwBbjY21+huvbE3ltp8apGr6uP921kQ="
}
```
