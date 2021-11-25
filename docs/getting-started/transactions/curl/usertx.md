---
id: usertx
title: User Administration Transaction
---
# User Administration Transaction

We can create, update and delete users of the Orion cluster using the user administration transaction. By issuing a `POST /user/tx {txPayload}`, we can perform the user administration.

Next, we will see an example for
  1. [Addition of Users](#2-addition-of-users)
  2. [Updation of Users](#3-updation-of-a-user)
  3. [Deletion of Users](#4-deletion-of-a-user)

Note that all user administration transactions must be submitted by the admin.

:::caution
It is recommended to start a fresh orion server for executing these examples. Otherwise, you need to carefully change the block number specified as
part of version in the read-set of the transaction payload. You will get more clarity as you read this doc. Once a fresh orion server is started
after removing the `ledger` directory, create two databases named `db1` and `db2`. Refer [here](./dbtx.md#create-databases-named-db1-and-db2)
for an example `cURL` command for creating databases named `db1` and `db2`.
:::

## 1) Addition of Users

### 1.1) Adding users `alice` and `bob`

When the cluster is started for the first time, it will contain only the admin user specified in the `config.yml`. This admin user can add any other user to the cluster.
In the below example, the admin user is adding two users named `alice` and `bob` with certain privileges. The crypto materials associated with `alice` and `bob` can
be found in `/deployment/sample/crypto`.

```shell
curl \
    -H "Content-Type: application/json" \
    -H "TxTimeout: 10s" \
    -X POST http://127.0.0.1:6001/user/tx \
    --data '{
        "payload": {
            "user_id": "admin",
            "tx_id": "7b6d6414-9b58-45d0-9723-1f31712add01",
            "user_writes": [
                {
                    "user": {
                        "id": "alice",
                        "certificate": "MIIBsjCCAVigAwIBAgIRAJp7i/UhOnaawHTSdkzxR1QwCgYIKoZIzj0EAwIwHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTAeFw0yMTA2MTYxMTEzMjdaFw0yMjA2MTYxMTE4MjdaMCQxIjAgBgNVBAMTGUNhciByZWdpc3RyeSBDbGllbnQgYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASdCmAgHdqck7uhAK5siEF/O1EIUEIYtiR3XVEjbVhNe/6GXFShtsSThXYL9/XK6p4qF4oSy9j/PURMGnWbzSnso3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiEAsRZlR4sDyxS//BJnYpC684EWu1hO/JU8rkNW6Nn0FFQCIH/p6m6ELkLNQpx+1QJsWWtH/LdW94WinVylhuA4jggQ",
                        "privilege": {
                            "db_permission": {
                                "db1": 0,
                                "db2": 1
                            }
                        }
                    }
                },
                {
                    "user": {
                        "id": "bob",
                        "certificate": "MIIBrzCCAVWgAwIBAgIQZOQpmvY31R8yeyy3ClrJtzAKBggqhkjOPQQDAjAeMRwwGgYDVQQDExNDYXIgcmVnaXN0cnkgUm9vdENBMB4XDTIxMDYxNjExMTMyN1oXDTIyMDYxNjExMTgyN1owIjEgMB4GA1UEAxMXQ2FyIHJlZ2lzdHJ5IENsaWVudCBib2IwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASUDaIwGvRPPHHMzw4UFPTX5BTuPons8Xv3AR6k/8dDJQsn09qdtKWauLLLGxiLNDY2J8S0qPzJhJVPGF6h/l9Uo3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiAFxiyZgtiTwvMFF6jKtUE5vV0YzthpWmdRiUIbUclKzQIhALQolKPJl9xmv66wOyJTvR2q13Fb6j75M4WGcG4KfjDZ",
                        "privilege": {
                            "db_permission": {
                                "db1": 0,
                                "db2": 0
                            }
                        }
                    },
                    "acl": {
                        "read_users": {
                            "admin": true
                        }
                    }
                }
          ]
    },
    "signature": "MEUCIHLCSwMzwxmnRfB6s1eON2bMfgDwFvxoSqaZ6ACXcbn0AiEA8KhjY56tSRg9Hh9UGchhGybTV2rWl1NcsAPLyW71Vu8="
}'
```
The user `alice` has read only access on the database `db1` and read-write access on the database `db2`. These privileges are defined under `db_permission`.
The `"db1":0` denotes that the user has read-only privilege on database `db1` while `"db2":1` denotes that the user has read-write privilege on database `db2`.
In other words, `0` denotes read-only privilege and `1` denotes read-write privilege. As the access control is not defined for the user,
any user can read the credential and privilege of `alice` but only `admin` user can modify the properties of `alice` user.

The user `bob` has read-only privilege on the database `db1` and `db2`. Further, only the `admin` user can read the credential and privilege of `bob`.

Moreover, the user `bob` cannot be modified as the `"read_write_users"` section is left out empty. This means no user has permission to write to user `bob`.

The signature for the above transaction payload is computed by executing the following command:
```shell
/bin/signer -privatekey=deployment/sample/crypto/admin/admin.key \
    -data='{"user_id":"admin","tx_id":"7b6d6414-9b58-45d0-9723-1f31712add01","user_writes":[{"user":{"id":"alice","certificate":"MIIBsjCCAVigAwIBAgIRAJp7i/UhOnaawHTSdkzxR1QwCgYIKoZIzj0EAwIwHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTAeFw0yMTA2MTYxMTEzMjdaFw0yMjA2MTYxMTE4MjdaMCQxIjAgBgNVBAMTGUNhciByZWdpc3RyeSBDbGllbnQgYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASdCmAgHdqck7uhAK5siEF/O1EIUEIYtiR3XVEjbVhNe/6GXFShtsSThXYL9/XK6p4qF4oSy9j/PURMGnWbzSnso3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiEAsRZlR4sDyxS//BJnYpC684EWu1hO/JU8rkNW6Nn0FFQCIH/p6m6ELkLNQpx+1QJsWWtH/LdW94WinVylhuA4jggQ","privilege":{"db_permission":{"db1":0,"db2":1}}}},{"user":{"id":"bob","certificate":"MIIBrzCCAVWgAwIBAgIQZOQpmvY31R8yeyy3ClrJtzAKBggqhkjOPQQDAjAeMRwwGgYDVQQDExNDYXIgcmVnaXN0cnkgUm9vdENBMB4XDTIxMDYxNjExMTMyN1oXDTIyMDYxNjExMTgyN1owIjEgMB4GA1UEAxMXQ2FyIHJlZ2lzdHJ5IENsaWVudCBib2IwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASUDaIwGvRPPHHMzw4UFPTX5BTuPons8Xv3AR6k/8dDJQsn09qdtKWauLLLGxiLNDY2J8S0qPzJhJVPGF6h/l9Uo3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiAFxiyZgtiTwvMFF6jKtUE5vV0YzthpWmdRiUIbUclKzQIhALQolKPJl9xmv66wOyJTvR2q13Fb6j75M4WGcG4KfjDZ","privilege":{"db_permission":{"db1":0,"db2":0}}},"acl":{"read_users":{"admin":true}}}]}'
```
**Output**
```
MEYCIQCBA3pw0C1wgJMjOYDnhr5C0QeaSfradKdCFVCSWwXFdwIhAP80o5VZY2VBp4Lr5+4lG9hNuFL3Da53LXP9N7uAyNfn
```

On a successful commit of the above transaction, the submitter of the transaction would receive the following transaction receipt:
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "receipt": {
      "header": {
        "base_header": {
          "number": 4,
          "previous_base_header_hash": "gc2T0zndfbSAsw9J/67Pzjk1IAVBjE9Ih93qjwYro1k=",
          "last_committed_block_hash": "eUxPHg5TFfTU3CRmYISAHDPI6DRHxbiQccaeytl4WBk=",
          "last_committed_block_num": 3
        },
        "skipchain_hashes": [
          "eUxPHg5TFfTU3CRmYISAHDPI6DRHxbiQccaeytl4WBk="
        ],
        "tx_merkel_tree_root_hash": "5nu+kdyEzZOIcy6qDDCls+GSRKK0aRdp6lbZJBUnZJQ=",
        "state_merkel_tree_root_hash": "milzir+V4vvodu+e2BPv/j5XKlh6SfJmy3cRsBlJttw=",
        "validation_info": [
          {}
        ]
      }
    }
  },
  "signature": "MEYCIQDxAuuukwThUp5ytZikobfQ0iBLMYLI6TRoK7322eNuqAIhAI5SkomPRnxH0K3iF6xqNFJUCgTHVuyelFgb9u1B4a8D"
}
```

### 1.2) Check the existance of alice
Once the transaction get committed, we can query the user information as follows:

Let's fetch the user `alice`.
```shell
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key \
    -data='{"user_id":"admin","target_user_id":"alice"}'
```
**Output**
```
MEYCIQDioRVRhtdaLEjFSeCPqrVCCtdwq+hvy7Y+i3cXaqhZ3wIhAK/gmdftR4x0KF3w8V86hSYXPehf/rlO8QcSnU9sFnvC
```
```shell
curl \
     -H "Content-Type: application/json" \
     -H "UserID: admin" \
     -H "Signature: MEYCIQDioRVRhtdaLEjFSeCPqrVCCtdwq+hvy7Y+i3cXaqhZ3wIhAK/gmdftR4x0KF3w8V86hSYXPehf/rlO8QcSnU9sFnvC" \
     -X GET http://127.0.0.1:6001/user/alice | jq .
```
**Output**
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "user": {
      "id": "alice",
      "certificate": "MIIBsjCCAVigAwIBAgIRAJp7i/UhOnaawHTSdkzxR1QwCgYIKoZIzj0EAwIwHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTAeFw0yMTA2MTYxMTEzMjdaFw0yMjA2MTYxMTE4MjdaMCQxIjAgBgNVBAMTGUNhciByZWdpc3RyeSBDbGllbnQgYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASdCmAgHdqck7uhAK5siEF/O1EIUEIYtiR3XVEjbVhNe/6GXFShtsSThXYL9/XK6p4qF4oSy9j/PURMGnWbzSnso3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiEAsRZlR4sDyxS//BJnYpC684EWu1hO/JU8rkNW6Nn0FFQCIH/p6m6ELkLNQpx+1QJsWWtH/LdW94WinVylhuA4jggQ",
      "privilege": {
        "db_permission": {
          "db1": 0,
          "db2": 1
        }
      }
    },
    "metadata": {
      "version": {
        "block_num": 4
      }
    }
  },
  "signature": "MEYCIQDY3OV2xzqe22X7PzO6UIeDY6t3Qn7DcMk5z/G8SGshvQIhAN8DnATYxIQyoj3jPufgoHuOJDTCnvpEj4kF0WtLHGRq"
}
```

### 1.3) Check the existance of bob

Let's fetch the user `bob`.
```shell
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","target_user_id":"bob"}'
```
**Output**
```
MEQCICeSXCL6Atyf3hgbd0XtC4L6HT0qXTxLyvUslAv5pYMqAiAxirniW1NW1lS2pUT8G0XeWYUhUKCjjWlq6WTuGAOxEQ==
```
```shell
curl \
     -H "Content-Type: application/json" \
     -H "UserID: admin" \
     -H "Signature: MEQCICeSXCL6Atyf3hgbd0XtC4L6HT0qXTxLyvUslAv5pYMqAiAxirniW1NW1lS2pUT8G0XeWYUhUKCjjWlq6WTuGAOxEQ==" \
     -X GET http://127.0.0.1:6001/user/bob | jq .
```
**Output**
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "user": {
      "id": "bob",
      "certificate": "MIIBrzCCAVWgAwIBAgIQZOQpmvY31R8yeyy3ClrJtzAKBggqhkjOPQQDAjAeMRwwGgYDVQQDExNDYXIgcmVnaXN0cnkgUm9vdENBMB4XDTIxMDYxNjExMTMyN1oXDTIyMDYxNjExMTgyN1owIjEgMB4GA1UEAxMXQ2FyIHJlZ2lzdHJ5IENsaWVudCBib2IwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASUDaIwGvRPPHHMzw4UFPTX5BTuPons8Xv3AR6k/8dDJQsn09qdtKWauLLLGxiLNDY2J8S0qPzJhJVPGF6h/l9Uo3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiAFxiyZgtiTwvMFF6jKtUE5vV0YzthpWmdRiUIbUclKzQIhALQolKPJl9xmv66wOyJTvR2q13Fb6j75M4WGcG4KfjDZ",
      "privilege": {
        "db_permission": {
          "db1": 0
        }
      }
    },
    "metadata": {
      "version": {
        "block_num": 4
      },
      "access_control": {
        "read_users": {
          "admin": true
        }
      }
    }
  },
  "signature": "MEUCIEuGxnvi2nwzJ1AZdUBhFBkOkv26kOupKZbCgt03S85zAiEAwiyE1xJEl0F9Kx/CyylKP6dogKrcNRiVXMJcqknrOE4="
}
```

## 2) Updation of a User

### 2.1) Remove all privileges of `alice`
Let's remove all privileges given to `alice`. In order to do that, we can execute the following steps:

1. Fetch the current committed information of the `alice`.
2. Remove the privilege section and construct the transaction payload.
3. Add the digital signature on the new transaction payload.
3. Submit the transaction payload by issuing a `POST /user/tx {txPayload}`

Though we can update multiple users within a transaction, for simplicity, in this example, we are updating only a single user `alice`.

> **Important for Successful Execution of this Example:**
The `user_reads` section in the below transaction payload contains the version of the read information. If this does not match the committed version, the transaction would be invalidated.
This is useful because the `alice` user can be updated by any other admin between step 1 and 2 listed above.
To ensure, serializability isolation, we must pass the read version. In our example, we use the version present in [this](#check-the-existance-of-alice-and-bob)
returned result. To be specific, we can see the the version in the metadata section of the query result as shown below:
```webmanifest
    "metadata": {
      "version": {
        "block_num": 3
      }
    }
```
This version might be different for you. Hence, provide the appropirate version number in the transaction payload and compute the digital signature.

The `user_reads` section says that commit this transaction only if users specified in the `user_reads` list are at a specified version as per the current
committed state. Otherwise, invalidate the transaction and do not apply the changes requested by the transaction.

> Note that it is not necessary to pass the version in `user_reads`. If the `user_reads` is left out, the write would be considered as a blind write.

> Within a single transaction, we can update more than a single user. For simplicity, the example updates a single user only.

```shell
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/user/tx \
   --data '{
	"payload": {
		"user_id": "admin",
		"tx_id": "1b6d6414-9b58-45d0-9723-1f31712add02",
        "user_reads": [
			{
				"user_id": "alice",
				"version": {
					"block_num": 3
				}
			}
		],
		"user_writes": [
			{
				"user": {
					"id": "alice",
                    "certificate": "MIIBsjCCAVigAwIBAgIRAJp7i/UhOnaawHTSdkzxR1QwCgYIKoZIzj0EAwIwHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTAeFw0yMTA2MTYxMTEzMjdaFw0yMjA2MTYxMTE4MjdaMCQxIjAgBgNVBAMTGUNhciByZWdpc3RyeSBDbGllbnQgYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASdCmAgHdqck7uhAK5siEF/O1EIUEIYtiR3XVEjbVhNe/6GXFShtsSThXYL9/XK6p4qF4oSy9j/PURMGnWbzSnso3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiEAsRZlR4sDyxS//BJnYpC684EWu1hO/JU8rkNW6Nn0FFQCIH/p6m6ELkLNQpx+1QJsWWtH/LdW94WinVylhuA4jggQ"
			    }
            }
		]
	},
    "signature": "MEUCIDTrlQ8fKu2ZeVWincE8RzWd/Y/MWjZGHEu37EckcTFnAiEA/kmKAOc2LK2GHFIA1wU+9/oog0Nqj3GLYPjSFGCeOOk="
}'
```
In the above transaction, we have removed the privilege section from the user information (compare against the query result provided [here](#check-the-existance-of-alice-and-bob)).
The signature is computed using the following command
```shell
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","tx_id":"1b6d6414-9b58-45d0-9723-1f31712add02","user_reads":[{"user_id":"alice","version":{"block_num":3}}],"user_writes":[{"user":{"id":"alice","certificate":"MIIBsjCCAVigAwIBAgIRAJp7i/UhOnaawHTSdkzxR1QwCgYIKoZIzj0EAwIwHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTAeFw0yMTA2MTYxMTEzMjdaFw0yMjA2MTYxMTE4MjdaMCQxIjAgBgNVBAMTGUNhciByZWdpc3RyeSBDbGllbnQgYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASdCmAgHdqck7uhAK5siEF/O1EIUEIYtiR3XVEjbVhNe/6GXFShtsSThXYL9/XK6p4qF4oSy9j/PURMGnWbzSnso3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiEAsRZlR4sDyxS//BJnYpC684EWu1hO/JU8rkNW6Nn0FFQCIH/p6m6ELkLNQpx+1QJsWWtH/LdW94WinVylhuA4jggQ"}}]}'
```
**Output**
```
MEQCIGy62qM0ZGdCd5FsCvgKJ0wVQi/uL5Gy0IXhevsNxUaeAiA2pOfu5vVuQ9MJnYakFyw8HcgHR6AA3NqyxF8p3E6exw==
```

### 2.2) Check the privilege of `alice`
Once the transaction get committed, we can query the user information as follows to check whether all privileges given to the `alice` have been removed:

```shell
curl \
     -H "Content-Type: application/json" \
     -H "UserID: admin" \
     -H "Signature: MEYCIQDioRVRhtdaLEjFSeCPqrVCCtdwq+hvy7Y+i3cXaqhZ3wIhAK/gmdftR4x0KF3w8V86hSYXPehf/rlO8QcSnU9sFnvC" \
     -X GET http://127.0.0.1:6001/user/alice | jq .
```
**Output**
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "user": {
      "id": "alice",
      "certificate": "MIIBsjCCAVigAwIBAgIRAJp7i/UhOnaawHTSdkzxR1QwCgYIKoZIzj0EAwIwHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTAeFw0yMTA2MTYxMTEzMjdaFw0yMjA2MTYxMTE4MjdaMCQxIjAgBgNVBAMTGUNhciByZWdpc3RyeSBDbGllbnQgYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASdCmAgHdqck7uhAK5siEF/O1EIUEIYtiR3XVEjbVhNe/6GXFShtsSThXYL9/XK6p4qF4oSy9j/PURMGnWbzSnso3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiEAsRZlR4sDyxS//BJnYpC684EWu1hO/JU8rkNW6Nn0FFQCIH/p6m6ELkLNQpx+1QJsWWtH/LdW94WinVylhuA4jggQ",
    },
    "metadata": {
      "version": {
        "block_num": 5
      }
    }
  },
  "signature": "MEYCIQDY3OV2xzqe22X7PzO6UIeDY6t3Qn7DcMk5z/G8SGshvQIhAN8DnATYxIQyoj3jPufgoHuOJDTCnvpEj4kF0WtLHGRq"
}
```
As we can see, the privilege section is no longer present and the version has been increased too.

## 3) Deletion of a User

### 3.1) Delete the user `alice`
Let's delete `alice` from the cluster. In order to do that, we can execute the following steps:

1. Fetch the current committed information of the `alice` to get the committed version.
2. Add the committed version to the `user_reads`.
3. Add the `alice` to the `user_deletes`.
4. Compute and add digital signature to the transaction payload.
5. Submit the transaction payload by issuing a `POST /user/tx {txPayload}`.

The example uses `"block_num": 4` as the version. While executing this example, query the user `alice` and use the version provided in the output of the query.
Refer [here](#updation-of-a-user) for the query result of `alice` after being updated. In the query result, we have
```webmanifest
    "metadata": {
      "version": {
        "block_num": 4
      }
    }
```
Hence, this example uses `"block_num": 4` as the version.

> Note that it is not necessary to pass the version in `user_reads`. If the `user_reads` is left out, the delete would be considered as a blind delete. For blind delete, steps 1 and 2 are not needed.

> Within a single transaction, we can delete more than a single user. For simplicity, the example delete a single user only.

```shell
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/user/tx \
   --data '{
	"payload": {
		"user_id": "admin",
		"tx_id": "1b6d6414-9b58-45d0-9723-1f31712add04",
        "user_reads": [
			{
				"user_id": "alice",
				"version": {
					"block_num": 4
				}
			}
		],
		"user_deletes": [
			{
				"user_id": "alice"
			}
		]
	},
    "signature": "MEQCID1O2vmSxgzJT8KNMws/ckV1oIf1R4oml6aWeZNzvjI9AiAphdfNAn+VKCJPPh+d0oW4/MH+cXy4xwQXXU5FQ8LpMQ=="
}'
```

The section `user_deletes` is an array and we can pass as many number of users in it and all will be deleted if they exist.
The signature on the payload is computed using the following command
```shell
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","tx_id":"1b6d6414-9b58-45d0-9723-1f31712add04","user_reads":[{"user_id":"alice","version":{"block_num":4}}],"user_deletes":[{"user_id":"alice"}]}'
```
**Output**
```
MEQCID1O2vmSxgzJT8KNMws/ckV1oIf1R4oml6aWeZNzvjI9AiAphdfNAn+VKCJPPh+d0oW4/MH+cXy4xwQXXU5FQ8LpMQ==
```

Once the transaction is validated and committed, the submitter of the transaction recevies the following transaction receipt
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "receipt": {
      "header": {
        "base_header": {
          "number": 7,
          "previous_base_header_hash": "+KCuJm2x9LzsIBFmAVkW6w/Hmw7IRjl52aZjNI7juR8=",
          "last_committed_block_hash": "7nEaUCRBiGvmrhbhl7tzupb0K/KjGdS/SSJtCLjvsjA=",
          "last_committed_block_num": 6
        },
        "skipchain_hashes": [
          "7nEaUCRBiGvmrhbhl7tzupb0K/KjGdS/SSJtCLjvsjA=",
          "IHP38VOoGoVHAHROXPZJ8ZWG3pZ6rmqQYovVzm5i7rw="
        ],
        "tx_merkel_tree_root_hash": "iz84iY89BolcBkVD8okAAqg5duGT/+eivcTO4OcpnNM=",
        "state_merkel_tree_root_hash": "tAb0KTsd9JgCQ1YZMwYLs2O3wpo0ynA7h22B8pKzBC0=",
        "validation_info": [
          {}
        ]
     }
    }
  },
  "signature": "MEUCIGWLKxgnxwlznZyBbKbpiFcnbUv32CbkBaFitOxEWnSMAiEAqcNh9pswgvF7f1leWrteozij2zSlG78gcfP5+eAfHhA="
}
```

### 3.2) Check the non-existence of the user `alice`
If we query the user, the response would not contain any details associated with the user.
```shell
curl \
     -H "Content-Type: application/json" \
     -H "UserID: admin" \
     -H "Signature: MEYCIQDioRVRhtdaLEjFSeCPqrVCCtdwq+hvy7Y+i3cXaqhZ3wIhAK/gmdftR4x0KF3w8V86hSYXPehf/rlO8QcSnU9sFnvC" \
     -X GET http://127.0.0.1:6001/user/alice | jq .
```
**Output**
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    }
  },
  "signature": "MEUCIDV6UgjPLtM5c+WqQ+Ue5xcKe/w85nAdwwRl7qPqJByVAiEA4RTqXa8Xf8S+O8YPRpFgIHzOCH5jOHkL2jMmLG7Zdpw="
}
```
