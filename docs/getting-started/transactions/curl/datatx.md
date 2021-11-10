---
id: datatx
title: Data Transaction
output:
  html_document:
    number_sections: true
---
# Data Transaction

We can store, update, delete any state, i.e., key-value pair, on the ledger by issuing a data transaction.
By submitting a `POST /data/tx {txPaylod}`, we can perform a data transaction where `txPayload` contains reads, writes, and deletes of states.

Using a data transaction, we can do the following:

  1. [Creation of new states](#2-creation-of-new-states)
  2. [Updation of an existing states](#3-updation-of-an-existing-state)
  3. [Deletion of an existing states](#4-deletion-of-an-existing-state)
  3. [Creation, Updation, Deletion of states within a single transaction](#5-creation-updation-deletion-of-states-within-a-single-transaction)
  4. [Operations on multiple databases](#6-operations-on-multiple-databases-in-a-single-transaction)
  5. [Multi-signatures transaction](#7-multi-signatures-transaction)

## (1) Pre-Requisite
It is recommended to start a fresh orion server for executing these examples. Otherwise, you need to carefully change the block number specified
as part of version in the read-set of the transaction payload. You will get more clarity as you read this doc.

Once a fresh orion server is started after removing the `ledger` directory,
  - [create two databases named db1 and db2](./dbtx#creation-of-databases), and
  - [create two users alice and bob](./usertx#addition-of-users).

## (2) Creation of new states

### (2.1) Create a state with key `key1`

Let's store a new state with the key `key1` with the value `{"name":"abc","age":31,"graduated":true}`.

```shell
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/data/tx \
   --data '{
	"payload": {
		"must_sign_user_ids": [
            "alice"
        ],
		"tx_id": "1b6d6414-9b58-45d0-9723-1f31712add81",
        "db_operations": [
            {
                "db_name": "db2",
		        "data_writes": [
			        {
                        "key": "key1",
                        "value": "eyJuYW1lIjoiYWJjIiwiYWdlIjozMSwiZ3JhZHVhdGVkIjp0cnVlfQ==",
                        "acl": {
                            "read_users": {
                                "alice": true,
                                "bob": true
                            },
                            "read_write_users": {
                                "alice": true
                            }
                        }
			        }
                ]
            }
		]
	},
    "signatures": {
        "alice": "MEUCIHS1BA4ZIeLcmlb/HSwhXGIuzqZOXxpirHevWx426nZgAiEAnCk3hoDXZ0Pn5jlU0igQLkHT3TU08qWH+rPgxVCYbD0="
    }
}'
```

The payload contains `must_sign_user_ids` which is a list of user ids who must sign the transaction's payload. The
`db_operations` hold the `data_writes` to be applied on the specified `db_name`, i.e., `db2`. The `value` in `data_writes` must
be encoded in base64. We need to use the `encoder` utility to get the base64 encoded output as shown below:

```shell
./bin/encoder -data='{"name":"abc","age":31,"graduated":true}'
```

**Output**
```shell
eyJuYW1lIjoiYWJjIiwiYWdlIjozMSwiZ3JhZHVhdGVkIjp0cnVlfQ==
```

The `acl` contains list of users in the `read_users` who can read-only the state and a list of
users in the `read_write_users` who can both read and write to the state. Here, the `signatures` holds a map of
each user in the `must_sign_user_ids` to their digital signature.

<details><summary>Signature Computation</summary>
<div>

The signature is computed using the `alice` private key as shown below:
```shell 
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"must_sign_user_ids":["alice"],"tx_id":"1b6d6414-9b58-45d0-9723-1f31712add81","db_operations":[{"db_name":"db2","data_writes":[{"key":"key1","value":"eyJuYW1lIjoiYWJjIiwiYWdlIjozMSwiZ3JhZHVhdGVkIjp0cnVlfQ==","acl":{"read_users":{"alice":true,"bob":true},"read_write_users":{"alice":true}}}]}]}'
```

</div>
<div>
Output

```shell
MEUCIHS1BA4ZIeLcmlb/HSwhXGIuzqZOXxpirHevWx426nZgAiEAnCk3hoDXZ0Pn5jlU0igQLkHT3TU08qWH+rPgxVCYbD0=
```

</div>
</details>

Once the above transaction gets committed, the submitter of the transaction would receive the transaction receipt
<details><summary>Transaction Receipt</summary>
<div>

```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "receipt": {
      "header": {
        "base_header": {
          "number": 5,
          "previous_base_header_hash": "0hi2UZF5+X4xcGhobE1akVFZ/depxYCWShukzF+KrNk=",
          "last_committed_block_hash": "t9F0hOUPIuyk8upeQhpDpTY/wLP3yoLuA7NBqNE0HUo=",
          "last_committed_block_num": 4
        },
        "skipchain_hashes": [
          "t9F0hOUPIuyk8upeQhpDpTY/wLP3yoLuA7NBqNE0HUo=",
          "PAFz7foMQVLrK0q1bO9TYxW9ZwUMl7rb6G3DZoSD1FY=",
          "xz3tsHvNxVNsB709SBOJ3s2fuUXo2GciBGThc70aaTM="
        ],
        "tx_merkel_tree_root_hash": "UTf7CV8x9/QC9luiVwe5QzHVg9hvCHNLxWAC2iD/5Zw=",
        "state_merkel_tree_root_hash": "+kBbsknNLNuDVKxDTktcgcJMsR9gRAHHW03plIjrTg0=",
        "validation_info": [
          {}
        ]
      }
    }
  },
  "signature": "MEQCIDwJOH8ZNqs2H1rD+GWTTm03Cu1X+LcILP5NnPpmZ14LAiAJHmD4/E2noofY47eyjJm5NCcAmTUytqoB8Dat3xeVaQ=="
}
```

</div>
</details>

### (2.2) Checking the existance of `key1`

Let's query the node to see whether `key1` exists. The query can be submitted by either `alice` or `bob` as both have
the read permission to this key. No one else can read `key1` including the admin user of the node. In this example,
we use `bob` to query the key.

First, compute the bob signature on the request payload.

```shell
./bin/signer -privatekey=deployment/sample/crypto/bob/bob.key \
    -data='{"user_id":"bob","db_name":"db2","key":"key1"}'
```

**Output**
```
MEUCIQDm6dLmAdd0X49JygTiUkh+brZxprWSr2+hcAH+QIu3AAIgF+m7kO33YXyyqSbnXS9HR79wt/aL3JGhKvXFQaFBJms=
```

Second, submit the query

```shell
 curl \
     -H "Content-Type: application/json" \
     -H "UserID: bob" \
     -H "Signature: MEUCIQDm6dLmAdd0X49JygTiUkh+brZxprWSr2+hcAH+QIu3AAIgF+m7kO33YXyyqSbnXS9HR79wt/aL3JGhKvXFQaFBJms=" \
     -X GET http://127.0.0.1:6001/data/db2/key1 | read json; ./base64_decoder -getresponse=$json | jq .
```

The default output would contain the base64 encoded string of the value field. To print a human readable value, we
use the `base64_decoder` utility.

**Output**
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
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
  "signature": "MEUCID5QuzmQukLWGFvPo0BCSmc2/E5IUHt8wOTVC1i5sxlOAiEAir2/H1AUfeJnhkJ/N4bdwn7wZN903oyenV0doIaagzw="
}
```

The result contains the base64 decoded `value` associated with the key and also the `access_control` and `version` as part of the
`metadata`. In order to verify the signature, do not use the output of `base64_decoder` utility but the `cURL` output.

## (3) Updation of an existing state

### (3.1) Update the key `key1`

Let's update the value of `key1`. In order to do that, we need to execute the following three steps:

1. Fetch `key1` from the server.
2. Construct the updated value and transaction payload including the read version.
    - update the `age` from `31`. The new value would be `{"name":"abc","age":32,"graduated":true}`
3. Submit the data transaction

> Note that the `data_reads` should contain the version of the `key1` that was read before modifying the value of `key1`.
In otherword, the version should be the last committed version. If `data_reads` is kept empty, the `data_writes`
would be considered as blind write.

```shell
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/data/tx \
   --data '{
	"payload": {
		"must_sign_user_ids": [
            "alice"
        ],
		"tx_id": "1b6d6414-9b58-45d0-9723-1f31712add83",
        "db_operations": [
            {
                "db_name": "db2",
                "data_reads": [
                    {
                        "key": "key1",
                        "version": {
                            "block_num": 4
                        }
                    }
                ],
		        "data_writes": [
			        {
                        "key": "key1",
				        "value": "eyJuYW1lIjoiYWJjIiwiYWdlIjozMiwiZ3JhZHVhdGVkIjp0cnVlfQ==",
                        "acl": {
                            "read_users": {
                                "alice": true
                            },
                            "read_write_users": {
                                "alice": true
                            }
                        }
			        }
                ]
            }
		]
	},
    "signatures": {
        "alice": "MEUCIQDU+vdGZny6RfjIujjJ/ZZPv43OEQ9JsxZ08gx1FDTWMgIgArbhOw+42yy0zP1YLXpVJ5BVyP/3Cqft4OWW8bsUKqU="
    }
}'
```

The `data_reads` holds the read key, `key1` and its version `"block_num":4`. Please fetch the `key1` to get the version
number in your setup. We have updated the `value` of the `key1` and removed the user `bob` from the reader's list.
The required signature is computed with alice private key using the following command

```shell
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"must_sign_user_ids":["alice"],"tx_id":"1b6d6414-9b58-45d0-9723-1f31712add83","db_operations":[{"db_name":"db2","data_reads":[{"key":"key1","version":{"block_num":4}}],"data_writes":[{"key":"key1","value":"eyJuYW1lIjoiYWJjIiwiYWdlIjozMiwiZ3JhZHVhdGVkIjp0cnVlfQ==","acl":{"read_users":{"alice":true},"read_write_users":{"alice":true}}}]}]}'
 ```

**Output**
```shell
MEUCIQDU+vdGZny6RfjIujjJ/ZZPv43OEQ9JsxZ08gx1FDTWMgIgArbhOw+42yy0zP1YLXpVJ5BVyP/3Cqft4OWW8bsUKqU=
```

Once the above transaction gets committed, the submitter of the transaction would get the following transaction receipt
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "receipt": {
      "header": {
        "base_header": {
          "number": 6,
          "previous_base_header_hash": "paoW5AH3PRvYnFgsuQGl8eVxA2cYoeHakyHY+mIogf0=",
          "last_committed_block_hash": "xrRU2Hy5curlSi/YafvCCeKaAb8bF9vUNGhr5usK4fc=",
          "last_committed_block_num": 5
        },
        "skipchain_hashes": [
          "xrRU2Hy5curlSi/YafvCCeKaAb8bF9vUNGhr5usK4fc="
        ],
        "tx_merkel_tree_root_hash": "bSgG1t1yZhOGVOrqT4LR+wXdtB/LTXvYy2De1ouaq+U=",
        "state_merkel_tree_root_hash": "TueFaZX9xju1WimSV0g7wxuekIJ7nq6Bf+o+WNWSnkc=",
        "validation_info": [
          {}
        ]
      }
    }
  },
  "signature": "MEQCIEaCKYOLUaa55VFdssKQfiFKnPY7gzm4sTw2loSTe1fnAiB5fzlYRg4dYjtjgWjHxiQHsDtHgDP+8ZSOdvJww3CSww=="
}
```

### (3.2) Checking the existance of the updated key `key1`

Let's query the node to see whether `key1` has been updated. The query can be submitted only by `alice` as `bob` is
no longer in the reader's list of the `key1`. No one else can read `key1` including the admin user of the node.
In this example, we use `alice` to query the key.

First, compute the alice signature on the request payload.

```shell
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"user_id":"alice","db_name":"db2","key":"key1"}'
```

**Output**
```shell
MEYCIQDUVfuDVppJ2BLuiD662M4iuBYdn2E2ttyspZo6UXYmNgIhAOEvZTdRe7d9/bkVliplmpFHeypKbz7wMHPluGYipqvw
```

Second, submit the query

```shell
curl \
    -H "Content-Type: application/json" \
    -H "UserID: alice" \
    -H "Signature: MEYCIQDUVfuDVppJ2BLuiD662M4iuBYdn2E2ttyspZo6UXYmNgIhAOEvZTdRe7d9/bkVliplmpFHeypKbz7wMHPluGYipqvw" \
    -X GET http://127.0.0.1:6001/data/db2/key1 | read json; ./base64_decoder -getresponse=$json | jq .
```

**Output**
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "value": "{\"name\":\"abc\",\"age\":32,\"graduated\":true}",
    "metadata": {
      "version": {
        "block_num": 5
      },
      "access_control": {
        "read_users": {
          "alice": true
        },
        "read_write_users": {
          "alice": true
        }
      }
    }
  },
  "signature": "MEUCIQCW2w0ec3AN7/y/1H5cuaRvmmBFVsmNY3LeggwNzYJhyQIgbUGQ6UqI33bAJNRKWxZ5FP278KQfCjtW10tSa7e3/XQ="
}
```

The result contains the updated `value` associated with the key and also the `access_control` and `version` as part of the
`metadata`. In the update state, we can find that the `value`, `version`, and `access_control` have changed.

## (4) Deletion of an existing state

### (4.1) Delete the key `key1`

Let's delete the key `key1`. In order to do that, we need to execute the following three steps:

1. Read the `key1` from the server. If we want to blidly delete `key1`, this step can be avoided and also, the read version is not needed.
2. Construct the transaction payload including the read version.
3. Submit the data transaction

> Note that the `data_reads` should contain the version of the `key1` that was read before modifying the value of `key1`. If `data_reads` is left out, the delete would be applied blindly.

```shell
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/data/tx \
   --data '{
	"payload": {
		"must_sign_user_ids": [
            "alice"
        ],
		"tx_id": "1b6d6414-9b58-45d0-9723-1f31712add85",
        "db_operations": [
            {
                "db_name": "db2",
                "data_reads": [
                    {
                        "key": "key1",
                        "version": {
                            "block_num": 5
                        }
                    }
                ],
		        "data_deletes": [
			        {
                        "key": "key1"
			        }
                ]
            }
		]
	},
    "signatures": {
        "alice": "MEUCICkMMnle29bx7MP5dPE3INwnZk1jw0ueXOlYUp2j7K90AiEA2M6GjiuzG5aDl0qKua+thfPGw5PiDICYk9XNgKZXtX0="
    }
}'
```
The transaction should be submitted by `alice` as only she has the write permission which is required for the delete operation too.
The `db_operations` holds the `data_reads` with `key1` and version `"block_num":5`. The `data_deletes` list contains the key `key1`.
The signature is computed using the following command
```shell
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"must_sign_user_ids":["alice"],"tx_id":"1b6d6414-9b58-45d0-9723-1f31712add85","db_operations":[{"db_name":"db2","data_reads":[{"key":"key1","version":{"block_num":5}}],"data_deletes":[{"key":"key1"}]}]}'
```

**Output**
```shell
MEUCICkMMnle29bx7MP5dPE3INwnZk1jw0ueXOlYUp2j7K90AiEA2M6GjiuzG5aDl0qKua+thfPGw5PiDICYk9XNgKZXtX0=
```

Once the above transaction gets committed, the submitting user would get the following transaction receipt
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
          "previous_base_header_hash": "E1QGMExoA6Nt2MEk9GBE+NHDCzpVR4YuBBeYMZONbKU=",
          "last_committed_block_hash": "WwuMQlvM05Kd/jh+6iD0Kt8ZxMXEdC1wEOk0wzxCBfc=",
          "last_committed_block_num": 6
        },
        "skipchain_hashes": [
          "WwuMQlvM05Kd/jh+6iD0Kt8ZxMXEdC1wEOk0wzxCBfc=",
          "xrRU2Hy5curlSi/YafvCCeKaAb8bF9vUNGhr5usK4fc="
        ],
        "tx_merkel_tree_root_hash": "vobaBbri1K+lJv+rXYXRofWuQ0D+SDMGKaBwkTtXQQ0=",
        "state_merkel_tree_root_hash": "jYpTKUQBA7dJZYlzz7gEZWqX503EqGC+z+9pOKJsDKQ=",
        "validation_info": [
          {}
        ]
      }
    }
  },
  "signature": "MEYCIQDgtHEC+7gaOGsOQbFWOqeCQW5yOrwgDsZI+aZnGu+sogIhAPv+Emp+WxHjUl0ZoSw9mpF50udpTUyI+YqnRXnvn+UA"
}
```

### (4.2) Checking the non-existance of the deleted key `key1`

```shell
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"user_id":"alice","db_name":"db2","key":"key1"}'
```

```shell
MEUCIQCQWTeWyDwbj2ARFHQTH3pgsplOTrpW6XaC8mC/TODaJgIgIplpD5DB/SSNmhh42qLpITCJc66Z75mcHP3AWsS3zcw=
```

```shell
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEUCIQCQWTeWyDwbj2ARFHQTH3pgsplOTrpW6XaC8mC/TODaJgIgIplpD5DB/SSNmhh42qLpITCJc66Z75mcHP3AWsS3zcw=" \
     -X GET http://127.0.0.1:6001/data/db2/key1 | jq .
```

**Output**
```webmanifest
{
  "response": {
    "header": {
      "nodeID": "bdb-node-1"
    }
  }
  "signature": "MEYCIQCVdcbqmwIQDLkwYiK2tmOMwHI0GbWFX6kMMPo4VBORpgIhAMBBvsjCjL8LkogH06m58KefvnMOncLy7uFobh4XNNvI"
}
```

## (5) Creation, Updation, Deletion of states within a single transaction

We can also use `data_writes`, `data_deletes` with multiple entries along with many `data_reads` within a single transaction.

Let's create `key1` and `key2` so that in the next transaction we can do all three operations.

### (5.1) Create `key1` and `key2`

```shell
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/data/tx \
   --data '{
	"payload": {
		"must_sign_user_ids": [
            "alice"
        ],
		"tx_id": "1b6d6414-91ac-abcd-9723-1f31712add83",
        "db_operations": [
            {
                "db_name": "db2",
		        "data_writes": [
			        {
                        "key": "key1",
				        "value": "eyJuYW1lIjoiYWJjIiwiYWdlIjozMiwiZ3JhZHVhdGVkIjp0cnVlfQ==",
                        "acl": {
                            "read_users": {
                                "alice": true
                            },
                            "read_write_users": {
                                "alice": true
                            }
                        }
			        },
                    {
                        "key": "key2",
				        "value": "eyJkZWdyZWUiOiJQaC5EIiwieWVhciI6MjAxOH0=",
                        "acl": {
                            "read_users": {
                                "alice": true
                            },
                            "read_write_users": {
                                "alice": true,
                                "bob": true
                            }
                        }
			        }
                ]
            }
		]
	},
    "signatures": {
        "alice": "MEUCIQDOz5CZKIlK/t4M/rnxU6Hy0saDDCl2RprtnFIkttE+RgIgGwGF6P5rbDlSTh2DREXyl1etTeHf+TYeCLpOJ3WE2jU="
    }
}'
```

To create `key1` and `key2`, we have two entries in the `data_writes` section.

The signature is computed on the payload as follows:

```shell
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"must_sign_user_ids":["alice"],"tx_id":"1b6d6414-91ac-abcd-9723-1f31712add83","db_operations":[{"db_name":"db2","data_writes":[{"key":"key1","value":"eyJuYW1lIjoiYWJjIiwiYWdlIjozMiwiZ3JhZHVhdGVkIjp0cnVlfQ==","acl":{"read_users":{"alice":true},"read_write_users":{"alice":true}}},{"key":"key2","value":"eyJkZWdyZWUiOiJQaC5EIiwieWVhciI6MjAxOH0=","acl":{"read_users":{"alice":true},"read_write_users":{"alice":true,"bob":true}}}]}]}'
```

**Output**
```shell
MEUCIQDOz5CZKIlK/t4M/rnxU6Hy0saDDCl2RprtnFIkttE+RgIgGwGF6P5rbDlSTh2DREXyl1etTeHf+TYeCLpOJ3WE2jU=
```

### (5.2) Create `key3`, update `key2`, and delete `key1`

Now, we have required data in the server, we can execute creation, updation, and deletion within a single transaction.

```shell
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/data/tx \
   --data '{
    "payload": {
        "must_sign_user_ids": [
            "alice"
        ],
        "tx_id": "1b6d6414-91ac-efgh-9723-1f31712add83",
        "db_operations": [
            {
                "db_name": "db2",
                 "data_reads": [
                    {
                        "key": "key1",
                        "version": {
                            "block_num": 7
                        }
                    },
                    {
                        "key": "key2",
                        "version": {
                            "block_num": 7
                        }
                    }
                ],
                "data_writes": [
                    {
                        "key": "key3",
                        "value": "eyJuYW1lIjoiZGVmIiwiYWdlIjoxOSwiZ3JhZHVhdGVkIjpmYWxzZX0="
                    },
                    {
                        "key": "key2",
                        "value": "eyJkZWdyZWUiOiJQaC5EIiwieWVhciI6MjAxOX0=",
                        "acl": {
                            "read_users": {
                                "alice": true
                            },
                            "read_write_users": {
                                "alice": true,
                                "bob": true
                            }
                        }
                    }
                ],
                "data_deletes": [
                    {
                        "key": "key1"
                    }
                ]
            }
        ]
    },
    "signatures": {
        "alice": "MEQCIG4+RtISyo/3ZEkwKVg1gCz9GrcUpgkuMPESz6LDcudyAiBDbiF74lNxmX1T1xK+a6iUqiyd3Z5NjeFhO6j5YxW+kg=="
    }
}'
```

In the above transaction's payload, it can be seen that the payload contains `data_reads`, `data_writes`, and `data_deletes`.
The `data_reads` contains the version of the `key1` and `key2`. The `data_writes` contains the new key `key3` and updated `key2`.
Finally, the `data_deletes` holds the key `key1` to be deleted.

The signature on the payload is computed as shown below:

```shell
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"must_sign_user_ids":["alice"],"tx_id":"1b6d6414-91ac-efgh-9723-1f31712add83","db_operations":[{"db_name":"db2","data_reads":[{"key":"key1","version":{"block_num":7}},{"key":"key2","version":{"block_num":7}}],"data_writes":[{"key":"key3","value":"eyJuYW1lIjoiZGVmIiwiYWdlIjoxOSwiZ3JhZHVhdGVkIjpmYWxzZX0="},{"key":"key2","value":"eyJkZWdyZWUiOiJQaC5EIiwieWVhciI6MjAxOX0=","acl":{"read_users":{"alice":true},"read_write_users":{"alice":true,"bob":true}}}],"data_deletes":[{"key":"key1"}]}]}'
```

**Output**

```shell
MEQCIG4+RtISyo/3ZEkwKVg1gCz9GrcUpgkuMPESz6LDcudyAiBDbiF74lNxmX1T1xK+a6iUqiyd3Z5NjeFhO6j5YxW+kg==
```

### (5.3) Check the non-existence of `key1`

Let's ensure the deletion of `key1`.

First, we need to compute the signature to be set in the `Signature` header field. The signature is computed on the
data `{"user_id":"alice","db_name":"db2","key":"key1"}` as shown below by the user who submits the query, i.e., alice.

```shell
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"user_id":"alice","db_name":"db2","key":"key1"}'
```

**Output**

```shell
MEUCIQCQWTeWyDwbj2ARFHQTH3pgsplOTrpW6XaC8mC/TODaJgIgIplpD5DB/SSNmhh42qLpITCJc66Z75mcHP3AWsS3zcw=
```

Let's execute the following `cURL` command to fetch `key1`.

```shell
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEUCIQCQWTeWyDwbj2ARFHQTH3pgsplOTrpW6XaC8mC/TODaJgIgIplpD5DB/SSNmhh42qLpITCJc66Z75mcHP3AWsS3zcw=" \
     -X GET http://127.0.0.1:6001/data/db2/key1 | jq .
```

**Output**

```webmanifest
{
  "response": {
    "header": {
      "nodeID": "bdb-node-1"
    }
  }
  "signature": "MEYCIQCVdcbqmwIQDLkwYiK2tmOMwHI0GbWFX6kMMPo4VBORpgIhAMBBvsjCjL8LkogH06m58KefvnMOncLy7uFobh4XNNvI"
}
```

As `key1` does not exist, the result is empty.

### (5.4) Check the updated `key2`

Let's see whether `key2` has been updated from `year: 2018` to `year: 2019`.

First, compute the signature to be set in the `Signature` header field as shwon below:

```shell
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"user_id":"alice","db_name":"db2","key":"key2"}'
```
**Output**
```shell
MEUCIE4wF1nNbUCZJD2WbhwnJULRRYn9Yj+sD3Ev8zvlMRgkAiEA6vAC0RuzNJ2PiAiyhgtlzY+edbcX59ELZCi9ZfqFHCs=
```

Second, execute the following `cURL` command to fetch `key2`
```shell
curl \
    -H "Content-Type: application/json" \
    -H "UserID: alice" \
    -H "Signature: MEUCIE4wF1nNbUCZJD2WbhwnJULRRYn9Yj+sD3Ev8zvlMRgkAiEA6vAC0RuzNJ2PiAiyhgtlzY+edbcX59ELZCi9ZfqFHCs=" \
    -X GET http://127.0.0.1:6001/data/db2/key2 | read json; ./base64_decoder -getresponse=$json | jq .
```

**Output**

```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "value": "{\"degree\":\"Ph.D\",\"year\":2019}",
    "metadata": {
      "version": {
        "block_num": 8
      },
      "access_control": {
        "read_users": {
          "alice": true
        },
        "read_write_users": {
          "alice": true,
          "bob": true
        }
      }
    }
  },
  "signature": "MEUCIBIvtHxzpA2c/W96KK50VUke+Ggu5Qs0+Ccps1L1/TY9AiEA7kbYlS9+7iTU4xWF9KHMo/ZlVpFV+cWSErn0LaZ4rPk="
}
```

From the output, we can see that the `year` field in the value has been changed from `2018` to `2019`. After the update,
the version has been changed.

### (5.5) Check the newly created `key3`

Let's fetch `key3`.

First, compute the signature on the data as shown below:

```shell
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"user_id":"alice","db_name":"db2","key":"key3"}'
```

**Output**

```shell
MEQCIEyna9lmuJzyBjlvPQPxL4iRd5LyxpZWQDCqs146MYuOAiBL05QRAHOOSjIHzZhYiv7wm8Xfpviyu0EWdlC9SemHaw==
```

Second, execute the following `cURL` command to fetch `key3`.
```shell
curl \
    -H "Content-Type: application/json" \
    -H "UserID: alice" \
    -H "Signature: MEQCIEyna9lmuJzyBjlvPQPxL4iRd5LyxpZWQDCqs146MYuOAiBL05QRAHOOSjIHzZhYiv7wm8Xfpviyu0EWdlC9SemHaw==" \
    -X GET http://127.0.0.1:6001/data/db2/key3 | read json; cmd/base64_decoder/base64_decoder -getresponse=$json | jq .
```

**Output**

```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "value": "{\"name\":\"def\",\"age\":19,\"graduated\":false}",
    "metadata": {
      "version": {
        "block_num": 8
      }
    }
  },
  "signature": "MEUCIFwf4I8jZZwJelStDvj0hf+sLAAM4EGKN7C/vjC8sbxzAiEA05nIOMhrSPYW49XHn1ZTM6BuS6Y4xVhZRPbnlM6BotQ="
}
```

## (6) Operations on Multiple Databases in a Single Transaction

A data transaction can access or modify more than one user database in a transaction. In the below example,
we perform operations on two databases `db1` and `db2` within a single transaction.

### (6.1) Update `alice`'s privileges

As both `alice` and `bob` have only read permission on the database `db1`, first, we update the privilege of `alice`
to have read-write permission on `db1` as shown below:

```shell
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/user/tx \
   --data '{
    "payload": {
        "user_id": "admin",
        "tx_id": "1b6d6414-9b58-45d5-9723-1f31712add02",
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
                    "certificate": "MIIBsjCCAVigAwIBAgIRAJp7i/UhOnaawHTSdkzxR1QwCgYIKoZIzj0EAwIwHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTAeFw0yMTA2MTYxMTEzMjdaFw0yMjA2MTYxMTE4MjdaMCQxIjAgBgNVBAMTGUNhciByZWdpc3RyeSBDbGllbnQgYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASdCmAgHdqck7uhAK5siEF/O1EIUEIYtiR3XVEjbVhNe/6GXFShtsSThXYL9/XK6p4qF4oSy9j/PURMGnWbzSnso3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiEAsRZlR4sDyxS//BJnYpC684EWu1hO/JU8rkNW6Nn0FFQCIH/p6m6ELkLNQpx+1QJsWWtH/LdW94WinVylhuA4jggQ",
                    "privilege": {
                        "db_permission": {
                            "db1": 1,
                            "db2": 1
                        }
                    }
                }
            }
        ]
    },
    "signature": "MEUCIQCEpAcCyD85v6TNooJP554XDLjyum5p0RauP3D0t8rXTwIgYq1ghLfvrulznP0tS311UrfCWXh/jgchRElRPiYZ3Ug="
}'
```
In order to construct the above transaction payload, first, we need to fetch the `alice` user to capture the certificate and current
privileges. For brevity, the fetch operation is not shown here. Next, we need to construct the above payload. In the `user_reads` field,
we have the set the version which would be used during multi-version concurrency control. The `user_writes` field holds the updated
privileges where the read-only permission on `db1` denoted by `"db1":0` has been changed to `"db1":1`.

The signature on the payload is computed using the following command:
```shell
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key \
    -data='{"user_id":"admin","tx_id":"1b6d6414-9b58-45d5-9723-1f31712add02","user_reads":[{"user_id":"alice","version":{"block_num":3}}],"user_writes":[{"user":{"id":"alice","certificate":"MIIBsjCCAVigAwIBAgIRAJp7i/UhOnaawHTSdkzxR1QwCgYIKoZIzj0EAwIwHjEcMBoGA1UEAxMTQ2FyIHJlZ2lzdHJ5IFJvb3RDQTAeFw0yMTA2MTYxMTEzMjdaFw0yMjA2MTYxMTE4MjdaMCQxIjAgBgNVBAMTGUNhciByZWdpc3RyeSBDbGllbnQgYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASdCmAgHdqck7uhAK5siEF/O1EIUEIYtiR3XVEjbVhNe/6GXFShtsSThXYL9/XK6p4qF4oSy9j/PURMGnWbzSnso3EwbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAU7nVzp7gto++BPlj5KAF1IA62TNEwDwYDVR0RBAgwBocEfwAAATAKBggqhkjOPQQDAgNIADBFAiEAsRZlR4sDyxS//BJnYpC684EWu1hO/JU8rkNW6Nn0FFQCIH/p6m6ELkLNQpx+1QJsWWtH/LdW94WinVylhuA4jggQ","privilege":{"db_permission":{"db1":1,"db2":1}}}}]}'
```
**Output**
```shell
MEUCIQCEpAcCyD85v6TNooJP554XDLjyum5p0RauP3D0t8rXTwIgYq1ghLfvrulznP0tS311UrfCWXh/jgchRElRPiYZ3Ug=
```

Now that `alice` has read-write permission on both `db1` and `db2`, we can perform the multi-database operation.

### (6.2) Create `key1` in `db1` and `key1` in `db2`

Let's create
  1. the key `key1` with value `v` in `db1`
  2. the key `key2` with value `v` in `db2`

```shell
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/data/tx \
   --data '{
	"payload": {
		"must_sign_user_ids": [
            "alice"
        ],
    	"tx_id": "1b6d6414-91af-abcd-efgh-1f31712add83",
        "db_operations": [
            {
                "db_name": "db1",
                "data_reads": [
                    {
                        "key": "key1"
                    }
                ],
		        "data_writes": [
			        {
                        "key": "key1",
				        "value": "eyJuYW1lIjoiYWJjIiwiYWdlIjozMiwiZ3JhZHVhdGVkIjp0cnVlfQ==",
                        "acl": {
                            "read_users": {
                                "alice": true
                            }
                        }
			        }
                ]
            },
            {
                "db_name": "db2",
                "data_reads": [
                    {
                        "key": "key1"
                    }
                ],
		        "data_writes": [
                    {
                        "key": "key1",
				        "value": "eyJkZWdyZWUiOiJQaC5EIiwieWVhciI6MjAxOH0=",
                        "acl": {
                            "read_users": {
                                "alice": true
                            },
                            "read_write_users": {
                                "alice": true,
                                "bob": true
                            }
                        }
			        }
                ]
            }
		]
	},
    "signatures": {
        "alice": "MEQCIHYKWmSmsfrnyUKpKSH89K8BNAIO1/IW/uprJeHDu6mGAiBQTzVCiwm/t63pJtyUQ34AnOSk3apZFwFMz0GYXzNAaA=="
    }
}'
```
In order to perform operations on multiple databases, we can put one entry per database in the `db_operations` field.
In the above command, we have one entry per `db1` and `db2`. Both entries hold a `data_reads` and `data_writes`
fields with `key1` and `key2`, respectively.

The signature on the payload is computed using the following command:
```shell
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"must_sign_user_ids":["alice"],"tx_id":"1b6d6414-91af-abcd-efgh-1f31712add83","db_operations":[{"db_name":"db1","data_reads":[{"key":"key1"}],"data_writes":[{"key":"key1","value":"eyJuYW1lIjoiYWJjIiwiYWdlIjozMiwiZ3JhZHVhdGVkIjp0cnVlfQ==","acl":{"read_users":{"alice":true}}}]},{"db_name":"db2","data_reads":[{"key":"key1"}],"data_writes":[{"key":"key1","value":"eyJkZWdyZWUiOiJQaC5EIiwieWVhciI6MjAxOH0=","acl":{"read_users":{"alice":true},"read_write_users":{"alice":true,"bob":true}}}]}]}'
```
**Output**
```shell
MEQCIHYKWmSmsfrnyUKpKSH89K8BNAIO1/IW/uprJeHDu6mGAiBQTzVCiwm/t63pJtyUQ34AnOSk3apZFwFMz0GYXzNAaA==
```

### (6.3) Check the existence of `key1` in `db1`

Let's fetch `key1` from `db1` to ensure that the key has been created successfully.

First, the signature to be set in the `Signature` header field needs to be computed as shown below:
```shell
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key \
    -data='{"user_id":"alice","db_name":"db1","key":"key1"}'
```
**Output**
```shell
MEUCIQCujK1egalHITJ378hf60/RZaUFOkGzGkFaBWjO2fQilgIgX2htCyuGJsB8NHynIdH0T5SRjNMeWrzK6AwcSoFsiLY=
```

Next, execute the following command by setting the `UserID` and `Signature` header to fetch `key1` from `db1`.
```shell
 curl \
      -H "Content-Type: application/json" \
      -H "UserID: alice" \
      -H "Signature: MEUCIQCujK1egalHITJ378hf60/RZaUFOkGzGkFaBWjO2fQilgIgX2htCyuGJsB8NHynIdH0T5SRjNMeWrzK6AwcSoFsiLY=" \
      -X GET http://127.0.0.1:6001/data/db1/key1 | read json; ./base64_decoder -getresponse=$json | jq .
```
**Output**
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "value": "{\"name\":\"abc\",\"age\":32,\"graduated\":true}",
    "metadata": {
      "version": {
        "block_num": 11
      },
      "access_control": {
        "read_users": {
          "alice": true
        }
      }
    }
  },
  "signature": "MEUCIECyz3V8GyJvsupm5IXdPLcAOeCjQJimo+I4e8VlHyUJAiEAzl3gILPF++H6AXZTO3hlRBDVYCVd1zwIdsvC3McYTgU="
}
```

### (6.4) Check the existence of `key1` in `db2`

Let's fetch `key1` from `db1` to ensure that the key has been created successfully.

First, the signature to be set in the `Signature` header field needs to be computed as shown below:
```shell
./bin/signer -privatekey=deployment/sample/crypto/bob/bob.key \
    -data='{"user_id":"bob","db_name":"db2","key":"key1"}'
```

**Output**
```
MEUCIA2i15BxJAYcwl7jeGgrqqr6C2xc3hbBNKJwdC8mmNgvAiEA+AzQiMi6VbCKSn/N011jrNTmz4JiZ/u1+QLryYaDuBg=
```

Second, submit the query
```shell
 curl \
    -H "Content-Type: application/json" \
    -H "UserID: bob" \
    -H "Signature: MEUCIA2i15BxJAYcwl7jeGgrqqr6C2xc3hbBNKJwdC8mmNgvAiEA+AzQiMi6VbCKSn/N011jrNTmz4JiZ/u1+QLryYaDuBg=" \
    -X GET http://127.0.0.1:6001/data/db2/key1 | read json; ./base64_decoder -getresponse=$json | jq .
```

**Output**
```webmanifest
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "value": "{\"degree\":\"Ph.D\",\"year\":2018}",
    "metadata": {
      "version": {
        "block_num": 11
      },
      "access_control": {
        "read_users": {
          "alice": true
        },
        "read_write_users": {
          "alice": true,
          "bob": true
        }
      }
    }
  },
  "signature": "MEUCIQCxVaLsp0gHWqzsuat7XdUY4OehLy2tN89viOzIpTajDgIgawoH+dBtI3tidwROYHGtkZmDE3RCgVtG24s7aFyTX1Q="
}
```

## (7) Multi-Signatures Transaction
