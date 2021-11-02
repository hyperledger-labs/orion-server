---
id: datatx
title: Data Transaction
---
# Data Transaction

We can store, update, delete any state, i.e., key-value pair, on the ledger by issuing a data transaction.
By submitting a `POST /data/tx {txPaylod}`, we can perform a data transaction where `txPayload` contains reads, writes, and deletes of states.

> As a prerequisite, we need to create users `alice` and `bob`. Refer [here](./usertx.md) for examples on creating users. Further, the node
should have a user database named `db2`. Refer [here](./dbtx.md) for examples on creating databases.

## Storing a new state

Let's store a new state with the key `key1`.

```json
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
                        "value": "eXl5",
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
        "alice": "MEQCIAM/FYzdfVlQGWBPcyDMp2BRDyzQdTdusOl0M/UBCk2gAiAxns+4m30Y/HzlO0e0dK0HnaWhbxch5tUys0P0ME7ZPw=="
    }
}'
```
The payload contains `must_sign_user_ids` which is a list of user ids who must sign the transaction's payload. The
`db_operations` hold the `data_writes` to be applied on the specified `db_name`. The `value` in `data_writes` must
be encoded in base64. The `acl` contains list of users in the `read_users` who can read-only the state and a list of
users in the `read_write_users` who can both read and write to the state. Here, the `signatures` holds a map of
each user in the `must_sign_user_ids` to their digital signature.

The signature is computed using the `alice` private key as follows:
```
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key -data='{"must_sign_user_ids":["alice"],"tx_id":"1b6d6414-9b58-45d0-9723-1f31712add81","db_operations":[{"db_name":"db2","data_writes":[{"key":"key1","value":"eXl5","acl":{"read_users":{"alice":true,"bob":true},"read_write_users":{"alice":true}}}]}]}'
```
**Output**
```
MEQCIAM/FYzdfVlQGWBPcyDMp2BRDyzQdTdusOl0M/UBCk2gAiAxns+4m30Y/HzlO0e0dK0HnaWhbxch5tUys0P0ME7ZPw==
```

Once the above transaction gets committed, the submitter of the transaction would receive the following transaction receipt
```json
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

### Checking the existance of the state

Let's query the node to see whether `key1` exists. The query can be submitted by either `alice` or `bob` as both have
the read permission to this key. No one else can read `key1` including the admin user of the node. In this example,
we use `bob` to query the key.

First, compute the bob signature on the request payload.
```sh
./bin/signer -privatekey=deployment/sample/crypto/bob/bob.key -data='{"user_id":"bob","db_name":"db2","key":"key1"}'
```
**Output**
```
MEUCIQDm6dLmAdd0X49JygTiUkh+brZxprWSr2+hcAH+QIu3AAIgF+m7kO33YXyyqSbnXS9HR79wt/aL3JGhKvXFQaFBJms=
```
Second, submit the query
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: bob" \
     -H "Signature: MEUCIQDm6dLmAdd0X49JygTiUkh+brZxprWSr2+hcAH+QIu3AAIgF+m7kO33YXyyqSbnXS9HR79wt/aL3JGhKvXFQaFBJms=" \
     -X GET http://127.0.0.1:6001/data/db2/key1 | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "value": "eXl5",
    "metadata": {
      "version": {
        "block_num": 5
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
  "signature": "MEUCID1+8HiN9nkv8990SytuQRl8BhBV2xUEe5InsPB5D7IwAiEAx8qENDc9BQTO3arlOxPWf9lh7OP8xXFoDS+jipnAA2Y="
}
```
The result contains the `value` associated with the key and also the `access_control` and `version` as part of the
`metadata`.

## Updating an existing state

Let's update the value of `key1`. In order to do that, we need to execute the following three steps:

1. Read the `key1` from the cluster.
2. Construct the updated value and transaction payload including the read version.
3. Submit the data transaction

> Note that the `data_reads` should contain the version of the `key1` that was read before modifying the value of `key1`. If `data_reads` is kept empty, the `data_writes` would be considered as blind write.

```json
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
                            "block_num": 5
                        }
                    }
                ],
		        "data_writes": [
			        {
                        "key": "key1",
				        "value": "aXDUvZio",
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
        "alice": "MEYCIQDFQpAI97qgNGrN/6lWM5v0Zn+ht3+4V5Mr57TIWDZFhAIhAKbatUhwr/lasFAkTydKSrDr+trEJM3KEnRWlz2kYcTV"
    }
}'
```
The `data_reads` holds the read key, `key1` and its version `"block_num":5`. Please fetch the `key1` to get the version
number in your setup. We have updated the `value` of the `key1` and removed the user `bob` from the reader's list.
The required signature is computed with alice private key using the following command
```sh
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key -data='{"must_sign_user_ids":["alice"],"tx_id":"1b6d6414-9b58-45d0-9723-1f31712add83","db_operations":[{"db_name":"db2","data_reads":[{"key":"key1","version":{"block_num":5}}],"data_writes":[{"key":"key1","value":"aXDUvZio","acl":{"read_users":{"alice":true},"read_write_users":{"alice":true}}}]}]}'
 ```
**Output**
 ```
MEYCIQDFQpAI97qgNGrN/6lWM5v0Zn+ht3+4V5Mr57TIWDZFhAIhAKbatUhwr/lasFAkTydKSrDr+trEJM3KEnRWlz2kYcTV
```

Once the above transaction gets committed, the submitter of the transaction would get the following transaction receipt
```
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

### Checking the existance of the updated key

Let's query the node to see whether `key1` has been updated. The query can be submitted only by `alice` as `bob` is
no longer in the reader's list of the `key1`. No one else can read `key1` including the admin user of the node.
In this example, we use `alice` to query the key.

First, compute the alice signature on the request payload.
```sh
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key -data='{"user_id":"alice","db_name":"db2","key":"key1"}'
```
**Output**
```
MEYCIQDUVfuDVppJ2BLuiD662M4iuBYdn2E2ttyspZo6UXYmNgIhAOEvZTdRe7d9/bkVliplmpFHeypKbz7wMHPluGYipqvw
```
Second, submit the query
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEYCIQDUVfuDVppJ2BLuiD662M4iuBYdn2E2ttyspZo6UXYmNgIhAOEvZTdRe7d9/bkVliplmpFHeypKbz7wMHPluGYipqvw" \
     -X GET http://127.0.0.1:6001/data/db2/key1 | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "value": "aXDUvZio",
    "metadata": {
      "version": {
        "block_num": 6
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
  "signature": "MEYCIQDbtOQex0Ea8ZrFEcfHGS/swsPo8uSHVspB9MujnZbkFwIhANwrtGYyuIgJvXt4cI9pM2qQ/xWSQZz8oOjHGcHx3SCP"
}
```
The result contains the `value` associated with the key and also the `access_control` and `version` as part of the
`metadata`. In the update state, we can find that the `value`, `version`, and `access_control` have changed.

## Deleting an existing state

Let's delete the key `key1`. In order to do that, we need to execute the following three steps:

1. Read the `key1` from the cluster. If we want to blidly delete `key1`, this step can be avoided and also, the read version is not needed.
2. Construct the transaction payload including the read version.
3. Submit the data transaction

> Note that the `data_reads` should contain the version of the `key1` that was read before modifying the value of `key1`. If `data_reads` is left out, the delete would be applied blindly.

```json
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
                            "block_num": 6
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
        "alice": "MEQCIATEMJZ2HYkQtG+ivADylvJRzaksTum3/jN0zeg96+CuAiAEYKugmTbPbHXsjKnAWOLirNqI0WWOPcLN9jIlVaeseQ=="
    }
}'
```
The transaction should be submitted by `alice` as only she has the write permission which is required for the delete operation too.
The `db_operations` holds the `data_reads` with `key1` and version `"block_num":5`. The `data_deletes` list contains the key `key1`.
The signature is computed using the following command
```sh
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key -data='{"must_sign_user_ids":["alice"],"tx_id":"1b6d6414-9b58-45d0-9723-1f31712add85","db_operations":[{"db_name":"db2","data_reads":[{"key":"key1","version":{"block_num":6}}],"data_deletes":[{"key":"key1"}]}]}'
```
**Output**
```
MEQCIATEMJZ2HYkQtG+ivADylvJRzaksTum3/jN0zeg96+CuAiAEYKugmTbPbHXsjKnAWOLirNqI0WWOPcLN9jIlVaeseQ==
```
Once the above transaction gets committed, the submitting user would get the following transaction receipt
```json
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

### Checking the non-existance of the deleted key

```sh
./bin/signer -privatekey=deployment/sample/crypto/alice/alice.key -data='{"user_id":"alice","db_name":"db2","key":"key1"}'
```
```
MEUCIQCQWTeWyDwbj2ARFHQTH3pgsplOTrpW6XaC8mC/TODaJgIgIplpD5DB/SSNmhh42qLpITCJc66Z75mcHP3AWsS3zcw=
```
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEUCIQCQWTeWyDwbj2ARFHQTH3pgsplOTrpW6XaC8mC/TODaJgIgIplpD5DB/SSNmhh42qLpITCJc66Z75mcHP3AWsS3zcw=" \
     -X GET http://127.0.0.1:6001/data/db2/key1 | jq .
```
**Output**
```json
{
  "response": {
    "header": {
      "nodeID": "bdb-node-1"
    }
  }
  "signature": "MEYCIQCVdcbqmwIQDLkwYiK2tmOMwHI0GbWFX6kMMPo4VBORpgIhAMBBvsjCjL8LkogH06m58KefvnMOncLy7uFobh4XNNvI"
}
```
## Storing, Updating, Deleting states within a single transaction

We can also use `data_writes`, `data_deletes` with multiple entries along with many `data_reads` within a single transaction.

## Invalid Data Transaction

TODO (subsequent PR)
