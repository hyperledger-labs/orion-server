# Data Transaction
We can store/update/delete any state on the ledger by issuing a data transaction. By submitting a `POST /data {txPaylod}`, we can perform a data transaction where `txPayload` contains reads, writes, and deletes of states.

> Before executing the commands in the document, we need to create a user `user1` with read-write privilege to the database `bdb`. Refer [here](usertx.md) to add a new user `user1` with a read-write database privilege. We also need to create `user2` but no privileges are required.

## Storing a new state

Let's store a new state with the key `key1`. 

```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/data \
   --data '{
	"payload": {
		"userID": "user1",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add80",
		"DB_name": "bdb",
		"data_writes": [
			{
				"key": "key1",
				"value": "dmFsdWUy",
                "ACL": {
					"read_users": {
						"user2": true
					},
					"read_write_users": {
						"user1": true
					}
				}
			}
		]
	},
    "signature": "aGVsbG8="
}'
```

Once the above transaction gets committed, we can query `key1` as follows:

```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: user1" \
     -H "Signature: abcd" \
     -X GET http://127.0.0.1:6001/data/bdb/key1 | jq .
```
**Output**
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
    "value": "dmFsdWUy",
    "metadata": {
      "version": {
        "block_num": 28
      },
      "access_control": {
        "read_users": {
          "user2": true
        },
        "read_write_users": {
          "user1": true
        }
      }
    }
  }
}
```

## Updating an existing state

Let's update the value of `key1`. In order to do that, we need to execute the following three steps:

1. Read the `key1` from the cluster.
2. Construct the updated value and transaction payload including the read version.
3. Submit the data transaction 

> Note that the `data_reads` should contain the version of the `key1` that was read before modifying the value of `key1`.

```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/data \
   --data '{
	"payload": {
		"userID": "user1",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add81",
		"DB_name": "bdb",
        "data_reads": [
			{
				"key": "key1",
				"version": {
					"block_num": 29,
					"tx_num": 0
				}
			}
        ],
		"data_writes": [
			{
				"key": "key1",
				"value": "aXDUvZio",
                "ACL": {
					"read_users": {
						"user2": true
					},
					"read_write_users": {
						"user1": true
					}
				}
			}
		]
	},
    "signature": "aGVsbG8="
}'
```
Once the above transaction gets committed, we can query `key1` as follows:
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: user1" \
     -H "Signature: abcd" \
     -X GET http://127.0.0.1:6001/data/bdb/key1 | jq .
```
**Output**
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
    "value": "aXDUvZio",
    "metadata": {
      "version": {
        "block_num": 29
      },
      "access_control": {
        "read_users": {
          "user2": true
        },
        "read_write_users": {
          "user1": true
        }
      }
    }
  }
}
```
In the update state, we can find that both the value and version have been changed.

## Deleting an existing state

Let's delete the key `key1`. In order to do that, we need to execute the following three steps:

1. Read the `key1` from the cluster.
2. Construct the transaction payload including the read version.
3. Submit the data transaction 

> Note that the `data_reads` should contain the version of the `key1` that was read before modifying the value of `key1`.

```json
 curl \
   -H "Content-Type: application/json" \
   -X POST http://127.0.0.1:6001/data \
   --data '{
	"payload": {
		"userID": "user1",
		"txID": "1b6d6414-9b58-45d0-9723-1f31712add81",
		"DB_name": "bdb",
        "data_reads": [
			{
				"key": "key1",
				"version": {
					"block_num": 29,
					"tx_num": 0
				}
			}
        ],
        "data_deletes": [
            {
                "key": "key1"
            }
        ]
	},
    "signature": "aGVsbG8="
}'
```
Once the above transaction gets committed, we can query `key1` as follows but we would not find any associated value:
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: user1" \
     -H "Signature: abcd" \
     -X GET http://127.0.0.1:6001/data/bdb/key1 | jq .
```
**Output**
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    }
  }
}
```
## Storing, Updating, Deleting states within a single transaction

We can also use `data_writes`, `data_deletes` with multiple entries along with many `data_reads` within a single transaction. 

## Invalid Data Transaction

TODO (subsequent PR)