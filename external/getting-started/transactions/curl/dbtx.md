---
id: dbtx
title: Database Administration Transaction
---

# Database Administration Transaction

To create or delete a database, we needs to submit a database administration transaction.

When the database node boots up for the first time, it would create a default database called `bdb` and
3 system databases named `_users`, `_dbs`, `_config`. The `bdb` database can be used to submit
data transactions whereas system databases are internal to the `bdb` server. The user cannot
directly read or write to the system databases.

To create or delete user databases, user needs to issue a `POST /db/tx {txPayload}` where `txPayload`
contains information about the database to be created and deleted.

In queries, we had to set the `UserID` and `Signature` headers. Whereas in the
transaction, we need to pass both the `UserID` and `Signature` as part of the `txPayload` itself.

Next, we will see example for
  1. [creation of new databases](#creation-of-databases)
  2. [deletion of existing databases](#deletion-of-databases)
  3. [creation and deletion of databases within a single transaction](#creation-and-deletion-of-databases-in-a-single-transaction)
  4. [invalid database administration transactions](#invalid-database-administration-transaction)

## Creation of Databases

We can create a new database to store data/states by issuing a database administration transaction.
Note that the database to be created should not exist in the node. Otherwise, the transaction would
fail.

The following curl command submits a database administration transaction to create two new databases
named `db1` and `db2`:

```json
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/db/tx \
   --data '{
    "payload": {
		"user_id": "admin",
		"tx_id": "1b6d6414-9b58-45d0-9723-1f31712add71",
		"create_dbs": [
			"db1",
			"db2"
		]
	},
  "signature": "MEUCIQDidxd5ScjpfYTIfVmSfC874zO0iosSyQUzRprs8j7VXgIgR7QxISwdjgXX58TktYXobJHwbCC3F/14rxCg0F8Ma1w="
}'
```

The `payload` of the database administration transaction must contain a `"user_id"` who submits the transaction, `"tx_id"` to
uniquely identify this transaction, and a list of dbs to be created in a `"create_dbs"` list as shown in the above cURL
command.

As all administrative transactions must be submitted only by the admin, the `"user_id"` is set to `"admin"`. As we are creating
two dbs named `db1` and `db2`, the `"create_dbs"` is set to `["db1","db2"]`. Finally, the signature field contains the admin's
signature on the payload and is computed using the following command:

```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","tx_id":"1b6d6414-9b58-45d0-9723-1f31712add71","create_dbs":["db1","db2"]}'
```
The output of the above command is set to the `signature` field in the data.

Once the db creation transaction gets validated and committed, it would return a receipt to the transaction submitter.
Note that only if the `TxTimeout` header is set, the submitting user would receive the transaction receipt. This is
because if the `TxTimeout` is not set, the transaction would be submitted asynchronously and the database node
returns as soon as it accepts the transaction into the queue. If the `TxTimeout` is set, the database node waits
for the specified time. If the transaction is committed by the specified time, the receipt would be returned.
The receipt for the above transaction would look something like the following:

```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "receipt": {
      "header": {
        "base_header": {
          "number": 2,
          "previous_base_header_hash": "YRMz96IjKSwQsNM9wPTGC13ueHxwPvjLCnpp/k/HnV4=",
          "last_committed_block_hash": "WgR2lRdea8rt6O8UBzSdhtu/LXcAFDAPyYRVGfasHeI=",
          "last_committed_block_num": 1
        },
        "skipchain_hashes": [
          "WgR2lRdea8rt6O8UBzSdhtu/LXcAFDAPyYRVGfasHeI="
        ],
        "tx_merkel_tree_root_hash": "CzIEbygWXNneRauTgFvxjSa5JvX1FWC3KN51jJDLxT0=",
        "state_merkel_tree_root_hash": "QPUvUPUpCL/P31VtV0CuRs5OWhDJZeh2psL1XHOyID8=",
        "validation_info": [
          {}
        ]
      }
    }
  },
  "signature": "MEYCIQDi91QtSpLRKfX2MiIT2KqH9OXZYrULPQZE13EpVfk4QIhAJr960MOF/TgrkX02hDus5z23G1I8DAQtApg2xGaza5Q"
}
```

Once the above transaction gets validated and committed, we can check the existance of `db1` and `db2`.

### Checking the existance of db1
Compute the digital signature on the request payload `'{"user_id":"admin","db_name":"db1"}'`
```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","db_name":"db1"}'
```
The above command outputs the digital signature which needs to be set in the `Signature` header
```
MEYCIQCeZXLrqrMYodbbgR7UjHR2yq42H2wbNHbj6KEDwW8a1QIhAIv1udmHjwSssKnJjS5iY1LDfez1/RDv9ZEue4TDfcJZ
```
Finally, we can submit the query.
```sh
curl \
      -H "Content-Type: application/json" \
      -H "UserID: admin" \
      -H "Signature: MEYCIQCeZXLrqrMYodbbgR7UjHR2yq42H2wbNHbj6KEDwW8a1QIhAIv1udmHjwSssKnJjS5iY1LDfez1/RDv9ZEue4TDfcJZ" \
      -X GET http://127.0.0.1:6001/db/db1 | jq .
```
**Output:**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "exist": true
  },
  "signature": "MEYCIQCtwYwdWo12alntzm1ZHkseOj5flLe8f8Hb8uGdQpjNwAIhAIHX7ddkikKAc+znEPCBE30iUemXpEC/Av8xdYQ5Rzxr"
}
```

### Checking the existance of db2
```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","db_name":"db1"}'
MEYCIQCcd9pucHSiyrP/wTIfSxer1M1qhyuYZ954WyuNO6NNuAIhALXfLg9NdwIDY2xDoLO9GxY5k/5hPqOz6i7fxvurd/v3
```

```sh
curl \
      -H "Content-Type: application/json" \
      -H "UserID: admin" \
      -H "Signature: MEYCIQCcd9pucHSiyrP/wTIfSxer1M1qhyuYZ954WyuNO6NNuAIhALXfLg9NdwIDY2xDoLO9GxY5k/5hPqOz6i7fxvurd/v3" \
      -X GET http://127.0.0.1:6001/db/db2 | jq .
```
**Output:**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "exist": true
  },
  "signature": "MEUCIQC1SHGOWpV53UJ39VGfrrm0sbRGE1NUi0yQtAcTggvhhQIgdzuw6vTkgC8i8v/RnnvYbmHJurCmdsjtNUikgvO4HQE="
}
```

## Deletion of Databases

We can delete an existing database by issuing a database administration transaction. Note that the database to be deleted should exist in the node.
Otherwise, the transaction would be marked invalid.

The following curl command can be used to delete two existing databases named `db1` and `db2`:
```json
curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/db/tx \
   --data '{
    "payload": {
		"user_id": "admin",
		"tx_id": "5c6d6414-3258-45d0-6923-2g31712add82",
		"delete_dbs": [
			"db1",
			"db2"
		]
	},
  "signature": "MEYCIQDC3t4gX4rAXmzqM8359u751vueqaSmYvBEXpCXdafeKAIhAKitFv8r89rRRuAlABhjcgeJPIPTEpkcc3tOZ77YmypV"
}'
```

The `payload` of the database administration transaction must contain a `"user_id"` who submits the transaction, `"tx_id"` to
uniquely identify this transaction, and a list of dbs to be deleted in a `"delete_dbs"` list as shown in the above `cURL`
command.

As all administrative transactions must be submitted only by the admin, the `"user_id"` is set to `"admin"`. As we are deleting
two existing dbs named `db1` and `db2`, the `"delete_dbs"` is set to `["db1","db2"]`. Finally, the signature field contains the admin's
signature on the payload and is computed using the following command:

```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","tx_id":"5c6d6414-3258-45d0-6923-2g31712add82","delete_dbs":["db1","db2"]}'
```
The output of the above command is set to the `signature` field in the data.

Once the db deletion transaction gets validated and committed, it would return a receipt to the transaction submitter.
Note that only if the `TxTimeout` header is set, the submitting user would receive the transaction receipt. This is
because if the `TxTimeout` is not set, the transaction would be submitted asynchronously and the database node
returns as soon as it accepts the transaction into the queue. If the `TxTimeout` is set, the database node waits
for the specified time. If the transaction is committed by the specified time, the receipt would be returned.
The receipt for the above transaction would look something like the following:

Once the above transaction gets validated and committed, we can check that the `db1` and `db2` do not exist anymore.

### Checking the existance of db1
```sh
curl \
      -H "Content-Type: application/json" \
      -H "UserID: admin" \
      -H "Signature: MEYCIQCeZXLrqrMYodbbgR7UjHR2yq42H2wbNHbj6KEDwW8a1QIhAIv1udmHjwSssKnJjS5iY1LDfez1/RDv9ZEue4TDfcJZ" \
      -X GET http://127.0.0.1:6001/db/db1 | jq .
```
**Output:**
```json
{
  "payload": {
    "header": {
      "nodeID": "bdb-node-1"
    },
  }
}
```
The default values are omitted and hence, the `exist = false` is not printed. 

### Checking the existance of db2

```sh
curl \
      -H "Content-Type: application/json" \
      -H "UserID: admin" \
      -H "Signature: MEYCIQCcd9pucHSiyrP/wTIfSxer1M1qhyuYZ954WyuNO6NNuAIhALXfLg9NdwIDY2xDoLO9GxY5k/5hPqOz6i7fxvurd/v3" \
      -X GET http://127.0.0.1:6001/db/db2 | jq .
```
**Output:**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    }
  },
  "signature": "MEYCIQCvzD85Rm/Xd1L6B6AvcZWlfhMeho0zj4WcBE66DY3wswIhALaugTByfBvY1O1BFjw7KuHDQUYOM4sDpsudd/6Hy7bt"
}
```

The default values are omitted and hence, the `exist = false` is not printed. 

## Creation and Deletion of Databases in a Single Transaction

Within a single transaction, we can create and delete as many numnber of database we want. Note that we can only delete dbs
if exists. Otherwise, the transaction would be invalidated. Hence, first create two databases using this [example](#creation-of-databases).
If this example was already executed on the database instance, change the `tx_id` used in that example and regenerate the
signature. Also, do not forgot to update the `tx_id` and `signature` set in the payload passed to `cURL`.

The following command submits a transaction that creates and deletes databases within a single transaction.
This transactions will be valid only if `db3` & `db4` do not exist and `db1` & `db2` exist in the cluster.
```json
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/db/tx \
   --data '{
    "payload": {
        "user_id": "admin",
        "tx_id": "1b6d6414-9b58-12d5-3733-1f31712add88",
        "create_dbs": [
            "db3",
            "db4"
        ],
        "delete_dbs": [
            "db1",
            "db2"
        ]
    },
  "signature": "MEUCIAjEtDZ2Q6n6cteisp94ggFXk3JUOXCjhfUlftc80gf6AiEA6IPtezn06SaPWQLfGhbx8BrFL4BI4iEIu/TDGtcaCKI="
}'
```
The signature is computed using the following command
```
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","tx_id":"1b6d6414-9b58-12d5-3733-1f31712add88","create_dbs":["db3","db4"],"delete_dbs":["db1","db2"]}'
```

## Invalid Database Administration Transaction

We cover the incorrect usage of administration transaction that can lead to invalidation of the submitted database administration transaction.

### Database to be created already exist

Let's create a new database `db5`
```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","tx_id":"1b6d6414-9b58-45d0-9723-1f31712add73","create_dbs":["db5"]}'
```
The above command outputs the digital signature on the transaction payload
```
MEUCIQCqYEdJOwf6JXAOCmAaub745uTEb2jyCFs10zZOhDIvUAIgN/ody6R9q3u5Q26Tabn3lPY1zz8NCUHCo6ymSu15jI4=
```
Include the above signature and submit the transaction to create the database `db5`
```json
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/db/tx \
   --data '{
    "payload": {
		"user_id": "admin",
		"tx_id": "1b6d6414-9b58-45d0-9723-1f31712add73",
		"create_dbs": [
			"db5"
		]
	},
  "signature": "MEUCIQCqYEdJOwf6JXAOCmAaub745uTEb2jyCFs10zZOhDIvUAIgN/ody6R9q3u5Q26Tabn3lPY1zz8NCUHCo6ymSu15jI4="
}'
```

Let's try to create `db5` again
```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","tx_id":"1b6d6414-9b58-45d0-9723-1f31712add74","create_dbs":["db5"]}'
MEQCIBySAJhI5DCshQ/KWagquxtV8S6gRSiqG/qYrcwxhmyTAiAZG8wpcQx33uTlItROQN5B5izTZhntxhqWRTfv8t84uw==
```
```json
 curl \
   -H "Content-Type: application/json" \
   -H "TxTimeout: 2s" \
   -X POST http://127.0.0.1:6001/db/tx \
   --data '{
    "payload": {
		"user_id": "admin",
		"tx_id": "1b6d6414-9b58-45d0-9723-1f31712add74",
		"create_dbs": [
			"db5"
		]
	},
  "signature": "MEQCIBySAJhI5DCshQ/KWagquxtV8S6gRSiqG/qYrcwxhmyTAiAZG8wpcQx33uTlItROQN5B5izTZhntxhqWRTfv8t84uw=="
}'
```
The above transaction would be invalidated with the following reason: `the database [db5] already exists in the cluster and hence, it cannot be created`
The exact output would be
```
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "receipt": {
      "header": {
        "base_header": {
          "number": 7,
          "previous_base_header_hash": "buuy+aLzGABHKoVS7XemlQFyynryMwtXHZ5Oq8SHewE=",
          "last_committed_block_hash": "4Km4QTQDIZ+u7GmFJOqG8HXgmWEMkedJRcczp6xweo4=",
          "last_committed_block_num": 6
        },
        "skipchain_hashes": [
          "4Km4QTQDIZ+u7GmFJOqG8HXgmWEMkedJRcczp6xweo4=",
          "JAS8SOSIZqBQMQs9PUkgrCjAF4I//lzjcYshgAtMvzs="
        ],
        "tx_merkel_tree_root_hash": "tgsHvDvjWbO8P/BIvtvRHuIBZUnptfpiHN9RyBSu9Lw=",
        "state_merkel_tree_root_hash": "qmolWEmx9D8BtWPRUEE0tz4/bvzhxLpZUJR1gA7AT4Q=",
        "validation_info": [
          {
            "flag": 5,
            "reason_if_invalid": "the database [db5] already exists in the cluster and hence, it cannot be created"
          }
        ]
      }
    }
  },
  "signature": "MEUCIFfdEaH9himxa0544ibi318+sVQ9BH3wst3a3dLngQklAiEA1FE42WXpQyqGx71stLp0NoRqhKi9LTQnTT6e3Erebvk="
}l
```
In the transaction receipt, we can see that the following:
```
        "validation_info": [
          {
            "flag": 5,
            "reason_if_invalid": "the database [db5] already exists in the cluster and hence, it cannot be created"
          }
        ]
```
### Database to be deleted does not exist

Let's try to delete `db6` which does not exist in the cluster
```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","tx_id":"1b6d6414-9b58-45d0-9723-1f31712add75","delete_dbs":["db6"]}'
```
```
MEUCIQCdUyJJEcBXqR1JPnIpaR6rVYXNSzFThhuLils1SWA2lAIgQ6KFClkJtrRRuhJqu3R7q9KUcQN2bBXrdvXJK3n9/Qk=
```
```json
curl \
     -H "Content-Type: application/json" \
     -H "TxTimeout: 2s" \
     -X POST http://127.0.0.1:6001/db/tx \
     --data '{
      "payload": {
            "user_id": "admin",
            "tx_id": "1b6d6414-9b58-45d0-9723-1f31712add75",
            "delete_dbs": [
                "db6"
            ]
      },
      "signature": "MEUCIQCdUyJJEcBXqR1JPnIpaR6rVYXNSzFThhuLils1SWA2lAIgQ6KFClkJtrRRuhJqu3R7q9KUcQN2bBXrdvXJK3n9/Qk="
  }'
```
The above transaction would be invalidated with the following reason: `the database [db6] does not exist in the cluster and hence, it cannot be deleted`.

The following would be the transaction receipt that holds the reason for the invalidation:
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "receipt": {
      "header": {
        "base_header": {
          "number": 8,
          "previous_base_header_hash": "lLU6v1B2shIuugSC8scHno989kzNrI9j+l3uw51ULt4=",
          "last_committed_block_hash": "XltNBYrr8XF6L8TSc9QcJmoGYchzqmbeCbpR/BjXTrE=",
          "last_committed_block_num": 7
        },
        "skipchain_hashes": [
          "XltNBYrr8XF6L8TSc9QcJmoGYchzqmbeCbpR/BjXTrE="
        ],
        "tx_merkel_tree_root_hash": "UIG89/PfrT79WGZJHZINST+qSHXGaVSt0CwcuL0V0kQ=",
        "state_merkel_tree_root_hash": "qmolWEmx9D8BtWPRUEE0tz4/bvzhxLpZUJR1gA7AT4Q=",
        "validation_info": [
          {
            "flag": 5,
            "reason_if_invalid": "the database [db6] does not exist in the cluster and hence, it cannot be deleted"
          }
        ]
      }
    }
  },
  "signature": "MEQCIEQJItcH5M3wptuNMdOsKmZTARMomqpgcvJigM46sxdUAiB8+3FMNwyU4x5NUdPIi5Prd2jL9KiNuDdNoHINksYB1Q=="
}
```

### Database to be deleted is a system database

Let's try to delete a system database `_config`

```sh
./bin/signer -privatekey=deployment/sample/crypto/admin/admin.key -data='{"user_id":"admin","tx_id":"1b6d6414-9b58-45d0-9723-1f31712add76","delete_dbs":["_config"]}'

MEUCIAiy5DIQvpPk8a1+e1Q5hZww+fm71PUx1kyBF8i5Vr70AiEA5i7Q33t5TbL66k4syAYmitj+fWnf4z6nvIjuS3ilJ9s=
```
```json
curl \
     -H "Content-Type: application/json" \
     -H "TxTimeout: 2s" \
     -X POST http://127.0.0.1:6001/db/tx \
     --data '{
      "payload": {
            "user_id": "admin",
            "tx_id": "1b6d6414-9b58-45d0-9723-1f31712add76",
            "delete_dbs": [
                "_config"
            ]
      },
      "signature": "MEUCIAiy5DIQvpPk8a1+e1Q5hZww+fm71PUx1kyBF8i5Vr70AiEA5i7Q33t5TbL66k4syAYmitj+fWnf4z6nvIjuS3ilJ9s="
  }'
```
The above transaction would be invalidated with the following reason: `the database [_config] is a system database which cannot be deleted`.
The following is the transaction receipt:
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "receipt": {
      "header": {
        "base_header": {
          "number": 9,
          "previous_base_header_hash": "mE6Gr5PTXzJbXsUATl/Fv+Xkg81Cbrtw8MfUJreb9og=",
          "last_committed_block_hash": "4QleYwwhc+DCYeYaIdrdTpMeOgUa5O8F40vuVuZFsEs=",
          "last_committed_block_num": 8
        },
        "skipchain_hashes": [
          "4QleYwwhc+DCYeYaIdrdTpMeOgUa5O8F40vuVuZFsEs=",
          "XltNBYrr8XF6L8TSc9QcJmoGYchzqmbeCbpR/BjXTrE=",
          "JAS8SOSIZqBQMQs9PUkgrCjAF4I//lzjcYshgAtMvzs=",
          "fCjyJMJc/xLMdW7uRH22w7ps2QbGeLiLIfIJPMntitI="
        ],
        "tx_merkel_tree_root_hash": "xEgj6T0djExEdGvWxwCrgD9nXj26VTyQYgYGTEcLscs=",
        "state_merkel_tree_root_hash": "qmolWEmx9D8BtWPRUEE0tz4/bvzhxLpZUJR1gA7AT4Q=",
        "validation_info": [
          {
            "flag": 5,
            "reason_if_invalid": "the database [_config] is a system database which cannot be deleted"
          }
        ]
      }
    }
  },
  "signature": "MEMCHztXBWGjF0X+CJQ01QZyyKftqK7h7kW2VhuTDHx6UoECIGdjkHMkflSGywWnVBAM9zKVhllWS0ApJkvyaYUDFOHQ"
}
```
