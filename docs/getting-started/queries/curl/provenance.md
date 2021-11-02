---
id: provenance
title: Provenance Queries on Historical Data
---
# Provenance queries
The provenance API gives the user access to the following BCDB data:
- The history of values for a given key, in different views and directions,
- Information about which users accessed or modified a specific piece of data,
- Information, including history, about the data items accessed by a given user,
- A history of user transactions.

Usually, provenance queries are used to investigate changes of some values over the time. For example, by sending `GET /provenance/data/history/{dbname}/{key}` we can follow changes of `key` over time.
As mentioned above, BCDB supports multiple types of provenance queries and here is a base list of them:

Query to get changes of values for a given key over time, supports multiple options, like directions, etc. Examples of this query, including input and output format, can be found [here], [here] and [here].
```http request
GET /provenance/data/history/{dbname}/{key}
```
Two queries that provide information about users who accessed or modified a specific piece of data. The example of data readers query is [here] and the example of data writers query [here].
```http request
GET /provenance/data/readers/{dbname}/{key}
```
```http request
GET /provenance/data/writers/{dbname}/{key}
```
Three queries for data items accessed by a given user. Examples are [here] and [here].
```http request
GET /provenance/data/read/{userId}
```
```http request
GET /provenance/data/written/{userId}
```
```http request
GET /provenance/data/deleted/{userId}
```
Query for all transactions submitted by given user. The example, the input and output format, etc, can be found [here].
```http request
GET /provenance/data/tx/{userId}
```

## Prepare data

To make this example more realistic and to see a meaningful outputs from these queries' execution, multiple transactions should be submitted to BDCD, and the ledger should contain multiple blocks. Next, we will submit multiple data transactions to BCDB.

> As a prerequisite, we need users `alice` and `bob` and database `db2` to exist in BCDB, and at least one data transaction submitted. 
> First, we need to create `db2` database, as described [here].
> Second, we need to create `alice` and `bob`. See [here] for user creation.
> In addition, some data should exist in `db2`, refer [here] for example of running data transaction.
### First data tx, only writes
First transaction contains writes to multiple keys: `key1`, `key2` and `key3`, and we send it using `/data/tx` POST request. 

> Please note, that the explanation about data transaction structure and its result is out of the scope of this document, for more information [see].
**Sign marshalled transaction content**
```sh
bin/signer -data '{"must_sign_user_ids":["alice"],"tx_id":"Tx000","db_operations":[{"db_name":"db2","data_writes":[{"key":"key1","value":"dGhpcyBpcyBhIGZpcnN0IHZhbHVl","acl":{"read_users":{"alice":true},"read_write_users":{"alice":true}}},{"key":"key2","value":"dGhpcyBpcyBhIHNlY29uZCB2YWx1ZQ==","acl":{"read_users":{"alice":true},"read_write_users":{"alice":true}}},{"key":"key3","value":"dGhpcyBpcyBhIHRoaXJkIHZhbHVl","acl":{"read_users":{"alice":true},"read_write_users":{"alice":true}}}]}]}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Output**
```
MEQCIBRWwsZHShtRGQkKlD81oicATj7+R6LDgG6RUFNGnUhVAiALVm6AdOE3+jC0LtyuPrYPDa7OAb3qDryv1nRNbig+rg==
```

**Submit transaction**
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
    "tx_id": "Tx000",
    "db_operations": [
      {
        "db_name": "db2",
        "data_writes": [
          {
            "key": "key1",
            "value": "dGhpcyBpcyBhIGZpcnN0IHZhbHVl",
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
            "value": "dGhpcyBpcyBhIHNlY29uZCB2YWx1ZQ==",
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
            "key": "key3",
            "value": "dGhpcyBpcyBhIHRoaXJkIHZhbHVl",
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
    "alice": "MEQCIBRWwsZHShtRGQkKlD81oicATj7+R6LDgG6RUFNGnUhVAiALVm6AdOE3+jC0LtyuPrYPDa7OAb3qDryv1nRNbig+rg=="
  }
}' | jq .
```

**Output**
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
          "previous_base_header_hash": "NqyVWNBl/XmWLM7PkK8NbI0qrwFmYvGHSTc03vj/zus=",
          "last_committed_block_hash": "nDNWHZPrlG3JVq6eLcuHPaS1iEZkBkemV7IleIVx6Jc=",
          "last_committed_block_num": 4
        },
        "skipchain_hashes": [
          "nDNWHZPrlG3JVq6eLcuHPaS1iEZkBkemV7IleIVx6Jc=",
          "wZmtCr8rJp/NGsEDjySSfHhi7Omr2Yw/d8rUaetrzLE=",
          "tl3PgPL/E52yhCWG1vLGk/bJXRqhw3rDxSXZzvMcuWo="
        ],
        "tx_merkel_tree_root_hash": "UXXqKu/I2Vr0Ma5cV9Hfun4Xo5285ZwdV9jcKspTnJo=",
        "state_merkel_tree_root_hash": "ksPp+NOrYLi909AYgrmrmGN1DKuez8ItpRJeLFpWy9g=",
        "validation_info": [
          {}
        ]
      }
    }
  },
  "signature": "MEYCIQDiau+kEkY6bpmtW7t5iN2F3+bwBzQ41AlNvGML9Z8eGAIhAKKdqa4xeuMYsHwUmMhwzSSMwPyTwwv6L1TRUgD6zBAF"
}
```

#### Second data tx, with both data reads and writes
Second transaction contains reads for multiple keys: `key1` and `key2`, writes to multiple keys: `key1` and `key2` and delete for `key3`.
**Sign marshalled transaction content**
```sh
bin/signer -data '{"must_sign_user_ids":["alice"],"tx_id":"Tx001","db_operations":[{"db_name":"db2","data_reads":[{"key":"key1","version":{"block_num":5}},{"key":"key2","version":{"block_num":5}}],"data_writes":[{"key":"key2","value":"dGhpcyBpcyBhIHNlY29uZCB2YWx1ZSB1cGRhdGVk","acl":{"read_users":{"alice":true},"read_write_users":{"alice":true}}},{"key":"key1","value":"dGhpcyBpcyBhIGZpcnN0IHZhbHVlIHVwZGF0ZWQ=","acl":{"read_users":{"alice":true},"read_write_users":{"alice":true}}}],"data_deletes":[{"key":"key3"}]}]}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Output**
```
MEUCIQC8Zd3NihB/TveLmhk/BAlvuIjSo76fnsywvyyh3JudOgIgbujXo5cMSyBa5i/I9vM3h0HWb/3Waop56hPSRB4Jw5k=
```

**Submit transaction**
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
    "tx_id": "Tx001",
    "db_operations": [
      {
        "db_name": "db2",
        "data_reads": [
          {
            "key": "key1",
            "version": {
              "block_num": 5
            }
          },
          {
            "key": "key2",
            "version": {
              "block_num": 5
            }
          }
        ],
        "data_writes": [
          {
            "key": "key2",
            "value": "dGhpcyBpcyBhIHNlY29uZCB2YWx1ZSB1cGRhdGVk",
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
            "key": "key1",
            "value": "dGhpcyBpcyBhIGZpcnN0IHZhbHVlIHVwZGF0ZWQ=",
            "acl": {
              "read_users": {
                "alice": true
              },
              "read_write_users": {
                "alice": true
              }
            }
          }
        ],
        "data_deletes": [
          {
            "key": "key3"
          }
        ]
      }
    ]
  },
  "signatures": {
    "alice": "MEUCIQC8Zd3NihB/TveLmhk/BAlvuIjSo76fnsywvyyh3JudOgIgbujXo5cMSyBa5i/I9vM3h0HWb/3Waop56hPSRB4Jw5k="
  }
}' | jq . 
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "receipt": {
      "header": {
        "base_header": {
          "number": 6,
          "previous_base_header_hash": "TJYT7zr95D3ghtU/M+j8OStU6r8Y+XRC5xfb90jshbI=",
          "last_committed_block_hash": "r57EtiB2XO1XlVAxBwFtn7cHP7YsfUBFi69y0UY0tw0=",
          "last_committed_block_num": 5
        },
        "skipchain_hashes": [
          "r57EtiB2XO1XlVAxBwFtn7cHP7YsfUBFi69y0UY0tw0="
        ],
        "tx_merkel_tree_root_hash": "dSc42JVp36bfORFO8QehzQFypEtBQfUzluT6ixhjeo4=",
        "state_merkel_tree_root_hash": "YRsFw5PcG1XzYO5o5nJa3lVfknijRqu6cCaDF1zpPA8=",
        "validation_info": [
          {}
        ]
      }
    }
  },
  "signature": "MEUCIHGNm7923sl7r3o/UBPi6IA4LhmV7vjISxB2aRTIJI8uAiEA4EKMhVJIwKMv1Nzy7zDaftEkal2wFl3pQN9heKw6tCY="
}
```

At the end of the preparation step, the ledger contains 6 blocks and last three blocks contain data transactions.
Let's summarize here state of the database and the ledger.

**Block 4**
- key: "key1", version: {block_num: 4, tx_num: 0}, value: "eXl5"

**Block 5**
- key: "key1", version: {block_num: 5, tx_num: 0}, value: "dGhpcyBpcyBhIGZpcnN0IHZhbHVl"
- key: "key2", version: {block_num: 5, tx_num: 0}, value: "dGhpcyBpcyBhIHNlY29uZCB2YWx1ZQ=="
- key: "key3", version: {block_num: 5, tx_num: 0}, value: "dGhpcyBpcyBhIHRoaXJkIHZhbHVl"

**Block 6**
- key: "key1", version: {block_num: 6, tx_num: 0}, value: "dGhpcyBpcyBhIGZpcnN0IHZhbHVlIHVwZGF0ZWQ="
- key: "key2", version: {block_num: 6, tx_num: 0}, value: "dGhpcyBpcyBhIHNlY29uZCB2YWx1ZSB1cGRhdGVk"
- key: "key3", version: {block_num: 6, tx_num: 0}, value: deleted



## History query
As first provenance query example, we will query for the full history of the changes in value of `key1` in `bd2`. To do this, we use `/provenance/data/history/{dbname}/{key}` GET query, and user `alice` will submit it.

First step to submit user should sign query parameters, represented as json object. 

**Sign query**
```sh
bin/signer -data '{"user_id":"alice","db_name":"db2","key":"key1"}' -privatekey=deployment/sample/crypto/alice/alice.key
```

Spaces, new lines and fields order in json are important to make it possible for server to validate users signature. The string we sign on is result of json serialization of protobuf history query object.
```protobuf
message GetHistoricalDataQuery {
  string user_id = 1;
  string db_name = 2;
  string key = 3;
  Version version = 4;
  string direction = 5;
  bool only_deletes = 6;
  bool most_recent = 7;
}
```
Because in this type of query `version`, `direction`, `only_deletes` and `most_recent` fields are not set, a json serialization ignores them, instead creating fields that contains empty values. We will see it multiple times later, while `tx_num` field equals to zero of protobuf `Version` object. Usually, block contains more than one transaction and for all transactions, except first one, `tx_num` will appear in json. 

**Signature**
```
MEUCIHTGSy8lJFlfRxJXEGq2gTi9czP81jwQ7vF2KdRxiigRAiEAjjCn9WSQPx6H99+EGYHyDQTNAr4O+1uhvY5eYI0ZT20=
```
User signature should be copied to query `Signature` header.

**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEUCIHTGSy8lJFlfRxJXEGq2gTi9czP81jwQ7vF2KdRxiigRAiEAjjCn9WSQPx6H99+EGYHyDQTNAr4O+1uhvY5eYI0ZT20=" \
     -X GET http://127.0.0.1:6001/provenance/data/history/db2/key1 | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "values": [
      {
        "value": "eXl5",
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
      {
        "value": "dGhpcyBpcyBhIGZpcnN0IHZhbHVl",
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
      {
        "value": "dGhpcyBpcyBhIGZpcnN0IHZhbHVlIHVwZGF0ZWQ=",
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
      }
    ]
  },
  "signature": "MEYCIQDoFCEmCAirIJHGcIi3/gz6Bd2a+iJdFvzIVRhXYg9RswIhANpSU319PofKI98lydpsjvz5IpIfvUJhKto8ISsrWfeR"
}
```

As we can see here, `key1` changed his value three times, in blocks 4, 5, 6.

> Please note that `version` json object in result contains only `block_num`, but no `tx_num`. It is because `tx_num` in our example is equals to zero and thus omitted as empty field during serializing a protobuf object to json.
Now lets query `key3` history, using same GET query, but will replace `key1` with `key3` as parameter.

**Sign query**
```sh
bin/signer -data '{"user_id":"alice","db_name":"db2","key":"key3"}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Signature**
```
MEYCIQDwogJW4wuIBGO5MYVLt1F0nFq+6BsULpKpmCcyGrcx/QIhAMj1uhj6LLW14pF6zZGToP5rdd0tgx9fvwUDHLXLbDO+
```
Don't forget to copy the signature to a query header.

**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEYCIQDwogJW4wuIBGO5MYVLt1F0nFq+6BsULpKpmCcyGrcx/QIhAMj1uhj6LLW14pF6zZGToP5rdd0tgx9fvwUDHLXLbDO+" \
     -X GET http://127.0.0.1:6001/provenance/data/history/db2/key3 | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "values": [
      {
        "value": "dGhpcyBpcyBhIHRoaXJkIHZhbHVl",
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
      }
    ]
  },
  "signature": "MEQCIFE2JIa9MkaGtBEZUqx8MIsYsSSiJS0Atz+TkI6Yl73HAiBRU98GTF+FlQoVXvSuqZArwd5UxjK5aMXBJLEdRCQKTA=="
}
```

Key `key3` changed only once, in block 5.

> Please note: one more time `version` json object contains only `block_num` field, without `tx_num`.
### History query with _only_deletes_ option
As you can see, the last query returned a value for `<db2, key3>`. Now we want to query if key was deleted at some time, and its last value before delete. We can run the same query, but with query parameter `only_deletes` turned `true`.

**Sign json query data**
```sh
bin/signer -data '{"user_id":"alice","db_name":"db2","key":"key3","only_deletes":true}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Output**
```
MEUCIQCPTVwKrosS6+2WnOUJ4qBU8Ru7ubJYkTOljCXOkFPAtgIgcFftqCm7bOZLxLkfzWloIznqpOkoTegyYtX+xgfHd6s=
```

Submit query
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEUCIQCPTVwKrosS6+2WnOUJ4qBU8Ru7ubJYkTOljCXOkFPAtgIgcFftqCm7bOZLxLkfzWloIznqpOkoTegyYtX+xgfHd6s=" \
     -X GET http://127.0.0.1:6001/provenance/data/history/db2/key3?onlydeletes=true | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "values": [
      {
        "value": "dGhpcyBpcyBhIHRoaXJkIHZhbHVl",
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
      }
    ]
  },
  "signature": "MEUCIEk90HGFa90Yr/dBDONE+GVD4GaowdQFRL25+S0ECY7tAiEA7+gJSZD8pt0v+KC318xxyLTmLrzcBPnhEMIb/xUmJtI="
}
```
As we can see here, the value returned by the query is the last value before the key was deleted. If we run same query for `key2`, we will get an empty list.

### History query for the specific version
Let's check what was the value of `key1` at the time when tx with version `{"block_num": 6, "tx_num": 0}` was committed.

> As we mentioned multiple times before that, this version will be marshalled to `{"block_num":6}`, because `tx_num` field is equals to zero, so we can say it value at block 6. 
To do that, we send `/provenance/data/history/{dbname}/{key}?blocknumber={blknum:[0-9]+}&transactionnumber={txnum:[0-9]+}` GET query. This query expected to return a value and metadata for the key at the time point associated with the version parameter.

**Sign json query data**
```sh
bin/signer -data '{"user_id":"alice","db_name":"db2","key":"key1","version":{"block_num":6}}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Signature**
```
MEQCIFBH19/vOMSK1Q66djyS6u/G1YSppl24S00VpmsjodYbAiAxzejpgkHRFSKM/lXIw9hC19Tf5JbdSKsJW/SGXPmrdQ==
```

**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEQCIFBH19/vOMSK1Q66djyS6u/G1YSppl24S00VpmsjodYbAiAxzejpgkHRFSKM/lXIw9hC19Tf5JbdSKsJW/SGXPmrdQ==" \
     -X GET -G "http://127.0.0.1:6001/provenance/data/history/db2/key1" -d blocknumber=6 -d transactionnumber=0 | jq .
```

> Please note, as mentioned earlier, transaction number eliminated from query used for sign, because it equals to 0, but still exists as part of GET url. In case if transaction index in block is not equal 0, `tx_num` will e part of serialized query to sign. 
**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "values": [
      {
        "value": "dGhpcyBpcyBhIGZpcnN0IHZhbHVlIHVwZGF0ZWQ=",
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
      }
    ]
  },
  "signature": "MEYCIQC6SFao6V8C3ETjp1AZ6FPZjUVhA8/DG+Vn9gLDTXjsGgIhAKaA3szWlfEZ4D2SDmBLs6KWxlH5t5byplqp6A8gAl4i"
}
```
Because `key1` was changed in block 6, the version in query result contains `block_num` 6, but, if for example, the value of `key1` last time was changed in block 4, returned version `block_num` will be 4.

### Transactions submitted by user
To query for all the transactions submitted by a specific user, we use `/provenance/data/tx/{user}` GET query.

**Sign json marshaled query**
```sh
bin/signer -data '{"user_id":"alice","target_user_id":"alice"}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Signature**
```
MEYCIQDNvPHvfb0GpBpbQ5Gedm0LmEQZbijkmf3vNt4JbuSr7gIhAJMp2cVFeckAQbLSMQg7Fn5vCMyTjyS4/Z7740eZalTj
```

**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEYCIQDNvPHvfb0GpBpbQ5Gedm0LmEQZbijkmf3vNt4JbuSr7gIhAJMp2cVFeckAQbLSMQg7Fn5vCMyTjyS4/Z7740eZalTj" \
     -X GET http://127.0.0.1:6001/provenance/data/tx/alice | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "txIDs": [
      "1b6d6414-9b58-45d0-9723-1f31712add81",
      "Tx000",
      "Tx001"
    ]
  },
  "signature": "MEUCIQDoeIihFrgwmOk2cQdOUsnlZ7gbDcmuGehkH0y0AYGo/AIgNk+Cy3mTUrGSytj5RIg4jHia/ceAUQ5TW5Q1RmG0FuQ="
}
```

Query result contains three transaction ids, and it means that user `alice` submitted 3 transactions - `"1b6d6414-9b58-45d0-9723-1f31712add81"`, `"Tx000"` and `"Tx001"`.

### User related queries

An important type of query addresses user activity, and allows knowing, for a particular user, which keys she read, wrote or deleted. There are different types of queries for that:

- [User reads]
- [User writes]
- User deletes follow the same pattern.

### Query for users reads

To query which keys were read by a specific user, including keys versions, we use `/provenance/data/read/{user}`GET query.

**Sign json marshalled query**
```sh
bin/signer -data '{"user_id":"alice","target_user_id":"alice"}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Signature**
```
MEUCIQCzRD+rS8043PgFRfwVFYKkT4FN+2KCPqU8OCciXlTz/QIgSDvMl/q+AiyeoZI3qDs87y4ScJgVW2e/6dXtnlTQaCQ=
```

**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEUCIQCzRD+rS8043PgFRfwVFYKkT4FN+2KCPqU8OCciXlTz/QIgSDvMl/q+AiyeoZI3qDs87y4ScJgVW2e/6dXtnlTQaCQ=" \
     -X GET http://127.0.0.1:6001/provenance/data/read/alice | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "KVs": [
      {
        "key": "key1",
        "value": "dGhpcyBpcyBhIGZpcnN0IHZhbHVl",
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
      {
        "key": "key2",
        "value": "dGhpcyBpcyBhIHNlY29uZCB2YWx1ZQ==",
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
      }
    ]
  },
  "signature": "MEUCIAPDXNsnoOCc+PisOiAmvk3giXxNfynW9YjAbcGSvPDaAiEAnKYp2K4WonHofJQ1ag3aYITcdfAXIDwNdggjSwkXS1w="
}
```

We can see that user `alice` read two keys, that happens, as we knew before that, during `Tx001`. Mathematically, the query returns the union of all read sets of all user transactions with addition of values.

### Query for users writes
To query which keys were written by a specific user, including their new values, original versions, etc, we use `/provenance/data/written/{user}`GET query.

**Sign json marshalled query**
```sh
bin/signer -data '{"user_id":"alice","target_user_id":"alice"}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Signature**
```
MEQCIHd3DkqD3AUXRt/V8cVEG6r0DbflL1j8r+FOdNGzfYJlAiAHVdguS0ezDCjAek+yLGRaHsraZ+R7Dr4XmxzSHE3GLQ==
```

**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEQCIHd3DkqD3AUXRt/V8cVEG6r0DbflL1j8r+FOdNGzfYJlAiAHVdguS0ezDCjAek+yLGRaHsraZ+R7Dr4XmxzSHE3GLQ==" \
     -X GET http://127.0.0.1:6001/provenance/data/written/alice | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "KVs": [
      {
        "key": "key1",
        "value": "eXl5",
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
      {
        "key": "key1",
        "value": "dGhpcyBpcyBhIGZpcnN0IHZhbHVl",
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
      {
        "key": "key2",
        "value": "dGhpcyBpcyBhIHNlY29uZCB2YWx1ZQ==",
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
      {
        "key": "key3",
        "value": "dGhpcyBpcyBhIHRoaXJkIHZhbHVl",
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
      {
        "key": "key2",
        "value": "dGhpcyBpcyBhIHNlY29uZCB2YWx1ZSB1cGRhdGVk",
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
      {
        "key": "key1",
        "value": "dGhpcyBpcyBhIGZpcnN0IHZhbHVlIHVwZGF0ZWQ=",
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
      }
    ]
  },
  "signature": "MEUCIQDaMpeJhgVlFOS0AL7AOd2wviQ6c9R/PrOH51grBifRlgIgL4hLscpLE+JNAryd0it+WkuFmiQyUHbg25/Z+T9ty3A="
}
```

As you can see, user `alice` wrote 3 times to key `key1`, in blocks 4, 5 and 6, two times to `key2`, in blocks 5 and 6 and one time to `key3`, in block 5. As we mentioned earlier, query result is union of the union of all write sets of all user transactions, with addition of version.

### Key access queries
Another important view on data history is to know which users accessed a specific key over time. The following two  queries provide us with this information.
- [Query for key readers]
- [Query for key writers]

### Query for key readers
To query for all the users that ever read a specific key, we use `/provenance/data/readers/{dbname}/{key}` GET query. The query results in list of all users who ever read the key and how many times user read the key.

**Sign json marshalled query**
```sh
bin/signer -data '{"user_id":"alice","db_name":"db2","key":"key1"}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Signature**
```
MEUCIQDAO/58FDUjaKx+aAN6D5GCsp87hQ0CePeBwxa3FKzPpQIgASJwGGk8rHtZPgKUEMJ7U4H5L8ttQwnXq7XrSCcL/B4=
```

**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEUCIQDAO/58FDUjaKx+aAN6D5GCsp87hQ0CePeBwxa3FKzPpQIgASJwGGk8rHtZPgKUEMJ7U4H5L8ttQwnXq7XrSCcL/B4=" \
     -X GET http://127.0.0.1:6001/provenance/data/readers/db2/key1 | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "read_by": {
      "alice": 1
    }
  },
  "signature": "MEQCIDI6cs21gAUfE/4l+/ha9ih9wPQ8YnnogpzMRt5XrDR9AiAfOsKf9FhUpeYPmMCvM9gIF0YQO6IFtdZUC8khOGTIcA=="
}
```

User `alice`read the `key1` value one time.

### Query for key writers
To query for all the users that ever wrote to a specific key, we use `/provenance/data/writers/{dbname}/{key}` GET query. This will return list of all users who ever wrote to the key and how many times the user wrote to the key.

**Sign json marshalled query**
```sh
bin/signer -data '{"user_id":"alice","db_name":"db2","key":"key2"}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Signature**
```
MEUCIQDFX8SgtDFMGf2bXpaP7wyGzhR+s/18A6VWzjFmxwawCwIgJOD5N5GtsH2A0XsMGCVKjEWbaRWJ6s7GGsuN24IlIyQ=
```

**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEUCIQDFX8SgtDFMGf2bXpaP7wyGzhR+s/18A6VWzjFmxwawCwIgJOD5N5GtsH2A0XsMGCVKjEWbaRWJ6s7GGsuN24IlIyQ=" \
     -X GET http://127.0.0.1:6001/provenance/data/writers/db2/key2 | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "written_by": {
      "alice": 2
    }
  },
  "signature": "MEQCICKkAFCF/0BCXWBwfXaJfV+dnde6XxpwpiT7Z6I28GlyAiA8zKTtHacZZrx1erv7VSyLZD9sNKzf8JNMYmH6XZu8lw=="
}
```

User `alice`wrote twice to the `key2`.
