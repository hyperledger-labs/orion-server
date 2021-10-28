# Ledger and Proof queries

## Preparing data

> As a prerequisite, we use all data used in provenance example, please reference [here](./provenance.md), including transactions from [Prepare data](./provenance.md#prepare-data) section.

## Block header query
First query provided by Ledger API is block header query.
Block header contains all cryptographic related data that used for all blockchain properties of BCDB. See [Block Header Data](../proofs/Provenance-and-proofs.md#block-header-data).
Result of this query used both to validate `TxReceipt` and ledger connectivity. To access it, we use `/ledger/block/{blocknum}` GET query. 

In this example we will query ledger for block 5 header.

**Sign json serialized query**
```sh
bin/signer -data '{"user_id":"alice","block_number":5}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Signature**
```
MEQCIEpt+TSeA6keDsecrim2EajdVtoeo1cgq9NN/WXEJ5HWAiB5ijMeZZ5y16CCzjXfIpqqnLCANGslD8nZ+0pTVwDrdA==
```

**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEQCIEpt+TSeA6keDsecrim2EajdVtoeo1cgq9NN/WXEJ5HWAiB5ijMeZZ5y16CCzjXfIpqqnLCANGslD8nZ+0pTVwDrdA==" \
     -X GET http://127.0.0.1:6001/ledger/block/5 | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "block_header": {
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
  },
  "signature": "MEUCIQDMER7sTBmOQNHRV6/GON4OjrsKhE6Di5ok181JpwLFvgIgIQA+DHIr6x+GJe8dnF0FLgzXJ+29H9sZHZE46fVg5HQ="
}
```
As you can see, BlockHeader contains 3 hashes in `skipchain_hashes` section (blocks 4, 3, 1), previous block hash `previous_base_header_hash` (block 4), roots of tx merkle tree `tx_merkel_tree_root_hash` and state merkle-patricia trie `state_merkel_tree_root_hash`. 

## Transaction receipt query
Transaction commit can be done in synchronous or asynchronous way. In case of synchronous call, `TxReceipt` is part of result. In case of asynchronous call, no `TxReceipt` exist yet, and we have to access ledger to for it.
This query used to get transaction receipt for specific tx from ledger, using `/ledger/tx/receipt/{TxId}` GET query. 
We use `Tx000` as example.

**Sign json serialized query**
```sh
bin/signer -data '{"user_id":"alice","tx_id":"Tx000"}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Signature**
```
MEUCIQC1u0SoiuSFLb2mzjU6XQ2EfQQynhqeXFSFkS5QYKbAUQIgV6Lo/b/qYatfO7l6TOolQD9HOBI+PkyptKor68GA8bE=
```

**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEUCIQC1u0SoiuSFLb2mzjU6XQ2EfQQynhqeXFSFkS5QYKbAUQIgV6Lo/b/qYatfO7l6TOolQD9HOBI+PkyptKor68GA8bE=" \
     -X GET http://127.0.0.1:6001/ledger/tx/receipt/Tx000 | jq .
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
  "signature": "MEUCIQCpRqtZ0TDFUNonkRNrp4CzgkbFds5FiN6vAPoGDp3kdQIgbMo4xec6LYhg1ji+HhG12xho2qbfDWfWa7DPp31d5HI="
}
```

In protobuf object definition, TxReceipt contains block header and tx index inside block. Because `tx_index` is equal to zero, protobuf marshaling eliminates this field. If `tx_index` is not 0, it will be part of response json.  
So, from response, we see that `Tx000` stored in block 5, tx_index 0.

## Path in ledger query

This type of query used to prove that block X connected to block Y, based on skip chain hashes. Query result contains list of block headers, including block X, connecting blocks and block Y. X > Y, because connectivity in skip chain, like all blockchains, is from latter block to earlier.
For more information about ledger consistency, see [here](../proofs/Provenance%20and%20Tx%20proofs.md) and [here](../proofs/skip-chain.md).

Server expose `ledger/path?start={startNum}&end={endNum}` GET query to access to ledger paths.

**Sign json serialized query**
```sh
bin/signer -data '{"user_id":"alice","start_block_number":1,"end_block_number":6}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Signature**
```
MEYCIQC6C8c4NBgnfAEC0L5ftPOQqxbP53rypSHKL+oZ7O5cggIhAPyz1Jg8Dlv98XVDM6GaN9D17X0XiVvA7D2l8cibXcYr
```
**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEYCIQC6C8c4NBgnfAEC0L5ftPOQqxbP53rypSHKL+oZ7O5cggIhAPyz1Jg8Dlv98XVDM6GaN9D17X0XiVvA7D2l8cibXcYr" \
     -X GET -G "http://127.0.0.1:6001/ledger/path" -d start=1 -d end=6 | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "block_headers": [
      {
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
      },
      {
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
      },
      {
        "base_header": {
          "number": 1
        },
        "tx_merkel_tree_root_hash": "1XYyyOxBKRKw/vHgGGClrbko+wjkHmgzkWIjWEqoGaU=",
        "state_merkel_tree_root_hash": "Gxq3k91oLlxknN9VJCBEnc9IFnJU7YKGbiKtbQLMDF0=",
        "validation_info": [
          {}
        ]
      }
    ]
  },
  "signature": "MEQCIF1CZuJcUOBG3IkFIo0YkoB6+6X6HvUYaPX4KF/MoIScAiB8L13Da+5rKta9L+kggWJlp9uwsHSt2mQJ/tWliTeliQ=="
}
```

As you can see, block 6 points to block 5 and block 5 to block 1 (Genesis)

>Worth to mention that block numbering in BCDB starts from 1 and not from 0. All power two operations related to block number, require decreasing block number by 1

### Transaction proof query

To prove transaction existence in specific block, we provide merkle tree path from leaf (transaction) to tree root. For more details see [Merkle tree](../proofs/Merkle-tree.md) 

We use `/ledger/proof/tx/{blockId:[0-9]+}?idx={idx:[0-9]+}` GET query for this query.

**Sign json serialized query**
```sh
bin/signer -data '{"user_id":"alice","block_number":5}' -privatekey=deployment/sample/crypto/alice/alice.key
```
> `tx_index` eliminated in json because it equals to zero.

**Signature**
```
MEQCID3KRS2YWmMnniuXkjgjV06s40zUTgiY7fTqufZ4N/OKAiBh6Q3qMWWx6PKcw9L0uLpal8ZmNp/rjX/iQfZiDnsn1g==
```
**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEQCID3KRS2YWmMnniuXkjgjV06s40zUTgiY7fTqufZ4N/OKAiBh6Q3qMWWx6PKcw9L0uLpal8ZmNp/rjX/iQfZiDnsn1g==" \
     -X GET -G "http://127.0.0.1:6001/ledger/proof/tx/5?idx=0" | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "hashes": [
      "UXXqKu/I2Vr0Ma5cV9Hfun4Xo5285ZwdV9jcKspTnJo="
    ]
  },
  "signature": "MEQCIExIMmMUPosWudaj2CBFs04biV4KPZOTmKwSm9kcc4grAiAxbNxRtmJSbk9rXLYJOWscygEvLgZIgQFf+OiART6RpQ=="
}
```

Because block contains only one transaction, we have only one intermediate hash in list.

### State proof query

As it mentioned in BCDB description [here](../../README.md), BCDB maintains separated persisted graph data structure for historical data transitions, so a user can execute query on those historical changes to understand the lineage of each data item. For more explanation about **Provenance Queries** and different views they provide on historical data, see [here](./provenance.md).

As complimentary to provenance, BCDB uses Ethereum style Merkle-Particia Trie to provide cryptographically verifiable proofs of all state transitions. 
Although it provides only one  single type of proof - that specific key was associated with specific value while specific block was committed to ledger.

For each block, root of Merkle-Particia Trie stored inside block header - `state_merkel_tree_root_hash` and because tamper-prove nature on ledger, trie root is enough to prove existence of specific value at time block was committed. 


**Sign json serialized query**
```sh
bin/signer -data '{"user_id":"alice","block_number":5,"db_name":"db2","key":"key1"}' -privatekey=deployment/sample/crypto/alice/alice.key
```

**Signature**
```
MEUCIElKzVqsY/4Yf1gf+3PCU0Su7KF8scdcTkRjZwQjjc0QAiEA/MbcF3XUzdfZkLIPI0jEayrqRwuC4bLLsqtzT5ArObc=
```
**Submit query**
```sh
curl \
     -H "Content-Type: application/json" \
     -H "UserID: alice" \
     -H "Signature: MEUCIElKzVqsY/4Yf1gf+3PCU0Su7KF8scdcTkRjZwQjjc0QAiEA/MbcF3XUzdfZkLIPI0jEayrqRwuC4bLLsqtzT5ArObc=" \
     -X GET -G "http://127.0.0.1:6001/ledger/proof/data/db2/key1?block=5" | jq .
```

**Output**
```json
{
  "response": {
    "header": {
      "node_id": "bdb-node-1"
    },
    "path": [
      {
        "hashes": [
          "AgIKCw4KDAkAAw8PBAkDDAIGDQILCAMHCQ8MAgkGBAsHDwYKAwUKBgkJAA0JCAwICQYBCg4ICwIGDA0ODwI=",
          "y08re7h7i3RY2CTFDDiZB5HQUhTK0pJfB++VRIG204E="
        ]
      },
      {
        "hashes": [
          null,
          null,
          null,
          null,
          "DpE0Npt4BFM3vZsJzmG4PO1xlMYTpoA5U3nK10Vejsc=",
          "0U6UMnKLCuzIMOsosKa1mZjfqk/vU/Lfpxw2nBDfuFQ=",
          null,
          null,
          null,
          "ZHm1aGvKmHJFL5H9gEIGyo3sNK+m+0T5Y4W7RalD3rE=",
          null,
          null,
          null,
          null,
          null,
          null
        ]
      },
      {
        "hashes": [
          "U9PoJB+8o7P9M7EsbVfVxNd2sENGYlXqeviBYUxLvU0=",
          "oj5KCPc3GFzGJPHjfpnFagPpQ5la98LWreApQNc3ssw=",
          "ENQ9r2wqD202CibxAlru6z/xfdHouQeTQ3x+466a4ik=",
          "ni4p/R5zAdxjOje/0vTLhk0nKSDqe9SgLxmXc7Iw+BY=",
          null,
          null,
          "8IqfhFECCgEPd8uOYjNnfNULy4l3oOirqU4hhIQChDE=",
          null,
          null,
          null,
          null,
          "P/6M7PdrnB9aoxEx5MkVtTVCfrQeUoGoFPk/oyey+48=",
          null,
          null,
          "ZLxYv3o1wbGKjAYNlfU5wzvH1VqB6PO+TpbDgrSyhDo=",
          "hgbr1+op4LBUUXNn1paw14L0kuaOyOU5XgtvXbWebl8="
        ]
      }
    ]
  },
  "signature": "MEYCIQCxrMF5rZlsv/4CcICzcvdH/Xbn+C99Mqswuvdy3gLzegIhANla0V7MGrir9c/I5Q+dIzRDjMnS7GJgdcah0p8XgO1U"
}
```

First element in path is ValueNode - for node types see [here](../proofs/State-Trie.md#patricia-trie)