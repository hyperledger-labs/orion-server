---
id: block-header
title: Query a Block Header
---

## Preparing data

> As a prerequisite, we use all data used in provenance example, please reference [here], including transactions from [Prepare data]section.
## Block header query
First query provided by Ledger API is block header query.
Block header contains all cryptographic related data that used for all blockchain properties of BCDB. See [Block Header Data].
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
