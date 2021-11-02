---
id: block
title: Proof of Existence of a Block
---

## Path in ledger query

This type of query used to prove that block X connected to block Y, based on skip chain hashes. Query result contains list of block headers, including block X, connecting blocks and block Y. X > Y, because connectivity in skip chain, like all blockchains, is from latter block to earlier.
For more information about ledger consistency, see here (TODO) and here (TODO).

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
