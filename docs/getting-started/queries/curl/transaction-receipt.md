---
id: transaction-receipt
title: Query a Transaction Receipt
---

## Transaction receipt query
A transaction commit can be done in a synchronous or asynchronous fashion. In the case of a synchronous call, `TxReceipt` is part of result. In the case of an asynchronous call, no `TxReceipt` exists yet, so we have to access the ledger for it.
To get a transaction receipt for a specific tx from the ledger, use the `/ledger/tx/receipt/{TxId}` GET query. 
We use `Tx000` as an example. 

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

In the protobuf object definition, TxReceipt contains a block header and the tx index inside block. Because `tx_index` is equal to zero, protobuf marshaling eliminates this field. If `tx_index` is not 0, it will be part of the response JSON.  
So, from that response, we see that `Tx000` is stored in block 5, tx_index 0.


