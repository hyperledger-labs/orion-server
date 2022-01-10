---
id: tx
title: Proof of Existence of a Transaction
---

### Transaction proof query

To prove the existence of a transaction in a specific block, we provide the Merkle tree path from the leaf (transaction) to the tree root. For more details, see [Merkle tree].

We use `/ledger/proof/tx/{blockId:[0-9]+}?idx={idx:[0-9]+}` for the GET query for this query.

**Sign JSON serialized query**
```sh
bin/signer -data '{"user_id":"alice","block_number":5}' -privatekey=deployment/sample/crypto/alice/alice.key
```
> `tx_index` is eliminated in JSON because it equals zero.

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

Because each block contains only one transaction, there's only one intermediate hash in the list.

