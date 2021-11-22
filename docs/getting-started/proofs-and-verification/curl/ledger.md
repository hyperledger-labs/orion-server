---
id: ledger
title: Ledger and Proof queries
---
# Ledger and Proof queries

The Ledger API gives the user access to the data stored in Orion ledger and used to prove data integrity, provides tamper resistance and non-repudiation property.
For more information about data structures exposed by ledger, see [block skip chain](../../../architecture-and-design/block-skip-chain), [Transactions merkle tree](../../../architecture-and-design/tx-merkle-tree) and [state trie](../../../architecture-and-design/state-merkle-patricia-tree)
- The Block Header query, including all block's transaction IDs.
```http request
GET /ledger/block/{blocknum}
```
- Ledger consistency query. i.e. ledger connectivity.
```http request
GET /ledger/path?start={startNum}&end={endNum}
```
- Transaction receipt query, used as anchor to prove transaction existence and validity.
```http request
GET /ledger/tx/receipt/{TxId}
```
- Block Merkle Tree transaction proof query.
```http request
GET /ledger/proof/tx/{blockId:[0-9]+}?idx={idx:[0-9]+}
```
- Orion Merkle-Patricia Trie state proof query.
```http request
GET /ledger/proof/data/{dbName}/{key}?block={blockNum}
```

