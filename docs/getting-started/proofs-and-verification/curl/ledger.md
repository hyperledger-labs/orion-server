---
id: ledger
title: Ledger and Proof queries
---
# Ledger and Proof queries

The ledger API gives the user access to the data stored in the Orion ledger and is used to prove data integrity and provide tamper resistance and non-repudiation. 
For more information about data structures exposed by the ledger, see [block skip chain](../../../architecture-and-design/block-skip-chain), [Transactions merkle tree](../../../architecture-and-design/tx-merkle-tree), and [state trie](../../../architecture-and-design/state-merkle-patricia-tree).

Here are several proof queries accessible in the Orion ledger: 

- Block header query, including all of a block's transaction IDs.
```http request
GET /ledger/block/{blocknum}
```
- Ledger consistency query, i.e., ledger connectivity.
```http request
GET /ledger/path?start={startNum}&end={endNum}
```
- Transaction receipt query, used as an anchor to prove transaction existence and validity.
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

