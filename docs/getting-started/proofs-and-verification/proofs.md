---
id: proofs
title: Ledger Data and Cryptographical Proofs
---
## Ledger Data and Cryptographical Proofs
Ledger API give user access to ledger data and integrity proofs
- block header data
- transaction existence in block proof
- block(s) existence in ledger proof
- specific db `key->value` existence at specific (block) time proof
- receipt for specific tx

There are multiple ways to access ledger data, including proofs. First, there are multiple sdks, including GO, for details see [here](gosdk/ledger)
and REST API to exposed by server. For details see [here](curl/ledger)

Based on Provenance and Ledger APIs, we can check
- ledger integrity by accessing block headers and validating consistency of ledger skip list
- transaction existence proof composed of merkle tree path to transaction in block and block existence proof: `GetTransactionProof()` and `GetLedgerPath()`
- block existence proof from TxReceipt or by accessing block headers and validating consistency of ledger skip list
- proof of active and past states can be done by `GetStateProof()` proving existence of all state changes

### Detailed proofs
Block proof contains connected list of blocks from end block to start block
```protobuf
message GetLedgerPathResponse {
  ResponseHeader header = 1;
  repeated BlockHeader block_headers = 2;
}
```
Transaction proof contains path in merkle tree in the block to the root.
```protobuf
message GetTxProofResponse {
  ResponseHeader header = 1;
  repeated bytes hashes = 2;
}
```
State proof contains path in block merkle-patricia trie from leaf (key,value) to the root.
```protobuf
message GetDataProofResponse {
  ResponseHeader header = 1;
  repeated MPTrieProofElement path = 2;
}

message MPTrieProofElement {
  repeated bytes hashes = 1;
}
```