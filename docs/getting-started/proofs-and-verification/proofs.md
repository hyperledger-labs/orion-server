---
id: proofs
title: Ledger Data and Cryptographical Proofs
---
## Ledger Data and Cryptographical Proofs
The Ledger API gives the user access to the data stored in Orion ledger and used to prove data integrity, provides tamper resistance and non-repudiation property.
For more information about data structures exposed by ledger, see [block skip chain](../../architecture-and-design/block-skip-chain), [Transactions merkle tree](../../architecture-and-design/tx-merkle-tree) and [state trie](../../architecture-and-design/state-merkle-patricia-tree)

In details, Ledger API give user access to ledger data and integrity proofs
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

### Proofs

#### Ledger connectivity proof
Proof generation:
1. Alice is provided with receipt (block header) from Bob who claim that some block is part of ledger
2. Alice requests from the server the header of the last block in the ledger, without revealing to the server which block she wants to validate 
   - She can store genesis block a-priori or request it as well
4. Alice requests the shortest path in the skip list from the block she validates to the genesis block
5. Alice requests the shortest path from the last block of the ledger (2) to the block she validates
7. Alice validates all a-priori known headers - genesis, block, last - in both paths and concatenate both paths, result is block proof

To check proof validity, for each block header in the proof, starting from Genesis, Alice executes following steps: 
1. Calculates the block header hash
2. If the calculated hash is in the next block header skip-list hash list, continue
3. If not, fail

Optimization - because the result of each call to `Commit()` is a transaction receipt containing a block header, the user can use this header, if it has a higher block number than the block he wants to validate, thus avoiding the need to ask the server for the last block.

Please note that the algorithm above allows to build proofs from multiple blocks.
For example, proof for blocks 3 and 5 will include (8, 6, 5, 4, 3, 2, 0). It is useful while validating specific value history.

Algorithm above, used as first step of [transaction proof](#transaction-existence-proof)/[state proof](#state-proof).
- To prove that block(s) is/are part of ledger


#### Transaction existence proof
Here is a detailed explanation for transaction existence proof, based on data stored during tx commit
Alice submit tx and in the future should prove its existence:
- During transaction commit, `TxReceipt`, containing block header returned as proof.
    - Transaction content and `TxReceipt` stored by Alice to use as proof to another users in the future.
- At some point in future Alice needs to prove (to Bob) that tx is part of ledger, using proof stored since tx commit
    - Alice provides Bob with `TxReceipt`, signed by server and transaction content, signed by Alice.
    - First, Bob checks ledger connectivity and that block header in `TxReceipt` is part of ledger, as described [here](#ledger-connectivity-proof)
    - Second, Bob calls `GetTxProof()` for tx path in the block merkle tree and tx content already provided by Alice
    - Bob validates proof using tx hash and merkle root stored in block header, for more details see [here](../../architecture-and-design/tx-merkle-tree#merkle-tree-proof-example)

![tx_proof](TxProof.png)

#### State proof
Here is a detailed explanation for historical state proof, based on data stored in Merkle-Patricia Trie.
Alice wants to get cryptographic proof from server that that key `K` was associated with value `X` at the end of block `N`.
- Alice asks server to `BlockHeader` for block `N`.
- As described in [here](#ledger-connectivity-proof), Alice checks ledger connectivity and that block header `N` is part of ledger.
- Alice calls to `GetDataProof()` and verify it.
  - Calculates hash of `<db, key, value>` tuple by calling `ConstructCompositeKey` and `CalculateKeyValueHash`
  - Call to `proof.Verify()`

![state_proof](StateProof.png)