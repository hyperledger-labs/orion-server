---
id: block-structure
title: Block Structure
---
### Block header data
A block is a collection of ordered transactions in a blockchain database. The header object within a block holds the block number, the root hash of the transaction merkle tree, the root hash of the state merkle tree, and the validation information. Except for the transactions themselves, the rest of the data is stored in the block header.

As mentioned above, the block header contains all the data required to prove the existence of a transaction and/or specific state at block time, in addition to the ledger connectivity (block existence) proofs.
- Block number
- List of hashes (pointers) in the ledger skip chain; for a full explanation, see [here](block-skip-chain)
- Two merkle tree roots:
    - Transaction merkle tree root - for more explanation about non-repudiation and immutability proofs, see [here](tx-merkle-tree)
    - DB state merkle-patricia trie root - ethereum style; for full trie explanation, see [here](state-merkle-patricia-tree)
- Validation info for all block transactions.

```protobuf
// BlockHeaderBase holds the block metadata and the chain information
// that computed before transaction validation
message BlockHeaderBase {
  uint64 number = 1;
  // Hash of (number - 1) BlockHeaderBase
  bytes previous_base_header_hash = 2;
  // Hash of BlockHeader of last block already committed to ledger
  bytes last_committed_block_hash = 3;
  // Number of last block already committed to ledger
  uint64 last_committed_block_num = 4;
}

// BlockHeader holds, in addition to base header, rest of chain information that computed after transactions validation, 
// including state and transaction merkle trees roots, skip-chain hashes and transaction validation info
message BlockHeader {
  BlockHeaderBase base_header = 1;
  // Skip chain hashed, based of BlockHeader hashed of blocks connected in blocks skip list
  repeated bytes skipchain_hashes = 2;
  // Root of Merkle tree that contains all transactions, including validation data
  bytes tx_merkle_tree_root_hash = 3;
  // Root hash of system wide state merkle-particia tree
  bytes state_merkle_tree_root_hash = 4;
  // Validation info for transactions in block.
  repeated ValidationInfo validation_info = 5;
}
```