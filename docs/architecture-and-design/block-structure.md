---
id: block-structure
title: Block Structure
---
### Block header data
A block is a collection of ordered transactions in blockchain database. The header object within a block holds the block number,
root hash of the transaction merkle tree, root hash of the state merkle tree, and validation information. Except to transactions themselves, rest of the data is
stored in block header.

As mentioned above, block header contains all data required to prove existence of transaction or/and specific state at block time,
in addition to ledger connectivity (block existence) proofs
- Block number
- List of hashes (pointers) in ledger skip chain, for full explanation see [here](block-skip-chain)
- Two merkle tree roots
    - Transactions merkle tree root - for more explanation about non-repudiation and immutability proofs see [here](tx-merkle-tree)
    - DB state merkle-patricia trie root - ethereum style, for full trie explanation see [here](state-merkle-patricia-tree)
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
  bytes tx_merkel_tree_root_hash = 3;
  // Root hash of system wide state merkle-particia tree
  bytes state_merkel_tree_root_hash = 4;
  // Validation info for transactions in block.
  repeated ValidationInfo validation_info = 5;
}
```