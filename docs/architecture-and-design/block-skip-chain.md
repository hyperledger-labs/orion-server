---
id: block-skip-chain
title: Block Skip Chain
---
## Proofs algorithm based on skiplist like chain

To prove ledger consistency, we need to prove that a block is part of the ledger, using blockchain properties, like the hash of the previous block.

The naive implementation is to return all block hashes from the genesis block to the block we want to validate, given users already keep the genesis block locally. The size of the proof is O(N), which is not acceptable for big ledgers.

Bitcoin-like ledgers use this approach because ledger blocks are already replicated between clients and stored locally.

Here we discuss implementation, which provides proof of O(log(N)) size, computation, and space-efficiency.

If the <img src="https://render.githubusercontent.com/render/math?math=BlockIndex \mod 2^{i} = 0" /> block header, in addition to the previous block hash, should contain the hash of the <img src="https://render.githubusercontent.com/render/math?math=BlockIndex - 2^{i}" /> block (see the example below)
![Block Skip List](BlockSkipList.png)  
it's worth mentioning, that the block with index <img src="https://render.githubusercontent.com/render/math?math=2^{i}" /> will contain _i_ hashes in its header.

### Ledger connectivity proof example
The validation algorithm finds the shortest path from the last block in the ledger to block _i_ and from block _i_ to the genesis block. The result is a list of `BlockHeader` containing each block in the path.
- Let's mark blocks in the picture as 0,1,...,7,8
- To generate a proof for block 3
    - First we build a path from the last block to 3
        - Adding the header of the last block, 8
        - 8 has 4 hashes, one of them to block 4, so adding 4's header
        - 4 has access to 3, so we add 3's header as well, for consistency
    - Next stage is to find the path to the genesis block
        - Block 3 contains only one hash, of block 2, so we add the header of block 2
        - Block 2 contains two hashes, block 1 and block 0, so we add the block 0 hash
    - As a result, we now have (8, 4, 3, 2, 0)
    - Now let's reverse it, because it's easier to validate hashes for the genesis block
- Validation of proof is done by checking hash references in all the blocks in the path

### Proof generation and validation algorithm
For detailed ledger connectivity proof generation and verification, see [here](../getting-started/proofs-and-verification/proofs#ledger-connectivity-proof).
