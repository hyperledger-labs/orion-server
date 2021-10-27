## Proofs algorithm based on skiplist like chain

To prove ledger consistency, we should  to prove that block is part of the ledger, using blockchain properties, like hash of previous block.

The naive implementation is to return all blocks hashes from the genesis block to block we want to validate,
given users already keep the genesis block locally. The size of proof is O(N), not acceptable for big ledgers. 

Bitcoin like ledger use this approach because ledger blocks already replicated between clients and stored locally.

Here we discuss implementation, which provides proof of O(log(N)) size and computation and space-efficient.

If <img src="https://render.githubusercontent.com/render/math?math=BlockIndex \mod 2^{i} = 0">, block header, in addition to previous block hash, should contain hash of <img src="https://render.githubusercontent.com/render/math?math=BlockIndex - 2^{i}"> block, see example below:
![Block Skip List](../figures/BlockSkipList.png)  
Worth mentioning, that block with index <img src="https://render.githubusercontent.com/render/math?math=2^{i}"> will contain _i_ hashes in its header.

Validation algorithm just finds the shortest path from last block in ledger to block _i_ and from block _i_ to the genesis block. The result is list of `BlockHeader` containing each block in the path.
- Let's mark blocks in the picture as 0,1,...,7,8
- To generate proof block 3
    - First we build path from last block to 3
        - Adding header of last block, 8
        - 8 has 4 hashes, one of them to block 4, so adding 4's header
        - 4 has access to 3, we will add 3's header as well, for consistency
    - Next stage is find path to genesis block
        - Block 3 contains only one hash, of block 2, so we add the header of block 2
        - Block 2 contains two hashes, block 1 and block 0, so we add block 0 hash
    - As result, we have (8, 4, 3, 2, 0)
    - Now lets reverse it, because it is easier to validate hashes for genesis block
- Validation of proof done by checking hash references in all blocks in path

### Proof generation and validation algorithm
Proof generation:
- Zero step – user provided with receipt (block header) from another user who claim that some block is part of ledger
- First step – user requests header of last block in ledger, without discovering what block he wants to validate
    - User can store genesis block apriori or request it as well
- Second step – user requests the shortest path in skip list from block he validates to genesis block
- The third step – user requests the shortest path from last block (first step) to block he validates
- The forth step – user validates all apriori known headers - genesis, block, last - in both paths and concatenate both paths, result is block proof

Proof validation:
- For each block header in proof, starting from Genesis
    - Calculate block hash, by calculating block header hash
    - If calculated hash is in next block header hashes list, continue
    - If not, fail

Optimization - because result of each call to Commit() is transaction receipt contains block header, user can use this header, only make sure that it has higher block number

Please note that algorithm above allows to build proofs from multiple blocks.
For example, proof for blocks 3 and 5 will include (8, 6, 5, 4, 3, 2, 0). It is useful while validating specific value history.

Algorithm above, used as first step of transaction/state proof. 
- To prove that block(s) is/are part of ledger

Here ledger response for path between two blocks:
```protobuf
message GetLedgerPathResponse {
  ResponseHeader header = 1;
  repeated BlockHeader block_headers = 2;
}
```
