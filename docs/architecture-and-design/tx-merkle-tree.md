---
id: tx-merkle-tree
title: Transaction Merkle Tree
---

## Merkle tree

From wiki: “Merkle tree is a tree in which every leaf node is labelled with the cryptographic hash of a data block, and every non-leaf node is labelled with the cryptographic hash of the labels of its child nodes.”
![Merkle tree](MerkleTree1.png)
- In blockchain world, Merkle tree usually used to verify existence and validity of transactions in block.
- Thanks to tree properties, to prove specific transaction existence in block, no need to expose rest of block transactions, just content of transaction itself and list of *log(N)* hashes.
- Usually, only Merkle tree root hash exposedto end-user.

### Merkle tree proof example
Here example of proof generation and verification, based on example tree ![tree](BlockMerkleTree.png)
- To prove that `Tx3` is part of block Merkle tree with root **B3..** , we need `Tx3`, its hash – **15..** and **78..** (hash of sibling transaction),  **A2..** (sibling in Merkle tree), **F5..** (sibling in Merkle tree).
- So, final proof is {**15..**, **78..**, **A2..**, **F5..**, **B3..**}
- Proof validation: _hash(hash(hash(**15..** || **78..**) || **A2..**) || **F5..**) == **B3..**_

- This is exact structure we used to prove specific transaction existence in block.
- We store only Merkle tree root hash as part of block header and recalculate tree on demand.

### BCDB Merkle tree API

Server side - Merkle tree implementation:

```go
// Node struct keep data for Merkle tree node. For now, it is binary Merkle tree
type Node struct {
...	
}

// BuildTreeForBlockTx builds Merkle tree from block transactions.
// Each data leaf addressed by its index inside block 
func BuildTreeForBlockTx(block *types.Block) (*Node, error)

// Proof calculate intermediate hashes between leaf with given index and root (caller node)
func (n *Node) Proof(leafIndex int) ([][]byte, error) {
    path, err := n.findPath(leafIndex)
    if err != nil {
        return nil, err
}
```

Protobuf's transaction proof message:
```protobuf
message GetTxProofQuery {
  string user_id = 1;
  uint64 block_number = 2;
  uint64 tx_index = 3;
}

message GetTxProofResponse {
  ResponseHeader header = 1;
  repeated bytes hashes = 2;
}
```

SDK side - Transaction proof validation:
```go
type TxProof struct {
	intermediateHashes [][]byte
}

func (p *TxProof) Verify(receipt *types.TxReceipt, tx proto.Message) (bool, error)
```
