---
id: ledger
title: Ledger and Proof queries
---
# Ledger and Proof queries

The Ledger API gives the user access to the data stored in Orion ledger and used to prove data integrity, provides tamper resistance and non-repudiation property.
For more information about data structures exposed by ledger, see [block skip chain](../../../architecture-and-design/block-skip-chain), [Transactions merkle tree](../../../architecture-and-design/tx-merkle-tree) and [state trie](../../../architecture-and-design/state-merkle-patricia-tree)

```go
type Ledger interface {
	// GetBlockHeader returns block header from ledger
	GetBlockHeader(blockNum uint64) (*types.BlockHeader, error)
	// GetLedgerPath returns cryptographically verifiable path between any block pairs in ledger skip list
	GetLedgerPath(startBlock, endBlock uint64) ([]*types.BlockHeader, error)
	// GetTransactionProof returns intermediate hashes from hash(tx, validating info) to root of
	// tx merkle tree stored in block header
	GetTransactionProof(blockNum uint64, txIndex int) (*TxProof, error)
	// GetTransactionReceipt return block header where tx is stored and tx index inside block
	GetTransactionReceipt(txId string) (*types.TxReceipt, error)
	// GetDataProof returns proof of existence of value associated with key in block Merkle-Patricia Trie
	// Proof itself is a path from node that contains value to root node in MPTrie
	GetDataProof(blockNum uint64, dbName, key string, isDeleted bool) (*state.Proof, error)
	// NewBlockHeaderDeliveryService creates a delivery service to deliver block header
	// from a given starting block number present in the config to all the future block
	// till the service is stopped
	NewBlockHeaderDeliveryService(conf *BlockHeaderDeliveryConfig) BlockHeaderDelivererService
}
```