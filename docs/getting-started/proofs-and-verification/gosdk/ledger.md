---
id: ledger
title: Ledger and Proof queries
---

### Ledger API

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