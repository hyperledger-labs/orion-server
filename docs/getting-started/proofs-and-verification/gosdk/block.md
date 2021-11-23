---
id: block
title: Proof of Existence of a Block
---
## Path in ledger query

This type of query used to prove that block X connected to block Y, based on skip chain hashes. Query result contains list of block headers, including block X, connecting blocks and block Y. X > Y, because connectivity in skip chain, like all blockchains, is from latter block to earlier.
For more information about ledger consistency, see here [Block Skip Chain](../../../architecture-and-design/block-skip-chain) and here [Block Structure](../../../architecture-and-design/block-structure).

For full Ledger API exposed by Orion Go SDK, see  
### Ledger API

Ledger API give user access to ledger data and integrity proofs
- block header data
- transaction existence in block proof
- block(s) existence in ledger proof
- specific db `key->value` existence at specific (block) time proof
- receipt for specific tx

### SDK API to access ledger and proofs data:
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
	// GetDataProof returns path in Merkle-Patricia Trie from leaf (value) to trie root stored in BlockHeader
	GetDataProof(blockNum uint64, dbName, key string, isDeleted bool) (*state.Proof, error)
}
```
### Ledger data access example
Attached simple code snippet to show how to call all Ledger API from SDK:
>Please pay attention, for presentation purpose, all results handling, except proofs and error handling were eliminated.
```go
	db, err := bcdb.Create(conConf)
	session, err := db.Session(&c.SessionConfig)
	ledger, err := session.Ledger()
	// Get block header for block 5")
	blockHeader, err := ledger.GetBlockHeader(5)
	
	// Get transaction receipt for "Tx000"
	txReceipt, err := ledger.GetTransactionReceipt("Tx000")
	
	// Get shortest path in ledger skip list between blocks and verify it
	path, err := ledger.GetLedgerPath(1, 6)
	res, err := bcdb.VerifyLedgerPath(path)
	// Get proof for tx existence and verify it
	txProof, err := ledger.GetTransactionProof(5, 0)
	// To validate data tx, you need its content, maybe stored somewhere
	res, err := txProof.Verify(txReceipt, tx)
	
	// Get data proof for "key2" at block 5 and validate it
	dataProof, err := ledger.GetDataProof(5, "db2", "key2", false)
	// Calculating <key, value> pair hash
	compositeKey, err := state.ConstructCompositeKey("db2", "key2")
	kvHash, err := state.CalculateKeyValueHash(compositeKey, []byte("this is a second value"))
	// Validation proof
	res, err := dataProof.Verify(kvHash, blockHeader.GetStateMerkelTreeRootHash(), false)
```