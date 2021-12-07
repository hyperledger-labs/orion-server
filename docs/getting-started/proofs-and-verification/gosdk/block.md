---
id: block
title: Proof of Existence of a Block
---
## Path in ledger query

This type of query used to prove that block X connected to block Y, based on skip chain hashes. Query result contains list of block headers, including block X, connecting blocks and block Y. X > Y, because connectivity in skip chain, like all blockchains, is from latter block to earlier.
For more information about ledger consistency, see here [Block Skip Chain](../../../architecture-and-design/block-skip-chain) and here [Block Structure](../../../architecture-and-design/block-structure).

For more details on the full Ledger API exposed by the Orion Go SDK, see [here](ledger)

### Ledger data access example
Attached simple code snippet to show how to call block retrieval and ledger connectivity validation APIs from SDK:
>Please pay attention, for presentation purpose, all results and errors handling's, except proofs were eliminated.
```go
    txId := "Tx000" 
	db, err := bcdb.Create(conConf)
	session, err := db.Session(&c.SessionConfig)
	ledger, err := session.Ledger()

	// Get block header for block 5"
	blockHeader, err := ledger.GetBlockHeader(5)
	
	// Get transaction receipt for txId
	txReceipt, err := ledger.GetTransactionReceipt(txId)

	// Get the shortest path in ledger skip list between blocks and verify it
	path, err := ledger.GetLedgerPath(1, txReceipt.Header.BaseHeader.Number)
    res, err := bcdb.VerifyLedgerPath(path)
	
	if !res {
        fmt.Printf("Block %d is not part of ledger\n", txReceipt.Header.BaseHeader.Number)
    }
````

