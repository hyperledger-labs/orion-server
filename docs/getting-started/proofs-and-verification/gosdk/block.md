---
id: block
title: Proof of Existence of a Block
---
## Path in ledger query

This type of query is used to prove that block X is connected to block Y, based on skip chain hashes. The query result contains a list of block headers, including block X, connecting blocks, and block Y. X > Y, because connectivity in skip chains, like in all blockchains, is from the latter block to the earlier one.
For more information about ledger consistency, see [Block Skip Chain](../../../architecture-and-design/block-skip-chain) and [Block Structure](../../../architecture-and-design/block-structure).

For more details on the full Ledger API exposed by the Orion Go SDK, see [here](ledger).

### Ledger data access example
The attached simple code snippet shows how to call block retrieval and ledger connectivity validation APIs from the SDK:
>Please note: For presentation purposes, all results and error handling, except proofs, were eliminated.
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

