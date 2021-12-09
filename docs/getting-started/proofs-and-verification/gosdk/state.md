---
id: state
title: Proof of Existence of a State/Data
---
### State proof example
Below is a simple code snippet that shows how to use the ledger API in order to generate a state existence proof:
>Please pay attention, for presentation purpose, all results and error handling, except proofs were eliminated.
Ledger connectivity proof is eliminated here as well, for details and example see [here](../proofs#ledger-connectivity-proof) and [here](ledger)

```go
    blockNum := 5
	db := "db2"
	key := "key2"
	value := "this is a second value"

	db, err := bcdb.Create(conConf)
    session, err := db.Session(&c.SessionConfig)
    ledger, err := session.Ledger()

	// Get data proof for <db, key> at block blockNum and validate it
	dataProof, err := ledger.GetDataProof(blockNum, db, key, false)
	// Calculating <db, key, value> tuple hash
	compositeKey, err := state.ConstructCompositeKey(db, key)
	kvHash, err := state.CalculateKeyValueHash(compositeKey, []byte(value))
	// Validation proof
	res, err := dataProof.Verify(kvHash, blockHeader.GetStateMerkelTreeRootHash(), false)

    if !res {
        fmt.Printf("DB-Key-Value tuple (%s, %s, %s) at time of block %d is not part of ledger\n", db, key, value, txReceipt.Header.BaseHeader.Number)
    }
```