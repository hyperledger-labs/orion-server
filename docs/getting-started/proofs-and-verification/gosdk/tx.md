---
id: tx
title: Proof of Existence of a Transaction
---

### Transaction proof example
Below is a simple code snippet that shows how to use the ledger API in order to generate a transaction existence proof:
>Please pay attention, for presentation purpose, all results and error handling, except proofs were eliminated.
Ledger connectivity proof is eliminated here as well, for details and example see [here](../proofs#ledger-connectivity-proof) and [here](ledger) 

```go
    db, err := bcdb.Create(conConf)
    session, err := db.Session(&c.SessionConfig)
    ledger, err := session.Ledger()
    
    // Load tx content from file 
    txBytes, err := ioutil.ReadFile(txFile)
    tx := &types.DataTxEnvelope{}
    err = proto.Unmarshal(txBytes, tx)
	txId := tx.Payload.TxId

    // Get transaction receipt for txId
    txReceipt, err := ledger.GetTransactionReceipt(txId)

    // Check if tx is valid
    if txReceipt.Header.ValidationInfo[txReceipt.TxIndex].Flag != types.Flag_VALID {
        fmt.Printf("Transaction number %s validation flag is invalid: %s\n", txId, txReceipt.Header.ValidationInfo[txReceipt.TxIndex].Flag)
    }

    // Get proof for tx existence and verify it
	txProof, err := ledger.GetTransactionProof(txReceipt.Header.BaseHeader.Number, txReceipt.TxIndex)
	// To validate data tx, you need its content, maybe stored somewhere
	res, err := txProof.Verify(txReceipt, tx)
	if !res {
        fmt.Printf("Transaction %s is not part of block %d\n", tx.Payload.TxId, txReceipt.Header.BaseHeader.Number)
    }
	
```