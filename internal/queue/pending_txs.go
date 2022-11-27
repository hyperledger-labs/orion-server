// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"sync"

	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type PendingTxs struct {
	txs    sync.Map
	logger *logger.SugarLogger
}

func NewPendingTxs(logger *logger.SugarLogger) *PendingTxs {
	return &PendingTxs{
		logger: logger,
	}
}

// Add returns true if the txId was already taken
func (p *PendingTxs) Add(txID string, promise *CompletionPromise) bool {
	_, loaded := p.txs.LoadOrStore(txID, promise)
	p.txs.Store(txID, promise)
	return loaded
}

func (p *PendingTxs) DeleteWithNoAction(txID string) {
	p.txs.Delete(txID)
}

func (p *PendingTxs) loadAndDelete(txID string) (*CompletionPromise, bool) {
	promise, loaded := p.txs.LoadAndDelete(txID)
	if !loaded {
		return nil, loaded
	}
	return promise.(*CompletionPromise), true
}

// DoneWithReceipt is called after the commit of a block.
// The `txIDs` slice must be in the same order that transactions appear in the block.
func (p *PendingTxs) DoneWithReceipt(txIDs []string, blockHeader *types.BlockHeader) {
	p.logger.Debugf("Done with receipt, block number: %d; txIDs: %v", blockHeader.GetBaseHeader().GetNumber(), txIDs)

	for txIndex, txID := range txIDs {
		promise, loaded := p.loadAndDelete(txID)
		if !loaded {
			continue
		}
		promise.done(
			&types.TxReceipt{
				Header:  blockHeader,
				TxIndex: uint64(txIndex),
			},
		)
	}
}

// ReleaseWithError is called when block replication fails with an error, typically NotLeaderError.
// This may come from the block replicator or the block creator.
// The `txIDs` slice does not have to be in the same order that transactions appear in the block.
func (p *PendingTxs) ReleaseWithError(txIDs []string, err error) {
	p.logger.Debugf("Release with error: %s; txIDs: %v", err, txIDs)

	for _, txID := range txIDs {
		promise, loaded := p.loadAndDelete(txID)
		if !loaded {
			continue
		}
		promise.error(err)
	}
}

func (p *PendingTxs) Has(txID string) bool {
	_, ok := p.txs.Load(txID)
	return ok
}

func (p *PendingTxs) Empty() bool {
	empty := true
	p.txs.Range(func(key, value interface{}) bool {
		empty = false
		return false
	})
	return empty
}
