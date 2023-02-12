// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type PendingTxs struct {
	txs    cmap.ConcurrentMap[string, *CompletionPromise]
	logger *logger.SugarLogger
}

func NewPendingTxs(logger *logger.SugarLogger) *PendingTxs {
	return &PendingTxs{
		txs:    cmap.New[*CompletionPromise](),
		logger: logger,
	}
}

// Add returns true if the txId was already taken
func (p *PendingTxs) Add(txID string, promise *CompletionPromise) bool {
	return !p.txs.SetIfAbsent(txID, promise)
}

func (p *PendingTxs) DeleteWithNoAction(txID string) {
	p.txs.Remove(txID)
}

func (p *PendingTxs) loadAndDelete(txID string) (*CompletionPromise, bool) {
	return p.txs.Pop(txID)
}

// DoneWithReceipt is called after the commit of a block.
// The `txIDs` slice must be in the same order that transactions appear in the block.
func (p *PendingTxs) DoneWithReceipt(txIDs []string, blockHeader *types.BlockHeader) {
	p.logger.Debugf("Done with receipt, block number: %d; txIDs: %v", blockHeader.GetBaseHeader().GetNumber(), txIDs)

	for txIndex, txID := range txIDs {
		if promise, loaded := p.loadAndDelete(txID); loaded {
			promise.done(
				&types.TxReceipt{
					Header:  blockHeader,
					TxIndex: uint64(txIndex),
				},
			)
		}
	}
}

// ReleaseWithError is called when block replication fails with an error, typically NotLeaderError.
// This may come from the block replicator or the block creator.
// The `txIDs` slice does not have to be in the same order that transactions appear in the block.
func (p *PendingTxs) ReleaseWithError(txIDs []string, err error) {
	p.logger.Debugf("Release with error: %s; txIDs: %v", err, txIDs)

	for _, txID := range txIDs {
		if promise, loaded := p.loadAndDelete(txID); loaded {
			promise.error(err)
		}
	}
}

// Has is used only for testing.
func (p *PendingTxs) Has(txID string) bool {
	return p.txs.Has(txID)
}

// Empty is used only for testing.
func (p *PendingTxs) Empty() bool {
	return p.txs.IsEmpty()
}
