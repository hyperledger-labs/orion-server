// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"sync"

	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

type PendingTxs struct {
	txs map[string]*CompletionPromise
	sync.RWMutex
}

func NewPendingTxs() *PendingTxs {
	return &PendingTxs{
		txs: make(map[string]*CompletionPromise),
	}
}

func (p *PendingTxs) Add(txID string, promise *CompletionPromise) {
	p.Lock()
	defer p.Unlock()

	p.txs[txID] = promise
}

// DoneWithReceipt is called after the commit of a block.
// The `txIDs` slice must be in the same order that transactions appear in the block.
func (p *PendingTxs) DoneWithReceipt(txIDs []string, blockHeader *types.BlockHeader) {
	p.Lock()
	defer p.Unlock()

	for txIndex, txID := range txIDs {
		p.txs[txID].done(
			&types.TxReceipt{
				Header:  blockHeader,
				TxIndex: uint64(txIndex),
			},
		)

		delete(p.txs, txID)
	}
}

func (p *PendingTxs) Has(txID string) bool {
	p.RLock()
	defer p.RUnlock()

	_, ok := p.txs[txID]
	return ok
}

func (p *PendingTxs) Empty() bool {
	p.RLock()
	defer p.RUnlock()

	return len(p.txs) == 0
}
