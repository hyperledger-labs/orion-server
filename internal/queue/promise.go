// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"fmt"
	"time"

	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type CompletionPromise struct {
	resultCh chan interface{}
	timeout  time.Duration
}

func NewCompletionPromise(timeout time.Duration) *CompletionPromise {
	if timeout <= 0 {
		return nil
	}

	return &CompletionPromise{
		resultCh: make(chan interface{}, 1),
		timeout:  timeout,
	}
}

// Wait is called in order to wait for synchronous transaction completion, rejection, or timeout.
// When a transaction is committed, a `TxReceipt` is returned.
// When a transaction is rejected, an error is returned, for example `NotLeaderError`.
// When wait times out, `TimeoutErr` is returned.
func (s *CompletionPromise) Wait() (*types.TxReceipt, error) {
	if s == nil {
		return nil, nil
	}

	ticker := time.NewTicker(s.timeout)
	select {
	case <-ticker.C:
		return nil, &ierrors.TimeoutErr{ErrMsg: "timeout has occurred while waiting for the transaction receipt"}
	case r := <-s.resultCh:
		ticker.Stop()
		switch r.(type) {
		case error:
			return nil, r.(error)
		case *types.TxReceipt:
			return r.(*types.TxReceipt), nil
		case nil:
			return nil, &ierrors.ClosedError{ErrMsg: "closed: promise consumed"}
		default:
			panic(fmt.Sprintf("unexpected result type: %v", r))
		}
	}
}

// done is called after a transaction commits, and receives the `TxReceipt` as a parameter.
func (s *CompletionPromise) done(r *types.TxReceipt) {
	if s == nil {
		return
	}

	s.resultCh <- r
	close(s.resultCh)
}

// error is called when a transaction is rejected and cannot be committed.
func (s *CompletionPromise) error(err error) {
	if s == nil {
		return
	}

	s.resultCh <- err
	close(s.resultCh)
}
