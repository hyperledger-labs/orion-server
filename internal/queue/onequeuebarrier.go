// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"sync"

	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
)

// OneQueueBarrier is used to synchronize a single producing go-routine and a single consuming go-routine.
// The producing go-routine enqueues an entry and waits for its consumption to finish.
// The consuming go-routine dequeues an entry, consumes it, and releases the waiting producing go-routine by sending
// a reply.
// Both go-routines are released by closing the OneQueueBarrier.
type OneQueueBarrier struct {
	entryCh chan interface{} // An unbuffered entry channel
	replyCh chan interface{} // An unbuffered reply channel

	stopCh   chan struct{} // Closed in order to signal go-routines to exit
	stopOnce sync.Once     // close stopCh only once

	logger *logger.SugarLogger
}

// NewOneQueueBarrier creates a new OneQueueBarrier.
func NewOneQueueBarrier(logger *logger.SugarLogger) *OneQueueBarrier {
	qb := &OneQueueBarrier{
		entryCh: make(chan interface{}),
		replyCh: make(chan interface{}),
		stopCh:  make(chan struct{}),
		logger:  logger,
	}
	return qb
}

// EnqueueWait submits an entry for consumption and ways for a reply, indicating processing had finished.
// An error is returned if the OneQueueBarrier was closed.
func (qb *OneQueueBarrier) EnqueueWait(entry interface{}) (interface{}, error) {
	select {
	case <-qb.stopCh:
		qb.logger.Debug("stopped before enqueue")
		return nil, &ierrors.ClosedError{ErrMsg: "closed"}
	case qb.entryCh <- entry:
		qb.logger.Debug("Enqueued entry")
	}

	select {
	case <-qb.stopCh:
		qb.logger.Debug("stopped before reply")
		return nil, &ierrors.ClosedError{ErrMsg: "closed"}
	case reply := <-qb.replyCh:
		return reply, nil
	}
}

// Dequeue waits for an entry to be consumed.
// After consuming the entry is done, the producing go-routine must be release by invoking Reply().
// An error is returned if the OneQueueBarrier was closed.
func (qb *OneQueueBarrier) Dequeue() (interface{}, error) {
	select {
	case <-qb.stopCh:
		qb.logger.Debug("stopped before dequeue")
		return nil, &ierrors.ClosedError{ErrMsg: "closed"}
	case entry := <-qb.entryCh:
		return entry, nil
	}
}

// Reply sends a reply to the waiting producing go-routine, thus releasing it.
// The reply can be nil or an object.
// An error is returned if the OneQueueBarrier was closed.
func (qb *OneQueueBarrier) Reply(reply interface{}) error {
	select {
	case <-qb.stopCh:
		qb.logger.Debug("stopped before reply")
		return &ierrors.ClosedError{ErrMsg: "closed"}
	case qb.replyCh <- reply:
		qb.logger.Debug("Submitted reply")
		return nil
	}
}

// Close signals both waiting go-routines to exit and close respective channels.
func (qb *OneQueueBarrier) Close() (err error) {
	err = &ierrors.ClosedError{ErrMsg: "closed"}
	qb.stopOnce.Do(func() {
		qb.logger.Info("closing stop channel")
		close(qb.stopCh)
		err = nil
	})

	return err
}
