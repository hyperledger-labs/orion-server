// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm

import (
	"github.com/hyperledger-labs/orion-server/pkg/logger"
)

type LogAdapter struct {
	SugarLogger *logger.SugarLogger
	Debug       bool
}

func (l *LogAdapter) Write(p []byte) (n int, err error) {
	if l.Debug {
		l.SugarLogger.Debug(string(p))
	} else {
		l.SugarLogger.Info(string(p))
	}

	return len(p), nil
}
