// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package logger

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestLogger(t *testing.T) {
	t.Parallel()

	logStatements := func(l *SugarLogger) {
		l.Debug("debug message is logged")
		l.Info("info message is logged")
		l.Warn("warning message is logged")
		l.Error("error message is logged")
	}

	tests := []struct {
		level               string
		fileName            string
		expectedMessages    []string
		notExpectedMessages []string
	}{
		{
			level:    "debug",
			fileName: "debug.txt",
			expectedMessages: []string{
				"debug message is logged",
				"info message is logged",
				"warning message is logged",
				"error message is logged",
			},
			notExpectedMessages: []string{},
		},
		{
			level:    "info",
			fileName: "info.txt",
			expectedMessages: []string{
				"info message is logged",
				"warning message is logged",
				"error message is logged",
			},
			notExpectedMessages: []string{
				"debug message is logged",
			},
		},
		{
			level:    "warn",
			fileName: "warn.txt",
			expectedMessages: []string{
				"warning message is logged",
				"error message is logged",
			},
			notExpectedMessages: []string{
				"debug message is logged",
				"info message is logged",
			},
		},
		{
			level:    "err",
			fileName: "err.txt",
			expectedMessages: []string{
				"error message is logged",
			},
			notExpectedMessages: []string{
				"debug message is logged",
				"info message is logged",
				"warning message is logged",
			},
		},
		{
			level:            "panic",
			fileName:         "panic.txt",
			expectedMessages: []string{},
			notExpectedMessages: []string{
				"debug message is logged",
				"info message is logged",
				"warning message is logged",
				"error message is logged",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.level, func(t *testing.T) {
			t.Parallel()

			testDir := t.TempDir()

			logFile := path.Join(testDir, tt.fileName)

			l, err := New(&Config{
				Level:         tt.level,
				OutputPath:    []string{logFile},
				ErrOutputPath: []string{logFile},
				Encoding:      "console",
			})
			require.NoError(t, err)

			logStatements(l)
			require.NoError(t, l.Sync())

			content, err := ioutil.ReadFile(logFile)
			require.NoError(t, err)

			for _, expected := range tt.expectedMessages {
				require.Contains(t, string(content), expected)
			}

			for _, unexpected := range tt.notExpectedMessages {
				require.NotContains(t, string(content), unexpected)
			}
		})
	}
}

func TestDynamicLogger(t *testing.T) {
	t.Parallel()

	logStatements := func(l *SugarLogger) {
		l.Debug("debug message is logged")
		l.Info("info message is logged")
		l.Warn("warning message is logged")
		l.Error("error message is logged")
	}

	tests := []struct {
		level                     string
		fileName                  string
		expectedMessagesBefore    []string
		notExpectedMessagesBefore []string
		newLevel                  string
		expectedMessagesAfter     []string
		notExpectedMessagesAfter  []string
	}{
		{
			level:    "debug",
			fileName: "dynamic-debug.txt",
			expectedMessagesBefore: []string{
				"debug message is logged",
				"info message is logged",
				"warning message is logged",
				"error message is logged",
			},
			notExpectedMessagesBefore: []string{},
			newLevel:                  "err",
			expectedMessagesAfter: []string{
				"error message is logged",
			},
			notExpectedMessagesAfter: []string{
				"debug message is logged",
				"info message is logged",
				"warning message is logged",
			},
		},
		{
			level:                  "panic",
			fileName:               "dynamic-error.txt",
			expectedMessagesBefore: []string{},
			notExpectedMessagesBefore: []string{
				"debug message is logged",
				"info message is logged",
				"warning message is logged",
				"error message is logged",
			},
			newLevel: "debug",
			expectedMessagesAfter: []string{
				"debug message is logged",
				"info message is logged",
				"warning message is logged",
				"error message is logged",
			},
			notExpectedMessagesAfter: []string{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.level, func(t *testing.T) {
			t.Parallel()

			testDir := t.TempDir()

			logFile := path.Join(testDir, tt.fileName)

			l, err := New(&Config{
				Level:         tt.level,
				OutputPath:    []string{logFile},
				ErrOutputPath: []string{logFile},
				Encoding:      "console",
			})
			require.NoError(t, err)

			logStatements(l)
			require.NoError(t, l.Sync())

			content, err := ioutil.ReadFile(logFile)
			require.NoError(t, err)

			for _, expected := range tt.expectedMessagesBefore {
				require.Contains(t, string(content), expected)
			}

			for _, unexpected := range tt.notExpectedMessagesBefore {
				require.NotContains(t, string(content), unexpected)
			}
			require.NoError(t, os.Truncate(logFile, 0))

			require.NoError(t, l.SetLogLevel(tt.newLevel))
			level, _ := getZapLogLevel(tt.newLevel)
			require.True(t, l.conf.Level.Enabled(level))

			logStatements(l)
			require.NoError(t, l.Sync())

			content, err = ioutil.ReadFile(logFile)
			require.NoError(t, err)

			for _, expected := range tt.expectedMessagesAfter {
				require.Contains(t, string(content), expected)
			}

			for _, unexpected := range tt.notExpectedMessagesAfter {
				require.NotContains(t, string(content), unexpected)
			}
		})
	}
}

func TestGetZapLogLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		level            string
		expectedZapLevel zapcore.Level
	}{
		{
			level:            "debug",
			expectedZapLevel: zapcore.DebugLevel,
		},
		{
			level:            "info",
			expectedZapLevel: zapcore.InfoLevel,
		},
		{
			level:            "warn",
			expectedZapLevel: zapcore.WarnLevel,
		},
		{
			level:            "err",
			expectedZapLevel: zapcore.ErrorLevel,
		},
		{
			level:            "panic",
			expectedZapLevel: zapcore.PanicLevel,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.level, func(t *testing.T) {
			t.Parallel()

			level, err := getZapLogLevel(tt.level)
			require.NoError(t, err)
			require.Equal(t, tt.expectedZapLevel, level)
		})
	}
}

func TestErrorPath(t *testing.T) {
	t.Parallel()

	t.Run("error in getZapLogLevel()", func(t *testing.T) {
		_, err := getZapLogLevel("unknown")
		require.EqualError(t, err, "unrecognized log level [unknown]. Only debug, info, warn, error, and panic log levels are supported")
	})

	t.Run("error in New() fron getZapLogLevel()", func(t *testing.T) {
		l, err := New(
			&Config{
				Level: "unknown",
			},
		)
		require.EqualError(t, err, "unrecognized log level [unknown]. Only debug, info, warn, error, and panic log levels are supported")
		require.Nil(t, l)
	})

	t.Run("error in New() from Build()", func(t *testing.T) {
		l, err := New(
			&Config{
				Level: "debug",
			},
		)
		require.EqualError(t, err, "error while creating a logger: no encoder name specified")
		require.Nil(t, l)
	})
}

func TestLoggerWith(t *testing.T) {
	t.Parallel()

	logStatements := func(l1 *SugarLogger) {
		l1.Debug("debug message is logged")
		l1.Info("info message is logged")
		l1.Warn("warning message is logged")
		l1.Error("error message is logged")
	}

	tests := []struct {
		level               string
		fileName            string
		expectedMessages    []string
		notExpectedMessages []string
	}{
		{
			level:    "debug",
			fileName: "debug.txt",
			expectedMessages: []string{
				"debug message is logged\t{\"id\": \"node1\"}",
				"info message is logged\t{\"id\": \"node1\"}",
				"warning message is logged\t{\"id\": \"node1\"}",
				"error message is logged\t{\"id\": \"node1\"}",
			},
			notExpectedMessages: []string{},
		},
		{
			level:    "info",
			fileName: "info.txt",
			expectedMessages: []string{
				"info message is logged\t{\"id\": \"node1\"}",
				"warning message is logged\t{\"id\": \"node1\"}",
				"error message is logged\t{\"id\": \"node1\"}",
			},
			notExpectedMessages: []string{
				"debug message is logged\t{\"id\": \"node1\"}",
			},
		},
		{
			level:    "warn",
			fileName: "warn.txt",
			expectedMessages: []string{
				"warning message is logged\t{\"id\": \"node1\"}",
				"error message is logged\t{\"id\": \"node1\"}",
			},
			notExpectedMessages: []string{
				"debug message is logged\t{\"id\": \"node1\"}",
				"info message is logged\t{\"id\": \"node1\"}",
			},
		},
		{
			level:    "err",
			fileName: "err.txt",
			expectedMessages: []string{
				"error message is logged\t{\"id\": \"node1\"}",
			},
			notExpectedMessages: []string{
				"debug message is logged\t{\"id\": \"node1\"}",
				"info message is logged\t{\"id\": \"node1\"}",
				"warning message is logged\t{\"id\": \"node1\"}",
			},
		},
		{
			level:            "panic",
			fileName:         "panic.txt",
			expectedMessages: []string{},
			notExpectedMessages: []string{
				"debug message is logged\t{\"id\": \"node1\"}",
				"info message is logged\t{\"id\": \"node1\"}",
				"warning message is logged\t{\"id\": \"node1\"}",
				"error message is logged\t{\"id\": \"node1\"}",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.level, func(t *testing.T) {
			t.Parallel()

			testDir := t.TempDir()

			logFile := path.Join(testDir, tt.fileName)
			l, err := New(&Config{
				Level:         tt.level,
				OutputPath:    []string{logFile},
				ErrOutputPath: []string{logFile},
				Encoding:      "console",
			})
			require.NoError(t, err)

			l1 := l.With("id", "node1")

			logStatements(l1)
			require.NoError(t, l1.Sync())

			content, err := ioutil.ReadFile(logFile)
			require.NoError(t, err)

			for _, expected := range tt.expectedMessages {
				require.Contains(t, string(content), expected)
			}

			for _, unexpected := range tt.notExpectedMessages {
				require.NotContains(t, string(content), unexpected)
			}
		})
	}
}

func TestSugarLogger_Hooks(t *testing.T) {
	logStatements := func(l1 *SugarLogger) {
		l1.Debug("debug bing is logged")
		l1.Info("info bing is logged")
		l1.Warn("warning bing is logged")
		l1.Error("error bing is logged")

		l1.Debug("debug bang is logged")
		l1.Info("info bang is logged")
		l1.Warn("warning bang is logged")
		l1.Error("error bang is logged")
	}

	tests := []struct {
		level         string
		expectedCount int
	}{
		{
			level:         "debug",
			expectedCount: 4,
		},
		{
			level:         "info",
			expectedCount: 3,
		},

		{
			level:         "warn",
			expectedCount: 2,
		},

		{
			level:         "err",
			expectedCount: 1,
		},
		{
			level:         "panic",
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run("hook-"+tt.level, func(t *testing.T) {
			var count int
			bangCountingHook := func(entry zapcore.Entry) error {
				if strings.Contains(entry.Message, "bang") {
					count++
				}
				return nil
			}

			l, err := New(
				&Config{
					Level:         tt.level,
					OutputPath:    []string{"stdout"},
					ErrOutputPath: []string{"stderr"},
					Encoding:      "console",
				},
				zap.Hooks(bangCountingHook),
			)
			require.NoError(t, err)

			logStatements(l)

			require.Equal(t, tt.expectedCount, count)

		})
	}
}
