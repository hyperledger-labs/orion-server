// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package logger

import (
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type SugarLogger struct {
	*zap.SugaredLogger
	conf  zap.Config
	mutex sync.RWMutex
}

type Config struct {
	Level         string
	OutputPath    []string
	ErrOutputPath []string
	Encoding      string
	Name          string
}

func New(c *Config) (*SugarLogger, error) {
	logLevel, err := getZapLogLevel(c.Level)
	if err != nil {
		return nil, err
	}

	logCfg := zap.Config{
		Encoding:         c.Encoding,
		Level:            zap.NewAtomicLevelAt(logLevel),
		OutputPaths:      c.OutputPath,
		ErrorOutputPaths: c.ErrOutputPath,
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey: "message",

			LevelKey:    "level",
			EncodeLevel: zapcore.CapitalLevelEncoder,

			TimeKey:    "time",
			EncodeTime: zapcore.ISO8601TimeEncoder,

			CallerKey:    "caller",
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}

	if len(c.Name) > 0 {
		logCfg.EncoderConfig.NameKey = "logger"
	}

	l, err := logCfg.Build()

	if err != nil {
		return nil, errors.Wrap(err, "error while creating a logger")
	}

	return &SugarLogger{
		SugaredLogger: l.Named(c.Name).Sugar(),
		conf:          logCfg,
	}, nil
}

func (l *SugarLogger) With(args ...interface{}) *SugarLogger {
	return &SugarLogger{
		SugaredLogger: l.SugaredLogger.With(args...),
	}
}

func (l *SugarLogger) SetLogLevel(level string) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	logLevel, err := getZapLogLevel(level)
	if err != nil {
		return err
	}

	l.conf.Level.SetLevel(logLevel)

	return nil
}

func getZapLogLevel(level string) (zapcore.Level, error) {
	var logLevel zapcore.Level

	switch level {
	case "debug":
		logLevel = zapcore.DebugLevel
	case "info":
		logLevel = zapcore.InfoLevel
	case "warn":
		logLevel = zapcore.WarnLevel
	case "err":
		logLevel = zapcore.ErrorLevel
	case "panic":
		logLevel = zapcore.PanicLevel
	default:
		return zapcore.InfoLevel, errors.Errorf(
			"unrecognized log level [%s]. Only debug, info, warn, error, and panic log levels are supported",
			level,
		)
	}

	return logLevel, nil
}

func (l *SugarLogger) Warning(v ...interface{}) {
	l.Warn(v...)
}

func (l *SugarLogger) Warningf(format string, v ...interface{}) {
	l.Warnf(format, v...)
}
