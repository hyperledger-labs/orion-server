// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package logger

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type SugarLogger struct {
	*zap.SugaredLogger
	conf zap.Config
}

type Config struct {
	Level         string
	OutputPath    []string
	ErrOutputPath []string
	Encoding      string
	Name          string
}

func New(c *Config, opts ...zap.Option) (*SugarLogger, error) {
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

			EncodeDuration: zapcore.StringDurationEncoder,
		},
	}

	if len(c.Name) > 0 {
		logCfg.EncoderConfig.NameKey = "logger"
	}

	l, err := logCfg.Build()
	if len(opts) > 0 {
		l = l.WithOptions(opts...)
	}

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

func (l *SugarLogger) IsDebug() bool {
	// Level is atomic so no need for locks
	return l.conf.Level.Enabled(zap.DebugLevel)
}

// SetLogLevel is used only for tests
func (l *SugarLogger) SetLogLevel(level string) error {
	logLevel, err := getZapLogLevel(level)
	if err != nil {
		return err
	}

	// Level is atomic so no need for locks
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
