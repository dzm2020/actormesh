package glog

import (
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	log *Logger // *zap.Logger
)

func init() {
	log, _ = New(DefaultConfig())
}

func New(cfg Config, opts ...zap.Option) (*Logger, error) {
	cfg = NormalizeOptions(cfg)
	if err := ValidateOptions(cfg); err != nil {
		return nil, err
	}
	level := zap.NewAtomicLevelAt(parseLevel(cfg.Level))
	if dir := filepath.Dir(cfg.Path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create log directory %s: %w", dir, err)
		}
	}
	encoderConfig := zapcore.EncoderConfig{
		MessageKey:     "M",
		LevelKey:       "L",
		TimeKey:        "T",
		CallerKey:      "C",
		NameKey:        "N",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout("2006/01/02 15:04:05.000000Z0700"),
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	loggerWriter := &lumberjack.Logger{
		Filename:   cfg.Path,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAge,
		LocalTime:  cfg.LocalTime,
		Compress:   cfg.Compress,
	}

	cores := make([]zapcore.Core, 0, 2)
	cores = append(cores, zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), zapcore.AddSync(loggerWriter), level))
	if cfg.PrintConsole {
		cores = append(cores, zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout)), level))
	}
	mulCore := zapcore.NewTee(cores...)

	zapOpts := []zap.Option{
		zap.AddCallerSkip(1),
		zap.AddCaller(),
		zap.AddStacktrace(zap.ErrorLevel),
	}
	zapOpts = append(zapOpts, opts...)
	return &Logger{
		Logger:      zap.New(mulCore, zapOpts...),
		atomicLevel: level,
	}, nil
}

type Logger struct {
	*zap.Logger
	atomicLevel zap.AtomicLevel
}

// SetLevel 设置日志级别
func (l *Logger) SetLevel(logLevel zapcore.Level) {
	l.atomicLevel.SetLevel(logLevel)
}

func (l *Logger) With(fields ...zap.Field) *Logger {
	return &Logger{
		Logger:      l.Logger.With(fields...),
		atomicLevel: l.atomicLevel,
	}
}

func (l *Logger) Stop() error {
	var lastErr error
	if l.Logger != nil {
		if err := l.Logger.Sync(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func Log() *Logger {
	return log
}

// Debug 输出 Debug 级别日志
func Debug(msg string, fields ...zap.Field) {
	if l := log; l != nil {
		l.Debug(msg, fields...)
	}
}

// Info 输出 Info 级别日志
func Info(msg string, fields ...zap.Field) {
	if l := log; l != nil {
		l.Info(msg, fields...)
	}
}

// Warn 输出 Warn 级别日志
func Warn(msg string, fields ...zap.Field) {
	if l := log; l != nil {
		l.Warn(msg, fields...)
	}
}

// Error 输出 Error 级别日志
func Error(msg string, fields ...zap.Field) {
	if l := log; l != nil {
		l.Error(msg, fields...)
	}
}

// Panic 输出 Panic 级别日志并触发 panic
func Panic(msg string, fields ...zap.Field) {
	if l := log; l != nil {
		l.Panic(msg, fields...)
	} else {
		// 如果 logger 未初始化，仍然触发 panic
		panic(msg)
	}
}

// Fatal 输出 Fatal 级别日志并退出程序
func Fatal(msg string, fields ...zap.Field) {
	if l := log; l != nil {
		l.Fatal(msg, fields...)
	} else {
		// 如果 logger 未初始化，仍然退出程序
		os.Exit(1)
	}
}
