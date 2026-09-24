package glog

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	loggerValue  atomic.Value // *zap.Logger
	sugaredValue atomic.Value // *zap.SugaredLogger
	atomicLevel  zap.AtomicLevel
)

func init() {
	_ = Init(DefaultConfig())
}

// Init 初始化全局 logger
// cfg: 配置对象，如果为 nil 则使用默认配置
func Init(cfg Config, opts ...zap.Option) error {
	cfg = NormalizeOptions(cfg)
	if err := ValidateOptions(cfg); err != nil {
		return err
	}
	if dir := filepath.Dir(cfg.Path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create log directory %s: %w", dir, err)
		}
	}
	atomicLevel = zap.NewAtomicLevelAt(parseLevel(cfg.Level))
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
	cores = append(cores, zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), zapcore.AddSync(loggerWriter), atomicLevel))
	if cfg.PrintConsole {
		cores = append(cores, zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout)), atomicLevel))
	}
	mulCore := zapcore.NewTee(cores...)

	zapOpts := []zap.Option{
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zap.ErrorLevel),
	}
	zapOpts = append(zapOpts, opts...)
	logger := zap.New(mulCore, zapOpts...)
	sugaredLogger := logger.Sugar()

	loggerValue.Store(logger)
	sugaredValue.Store(sugaredLogger)
	return nil
}

func callerLogger() *zap.Logger {
	if l := GetLogger(); l != nil {
		return l
	}
	return nil
}

func callerSugaredLogger() *zap.SugaredLogger {
	if l := callerLogger(); l != nil {
		return l.Sugar()
	}
	return nil
}

func With(fields ...zap.Field) *zap.Logger {
	logger := GetLogger()
	return logger.WithOptions(zap.AddCallerSkip(-1), zap.Fields(fields...))
}

// GetLogger 获取当前 logger
func GetLogger() *zap.Logger {
	if v := loggerValue.Load(); v != nil {
		if l, ok := v.(*zap.Logger); ok {
			return l
		}
	}
	return nil
}

// GetSugaredLogger 获取当前 sugared logger
func GetSugaredLogger() *zap.SugaredLogger {
	if v := sugaredValue.Load(); v != nil {
		if sl, ok := v.(*zap.SugaredLogger); ok {
			return sl
		}
	}
	return nil
}

// Stop 停止 logger，同步所有缓冲的日志
func Stop() error {
	var lastErr error
	if l := GetLogger(); l != nil {
		if err := l.Sync(); err != nil {
			lastErr = err
		}
	}
	if sl := GetSugaredLogger(); sl != nil {
		if err := sl.Sync(); err != nil {
			if lastErr == nil {
				lastErr = err
			}
		}
	}
	return lastErr
}

// SetLogLevel 设置日志级别
func SetLogLevel(logLevel zapcore.Level) {
	atomicLevel.SetLevel(logLevel)
}

// GetLevel 获取当前日志级别
func GetLevel() zapcore.Level {
	return atomicLevel.Level()
}

// Debug 输出 Debug 级别日志
func Debug(msg string, fields ...zap.Field) {
	if l := callerLogger(); l != nil {
		l.Debug(msg, fields...)
	}
}

// Info 输出 Info 级别日志
func Info(msg string, fields ...zap.Field) {
	if l := callerLogger(); l != nil {
		l.Info(msg, fields...)
	}
}

// Warn 输出 Warn 级别日志
func Warn(msg string, fields ...zap.Field) {
	if l := callerLogger(); l != nil {
		l.Warn(msg, fields...)
	}
}

// Error 输出 Error 级别日志
func Error(msg string, fields ...zap.Field) {
	if l := callerLogger(); l != nil {
		l.Error(msg, fields...)
	}
}

// Panic 输出 Panic 级别日志并触发 panic
func Panic(msg string, fields ...zap.Field) {
	if l := callerLogger(); l != nil {
		l.Panic(msg, fields...)
	} else {
		// 如果 logger 未初始化，仍然触发 panic
		panic(msg)
	}
}

// Fatal 输出 Fatal 级别日志并退出程序
func Fatal(msg string, fields ...zap.Field) {
	if l := callerLogger(); l != nil {
		l.Fatal(msg, fields...)
	} else {
		// 如果 logger 未初始化，仍然退出程序
		os.Exit(1)
	}
}

// Debugf 使用格式化字符串输出 Debug 级别日志
func Debugf(template string, args ...interface{}) {
	if sl := callerSugaredLogger(); sl != nil {
		sl.Debugf(template, args...)
	}
}

// Infof 使用格式化字符串输出 Info 级别日志
func Infof(template string, args ...interface{}) {
	if sl := callerSugaredLogger(); sl != nil {
		sl.Infof(template, args...)
	}
}

// Warnf 使用格式化字符串输出 Warn 级别日志
func Warnf(template string, args ...interface{}) {
	if sl := callerSugaredLogger(); sl != nil {
		sl.Warnf(template, args...)
	}
}

// Errorf 使用格式化字符串输出 Error 级别日志
func Errorf(template string, args ...interface{}) {
	if sl := callerSugaredLogger(); sl != nil {
		sl.Errorf(template, args...)
	}
}

// DPanicf 使用格式化字符串输出 DPanic 级别日志
func DPanicf(template string, args ...interface{}) {
	if sl := callerSugaredLogger(); sl != nil {
		sl.DPanicf(template, args...)
	}
}

// Panicf 使用格式化字符串输出 Panic 级别日志并触发 panic
func Panicf(template string, args ...interface{}) {
	if sl := callerSugaredLogger(); sl != nil {
		sl.Panicf(template, args...)
	} else {
		// 如果 logger 未初始化，仍然触发 panic
		panic(fmt.Sprintf(template, args...))
	}
}

// Fatalf 使用格式化字符串输出 Fatal 级别日志并退出程序
func Fatalf(template string, args ...interface{}) {
	if sl := callerSugaredLogger(); sl != nil {
		sl.Fatalf(template, args...)
	} else {
		// 如果 logger 未初始化，仍然退出程序
		panic(fmt.Sprintf(template, args...))
	}
}

type Logger struct{}

func (Logger) Debugf(format string, args ...any) { Debugf(format, args...) }
func (Logger) Infof(format string, args ...any)  { Infof(format, args...) }
func (Logger) Warnf(format string, args ...any)  { Warnf(format, args...) }
func (Logger) Errorf(format string, args ...any) { Errorf(format, args...) }
