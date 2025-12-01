// logging 包提供 zap 日志器的初始化辅助方法。
package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New 创建一个 zap SugaredLogger，可配置级别、格式以及可选的文件输出。
func New(level string, format string, file string) (*zap.SugaredLogger, func(), error) {
	var cfg zap.Config
	if format == "json" {
		cfg = zap.NewProductionConfig()
	} else {
		cfg = zap.NewDevelopmentConfig()
		cfg.Encoding = "console"
	}
	switch level {
	case "debug":
		cfg.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	case "info":
		cfg.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	case "warn":
		cfg.Level = zap.NewAtomicLevelAt(zapcore.WarnLevel)
	case "error":
		cfg.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	default:
		cfg.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}
	if file != "" {
		cfg.OutputPaths = []string{file}
		cfg.ErrorOutputPaths = []string{file}
	}
	lg, err := cfg.Build()
	if err != nil {
		return nil, func() {}, err
	}
	sugar := lg.Sugar()
	cleanup := func() { _ = lg.Sync() }
	return sugar, cleanup, nil
}
