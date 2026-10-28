package logger

import (
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func FxEvents(l *zap.Logger) fxevent.Logger {
	z := &fxevent.ZapLogger{Logger: l.Named("fx")}
	z.UseLogLevel(zapcore.DebugLevel)
	z.UseErrorLevel(zapcore.ErrorLevel)
	return z
}
