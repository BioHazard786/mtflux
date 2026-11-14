// Package logging builds the zap loggers used by mtflux.
package logging

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ParseLevel parses a LOG_LEVEL / TG_LOG_LEVEL value.
func ParseLevel(s string) (zapcore.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info", "":
		return zapcore.InfoLevel, nil
	case "warn", "warning":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	}
	return 0, fmt.Errorf("log level %q is not one of debug|info|warn|error", s)
}

// ValidFormat reports whether s is a supported LOG_FORMAT.
func ValidFormat(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "console", "json":
		return true
	}
	return false
}

// New builds a logger writing to stderr at the given level. Format "console"
// is one readable line per entry; "json" is one JSON object per entry.
//
// Below debug level, errors are logged as their message only: zap otherwise
// adds an "errorVerbose" field with the multi-line stack that gotd's errors
// carry, which is most of what makes the default output unreadable.
func New(level zapcore.Level, format string) *zap.Logger {
	enc := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		MessageKey:     "msg",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000"),
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
	}
	if level <= zapcore.DebugLevel {
		enc.CallerKey = "caller"
		enc.EncodeCaller = zapcore.ShortCallerEncoder
	}

	var encoder zapcore.Encoder
	if strings.EqualFold(strings.TrimSpace(format), "json") {
		enc.EncodeTime = zapcore.RFC3339NanoTimeEncoder
		encoder = zapcore.NewJSONEncoder(enc)
	} else {
		enc.EncodeLevel = zapcore.CapitalLevelEncoder
		enc.ConsoleSeparator = "  "
		encoder = zapcore.NewConsoleEncoder(enc)
	}

	var core zapcore.Core = zapcore.NewCore(encoder, zapcore.Lock(os.Stderr), level)
	opts := []zap.Option{}
	if level <= zapcore.DebugLevel {
		opts = append(opts, zap.AddCaller())
	} else {
		core = plainErrors{core}
	}
	return zap.New(core, opts...)
}

// plainErrors is a core that reduces error fields to their message.
type plainErrors struct{ zapcore.Core }

func (c plainErrors) With(fields []zapcore.Field) zapcore.Core {
	return plainErrors{c.Core.With(flatten(fields))}
}

func (c plainErrors) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c plainErrors) Write(e zapcore.Entry, fields []zapcore.Field) error {
	return c.Core.Write(e, flatten(fields))
}

func flatten(fields []zapcore.Field) []zapcore.Field {
	var out []zapcore.Field
	for i, f := range fields {
		if f.Type != zapcore.ErrorType {
			continue
		}
		err, ok := f.Interface.(error)
		if !ok || err == nil {
			continue
		}
		if out == nil {
			out = make([]zapcore.Field, len(fields))
			copy(out, fields)
		}
		out[i] = zap.String(f.Key, err.Error())
	}
	if out == nil {
		return fields
	}
	return out
}
