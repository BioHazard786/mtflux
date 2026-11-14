package logging

import (
	"errors"
	"fmt"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// verboseErr mimics gotd's errors, which print a stack with %+v.
type verboseErr struct{}

func (verboseErr) Error() string { return "short" }
func (verboseErr) Format(s fmt.State, _ rune) {
	fmt.Fprint(s, "short\n  stack line 1\n  stack line 2")
}

func TestParseLevel(t *testing.T) {
	for in, want := range map[string]zapcore.Level{
		"debug": zapcore.DebugLevel, "": zapcore.InfoLevel, "INFO": zapcore.InfoLevel,
		" warn ": zapcore.WarnLevel, "warning": zapcore.WarnLevel, "error": zapcore.ErrorLevel,
	} {
		got, err := ParseLevel(in)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := ParseLevel("verbose"); err == nil {
		t.Error("ParseLevel(verbose) should fail")
	}
}

func TestPlainErrorsDropsVerboseStack(t *testing.T) {
	obs, logs := observer.New(zapcore.InfoLevel)
	log := zap.New(plainErrors{obs})

	log.With(zap.Error(verboseErr{})).Warn("with", zap.NamedError("cause", errors.New("boom")), zap.Int("n", 1))
	log.Debug("filtered out")

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["error"] != "short" || fields["cause"] != "boom" || fields["n"] != int64(1) {
		t.Errorf("unexpected fields: %#v", fields)
	}
	if _, ok := fields["errorVerbose"]; ok {
		t.Errorf("errorVerbose should be dropped: %#v", fields)
	}
}

func TestNewBuilds(t *testing.T) {
	for _, format := range []string{"console", "json"} {
		for _, lvl := range []zapcore.Level{zapcore.DebugLevel, zapcore.WarnLevel} {
			l := New(lvl, format)
			if l.Core().Enabled(zapcore.InfoLevel) != (lvl <= zapcore.InfoLevel) {
				t.Errorf("format %s level %v: wrong enablement", format, lvl)
			}
		}
	}
	if !ValidFormat("JSON") || ValidFormat("xml") {
		t.Error("ValidFormat mismatch")
	}
}
