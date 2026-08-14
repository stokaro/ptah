package cliobs_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/cliobs"
)

// QuietDefaultLogger keeps library narration off the Atlas-compatible binary's
// streams (stokaro/ptah#967) without dropping the diagnostics that have no
// other channel. The process-level contract is pinned by the subprocess tests
// in cmd/ptah-compat; this holds the level split those rest on.
func TestQuietDefaultLoggerDropsNarrationAndKeepsDiagnostics(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	cliobs.QuietDefaultLogger()

	ctx := context.Background()
	tests := []struct {
		name    string
		level   slog.Level
		escapes bool
	}{
		{name: "debug narration is dropped", level: slog.LevelDebug, escapes: false},
		{name: "info narration is dropped", level: slog.LevelInfo, escapes: false},
		{name: "warn diagnostics are kept", level: slog.LevelWarn, escapes: true},
		{name: "error diagnostics are kept", level: slog.LevelError, escapes: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(slog.Default().Enabled(ctx, tt.level), qt.Equals, tt.escapes)
		})
	}
}

// A command that starts its own observability runtime must still log at the
// level it asked for: Start installs its logger over the quiet default and
// restores the quiet default on shutdown, so the two nest instead of fighting.
func TestQuietDefaultLoggerNestsUnderStart(t *testing.T) {
	c := qt.New(t)
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	cliobs.QuietDefaultLogger()
	quiet := slog.Default()

	var out bytes.Buffer
	runtime, err := cliobs.Start(context.Background(), cliobs.Options{
		Command:   "test",
		LogFormat: "text",
		LogLevel:  "info",
		LogWriter: &out,
	})
	c.Assert(err, qt.IsNil)

	slog.Info("visible while the runtime owns the default logger")
	c.Assert(out.String(), qt.Contains, "visible while the runtime owns the default logger")

	c.Assert(runtime.Shutdown(context.Background()), qt.IsNil)
	c.Assert(slog.Default(), qt.Equals, quiet)
}
