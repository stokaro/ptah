package cliobs_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/cliobs"
)

// SilenceDefaultLogger is what keeps library code that narrates through the
// package-level slog functions off the Atlas-compatible binary's streams
// (stokaro/ptah#967).
func TestSilenceDefaultLoggerDropsEveryLevel(t *testing.T) {
	c := qt.New(t)
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	cliobs.SilenceDefaultLogger()

	ctx := context.Background()
	levels := []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError}
	for _, level := range levels {
		c.Assert(slog.Default().Enabled(ctx, level), qt.IsFalse, qt.Commentf("level %s", level))
	}
}

// A command that starts its own observability runtime must still log: Start
// installs its logger over the silenced default and restores the silence on
// shutdown, so the two nest instead of fighting.
func TestSilenceDefaultLoggerNestsUnderStart(t *testing.T) {
	c := qt.New(t)
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	cliobs.SilenceDefaultLogger()
	silenced := slog.Default()

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
	c.Assert(slog.Default(), qt.Equals, silenced)
}
