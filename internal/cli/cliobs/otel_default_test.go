//go:build !observability

package cliobs_test

import (
	"context"
	"io"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/cliobs"
)

// TestStartExportsNothingWithoutTheObservabilityTag is the control for the
// observability build tag. The default build links no trace exporter, so the
// endpoint variable alone sends nothing, and the command still starts and shuts
// down cleanly. TestStartExportsMigrationSpansOverOTLPHTTP, in the tagged
// build, proves the same receiver does take what an exporter sends.
func TestStartExportsNothingWithoutTheObservabilityTag(t *testing.T) {
	c := qt.New(t)
	receiver := newOTLPReceiver(c)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", receiver.server.URL)

	runtime, err := cliobs.Start(t.Context(), cliobs.Options{Command: "migrations.up", LogWriter: io.Discard})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(runtime.Shutdown(context.Background()), qt.IsNil) })
	_, span := runtime.Observer().StartSpan(t.Context(), "ptah.migrate.up")
	span.End(nil)
	c.Assert(runtime.Shutdown(t.Context()), qt.IsNil)

	c.Assert(receiver.requests(), qt.HasLen, 0)
}
