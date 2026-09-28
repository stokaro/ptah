//go:build !observability

package cliobs_test

import (
	"context"
	"io"
	"net/http"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/cliobs"
)

// TestStartExportsNothingWithoutTheObservabilityTag is the control for the
// observability build tag. The default build links no trace exporter, so neither
// endpoint variable sends anything, and the command still starts and shuts down
// cleanly. The tagged tests prove the same receiver takes what an exporter
// sends.
func TestStartExportsNothingWithoutTheObservabilityTag(t *testing.T) {
	c := qt.New(t)
	receiver := newOTLPReceiver(c, http.StatusOK)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", receiver.server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", receiver.server.URL+"/v1/traces")

	runtime, err := cliobs.Start(t.Context(), cliobs.Options{Command: "migrations.up", LogWriter: io.Discard})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(runtime.Shutdown(context.Background()), qt.IsNil) })
	_, span := runtime.Observer().StartSpan(t.Context(), "ptah.migrate.up")
	span.End(nil)
	c.Assert(runtime.Shutdown(t.Context()), qt.IsNil)

	c.Assert(receiver.requests(), qt.HasLen, 0)
}
