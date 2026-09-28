//go:build observability

package cliobs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/cliobs"
)

// logRecord is the part of a JSON log record these tests read.
type logRecord struct {
	Level string `json:"level"`
	Msg   string `json:"msg"`
	Error string `json:"error"`
}

// runAtWarn runs one command span with tracing on and the log at --log-level
// warn, the level an operator runs with, and returns the records that reached
// the log. The endpoint variables are whatever the caller set.
func runAtWarn(c *qt.C) []logRecord {
	c.Helper()
	var out bytes.Buffer
	runtime, err := cliobs.Start(c.Context(), cliobs.Options{
		Command:   "migrations.up",
		LogFormat: "json",
		LogLevel:  "warn",
		LogWriter: &out,
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(runtime.Shutdown(context.Background()), qt.IsNil) })
	_, span := runtime.Observer().StartSpan(c.Context(), "ptah.migrate.up")
	span.End(nil)
	c.Assert(runtime.Shutdown(c.Context()), qt.IsNil)

	records := make([]logRecord, 0)
	for line := range strings.Lines(out.String()) {
		var record logRecord
		c.Assert(json.Unmarshal([]byte(line), &record), qt.IsNil, qt.Commentf("log line: %s", line))
		records = append(records, record)
	}
	return records
}

// closedEndpoint is the address of a receiver that has stopped listening, so an
// export to it is refused.
func closedEndpoint(c *qt.C) string {
	c.Helper()
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()
	return server.URL
}

// TestStartExportsToTheTracesEndpointAlone holds the variable the OpenTelemetry
// specification lets stand alone: with only OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
// set, the exporter starts and sends to that URL as written, not to a
// /v1/traces path below it.
func TestStartExportsToTheTracesEndpointAlone(t *testing.T) {
	c := qt.New(t)
	receiver := newOTLPReceiver(c, http.StatusOK)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", receiver.server.URL+"/collector/traces")

	c.Assert(runAtWarn(c), qt.HasLen, 0)

	requests := receiver.requests()
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].path, qt.Equals, "/collector/traces")
}

// TestStartTreatsABlankEndpointAsUnset holds the exporter's own reading of a
// value of only spaces: unset. Started anyway, the exporter would send to its
// default, localhost:4318, and a refusal there would reach the log.
func TestStartTreatsABlankEndpointAsUnset(t *testing.T) {
	c := qt.New(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "  ")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	c.Assert(runAtWarn(c), qt.HasLen, 0)
}

// TestStartLogsARefusedExportAtWarn holds a lost export to the level an
// operator runs with: the receiver refuses the connection, the run's spans are
// gone, and the log says so at warn, where --log-level warn keeps it. The
// migration itself is unaffected.
func TestStartLogsARefusedExportAtWarn(t *testing.T) {
	c := qt.New(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", closedEndpoint(c))
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	records := runAtWarn(c)

	c.Assert(records, qt.HasLen, 1)
	c.Assert(records[0].Level, qt.Equals, "WARN")
	c.Assert(records[0].Msg, qt.Equals, "OpenTelemetry tracing failed")
	c.Assert(records[0].Error, qt.Contains, "traces export: Post")
}

// TestStartLogsARejectedExportAtWarn is the same loss from a receiver that
// answers and refuses the batch.
func TestStartLogsARejectedExportAtWarn(t *testing.T) {
	c := qt.New(t)
	receiver := newOTLPReceiver(c, http.StatusBadRequest)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", receiver.server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	records := runAtWarn(c)

	c.Assert(records, qt.HasLen, 1)
	c.Assert(records[0].Level, qt.Equals, "WARN")
	c.Assert(records[0].Msg, qt.Equals, "OpenTelemetry tracing failed")
	c.Assert(records[0].Error, qt.Contains, "400 Bad Request")
	c.Assert(receiver.requests(), qt.HasLen, 1)
}

// TestStartLogsNothingAtWarnForADeliveredExport is the control for the two
// above: a handler that logged every export would satisfy them.
func TestStartLogsNothingAtWarnForADeliveredExport(t *testing.T) {
	c := qt.New(t)
	receiver := newOTLPReceiver(c, http.StatusOK)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", receiver.server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	c.Assert(runAtWarn(c), qt.HasLen, 0)
	c.Assert(receiver.requests(), qt.HasLen, 1)
}
