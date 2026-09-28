//go:build observability

package cliobs_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	qt "github.com/frankban/quicktest"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"ptah.run/internal/cli/cliobs"
	"ptah.run/migration/migrator"
)

// TestStartExportsMigrationSpansOverOTLPHTTP holds what the observability build
// promises and what the tracing page documents. With OTEL_EXPORTER_OTLP_ENDPOINT
// set, a migration command's spans reach an OTLP/HTTP receiver at /v1/traces.
// They carry the service name ptah.<command> and the instrumentation scope
// ptah.run, and they nest as the migrator nests them. They also keep their
// attributes, and a span that ended with an error has an error status.
//
// OTEL_SERVICE_NAME is set to show that Ptah's service name replaces it, and
// OTEL_RESOURCE_ATTRIBUTES to show that other resource attributes are kept.
func TestStartExportsMigrationSpansOverOTLPHTTP(t *testing.T) {
	c := qt.New(t)
	receiver := newOTLPReceiver(c, http.StatusOK)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", receiver.server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_SERVICE_NAME", "not-ptah")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=cliobs-test")

	runtime, err := cliobs.Start(t.Context(), cliobs.Options{Command: "migrations.up", LogWriter: io.Discard})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(runtime.Shutdown(context.Background()), qt.IsNil) })
	observer := runtime.Observer()
	ctx, up := observer.StartSpan(t.Context(), "ptah.migrate.up",
		migrator.ObservationAttribute{Key: "db.system", Value: "postgres"})
	_, apply := observer.StartSpan(ctx, "ptah.migrate.apply",
		migrator.ObservationAttribute{Key: "migration.version", Value: int64(7)},
		migrator.ObservationAttribute{Key: "migration.description", Value: "create users"})
	apply.End(errors.New(`relation "users" already exists`))
	up.End(nil)
	c.Assert(runtime.Shutdown(t.Context()), qt.IsNil)

	requests := receiver.requests()
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].method, qt.Equals, http.MethodPost)
	c.Assert(requests[0].path, qt.Equals, "/v1/traces")
	c.Assert(requests[0].contentType, qt.Equals, "application/x-protobuf")

	var export coltracepb.ExportTraceServiceRequest
	c.Assert(proto.Unmarshal(requests[0].body, &export), qt.IsNil)
	c.Assert(export.GetResourceSpans(), qt.HasLen, 1)
	resource := export.GetResourceSpans()[0]
	resourceAttributes := stringAttributes(resource.GetResource().GetAttributes())
	c.Assert(resourceAttributes["service.name"], qt.Equals, "ptah.migrations.up")
	c.Assert(resourceAttributes["deployment.environment"], qt.Equals, "cliobs-test")

	c.Assert(resource.GetScopeSpans(), qt.HasLen, 1)
	scope := resource.GetScopeSpans()[0]
	c.Assert(scope.GetScope().GetName(), qt.Equals, "ptah.run")
	c.Assert(scope.GetSpans(), qt.HasLen, 2)
	spans := spansByName(scope.GetSpans())
	c.Assert(spans, qt.HasLen, 2)

	upSpan := spans["ptah.migrate.up"]
	applySpan := spans["ptah.migrate.apply"]
	c.Assert(upSpan.GetParentSpanId(), qt.HasLen, 0)
	c.Assert(applySpan.GetTraceId(), qt.DeepEquals, upSpan.GetTraceId())
	c.Assert(applySpan.GetParentSpanId(), qt.DeepEquals, upSpan.GetSpanId())

	c.Assert(stringAttributes(upSpan.GetAttributes())["db.system"], qt.Equals, "postgres")
	c.Assert(upSpan.GetStatus().GetCode(), qt.Equals, tracepb.Status_STATUS_CODE_UNSET)

	c.Assert(intAttributes(applySpan.GetAttributes())["migration.version"], qt.Equals, int64(7))
	c.Assert(stringAttributes(applySpan.GetAttributes())["migration.description"], qt.Equals, "create users")
	c.Assert(applySpan.GetStatus().GetCode(), qt.Equals, tracepb.Status_STATUS_CODE_ERROR)
	c.Assert(applySpan.GetStatus().GetMessage(), qt.Equals, `relation "users" already exists`)
	c.Assert(eventNames(applySpan.GetEvents()), qt.DeepEquals, []string{"exception"})
}

// spansByName indexes exported spans by name. Two spans with one name collapse
// into one entry, which the caller's length check reports.
func spansByName(spans []*tracepb.Span) map[string]*tracepb.Span {
	byName := make(map[string]*tracepb.Span, len(spans))
	for _, span := range spans {
		byName[span.GetName()] = span
	}
	return byName
}

// stringAttributes reads attributes as strings. An attribute of another type
// reads as the empty string.
func stringAttributes(attributes []*commonpb.KeyValue) map[string]string {
	values := make(map[string]string, len(attributes))
	for _, attribute := range attributes {
		values[attribute.GetKey()] = attribute.GetValue().GetStringValue()
	}
	return values
}

// intAttributes reads attributes as integers. An attribute of another type
// reads as zero.
func intAttributes(attributes []*commonpb.KeyValue) map[string]int64 {
	values := make(map[string]int64, len(attributes))
	for _, attribute := range attributes {
		values[attribute.GetKey()] = attribute.GetValue().GetIntValue()
	}
	return values
}

func eventNames(events []*tracepb.Span_Event) []string {
	names := make([]string, 0, len(events))
	for _, event := range events {
		names = append(names, event.GetName())
	}
	return names
}
