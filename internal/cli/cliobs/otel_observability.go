//go:build observability

package cliobs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.opentelemetry.io/otel/trace"

	"ptah.run/migration/migrator"
)

// instrumentationName is the OpenTelemetry instrumentation-scope identity, not
// an import path -- it is emitted as otel.scope.name on every span this CLI
// produces. It tracked the module path before the move to ptah.run and
// deliberately still does, following the OTel convention that a scope is named
// after the instrumenting library. That makes the import-path move a BREAKING
// OBSERVABILITY CHANGE as well: a dashboard, alert or query filtering on the
// previous scope name goes blank rather than erroring, so it has to be updated
// alongside the import paths. Pin this to a literal instead if downstream
// dashboards must survive a future rename untouched.
const instrumentationName = "ptah.run"

// tracesEndpointVariables are the variables that say where traces go. Either
// one set on its own asks for tracing: the OpenTelemetry specification lets the
// traces-only variable stand alone, and the exporter reads both, the traces-only
// one taking precedence.
var tracesEndpointVariables = []string{
	"OTEL_EXPORTER_OTLP_ENDPOINT",
	"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
}

func startOTel(ctx context.Context, opts Options) (migrator.Observer, func(context.Context) error, error) {
	if !slices.ContainsFunc(tracesEndpointVariables, endpointSet) {
		return nil, nil, nil
	}
	otel.SetErrorHandler(warnHandler{})
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("initialize OTLP trace exporter: %w", err)
	}
	serviceName := "ptah"
	if opts.Command != "" {
		serviceName += "." + opts.Command
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		)),
	)
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	return otelObserver{tracer: provider.Tracer(instrumentationName)}, func(ctx context.Context) error {
		defer otel.SetTracerProvider(previousProvider)
		return provider.Shutdown(ctx)
	}, nil
}

// endpointSet reports whether an endpoint variable holds a value. A value of
// only spaces is unset, because the exporter trims a value and ignores an empty
// one: counting it as set would start an exporter that sends to its default,
// localhost:4318, which nobody asked for.
func endpointSet(name string) bool {
	return strings.TrimSpace(os.Getenv(name)) != ""
}

// warnHandler logs the errors the OpenTelemetry SDK reports -- a refused
// connection, a batch the receiver rejects -- at warn. Each one is a trace the
// run produced and the backend will not have. The SDK's own handler calls
// log.Print, which the command's slog default turns into an info record, so
// --log-level warn hid every such loss.
//
// It logs through slog.Default() at the time of the call, which during a
// command is the command's logger: the tracer provider flushes before Start's
// shutdown restores the previous default. It holds no state, so leaving it
// installed after the command is correct for the next one. It cannot be
// uninstalled anyway: the SDK hands its default handler to the first handler
// set, permanently.
type warnHandler struct{}

func (warnHandler) Handle(err error) {
	slog.Warn("OpenTelemetry tracing failed", "error", err)
}

type otelObserver struct {
	tracer trace.Tracer
}

func (o otelObserver) StartSpan(ctx context.Context, name string, attrs ...migrator.ObservationAttribute) (context.Context, migrator.ObservationSpan) {
	ctx, span := o.tracer.Start(ctx, name, trace.WithAttributes(otelAttrs(attrs)...))
	return ctx, otelSpan{span: span}
}

func (otelObserver) AddCounter(context.Context, string, int64, ...migrator.ObservationAttribute) {}

func (otelObserver) RecordDuration(context.Context, string, time.Duration, ...migrator.ObservationAttribute) {
}

type otelSpan struct {
	span trace.Span
}

func (s otelSpan) SetAttributes(attrs ...migrator.ObservationAttribute) {
	s.span.SetAttributes(otelAttrs(attrs)...)
}

func (s otelSpan) End(err error) {
	if err != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
	}
	s.span.End()
}

func otelAttrs(attrs []migrator.ObservationAttribute) []attribute.KeyValue {
	values := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		switch value := attr.Value.(type) {
		case string:
			values = append(values, attribute.String(attr.Key, value))
		case int:
			values = append(values, attribute.Int(attr.Key, value))
		case int64:
			values = append(values, attribute.Int64(attr.Key, value))
		case bool:
			values = append(values, attribute.Bool(attr.Key, value))
		case float64:
			values = append(values, attribute.Float64(attr.Key, value))
		default:
			values = append(values, attribute.String(attr.Key, fmt.Sprint(value)))
		}
	}
	return values
}
