//go:build observability

package cliobs

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/stokaro/ptah/migration/migrator"
)

const instrumentationName = "github.com/stokaro/ptah"

func startOTel(ctx context.Context, opts Options) (migrator.Observer, func(context.Context) error, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return nil, nil, nil
	}
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
