package cliobs

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stokaro/ptah/migration/migrator"
)

func TestEmitterJSONModeWritesParseableJSONLines(t *testing.T) {
	var out bytes.Buffer
	runtime, err := Start(context.Background(), Options{
		Command:   "test",
		LogFormat: "json",
		LogLevel:  "info",
		LogWriter: &out,
	})
	if err != nil {
		t.Fatalf("start observability: %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown observability: %v", err)
		}
	})

	emit := NewEmitter(&out, runtime)
	emit.Println("plain status")
	emit.Printf("version: %d\n", 42)

	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		var payload map[string]any
		if err := json.Unmarshal([]byte(line), &payload); err != nil {
			t.Fatalf("line is not JSON: %q: %v", line, err)
		}
		if payload["msg"] == "" {
			t.Fatalf("line missing slog msg: %q", line)
		}
		if payload["correlation_id"] == "" {
			t.Fatalf("line missing correlation_id: %q", line)
		}
	}
}

func TestJSONOutputWriterLogsSubprocessLines(t *testing.T) {
	var out bytes.Buffer
	runtime, err := Start(context.Background(), Options{
		Command:   "test",
		LogFormat: "json",
		LogLevel:  "info",
		LogWriter: &out,
	})
	if err != nil {
		t.Fatalf("start observability: %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown observability: %v", err)
		}
	})

	writer := NewOutputWriter(&out, runtime, "hook output")
	if _, err := writer.Write([]byte("line one\nline two\n")); err != nil {
		t.Fatalf("write hook output: %v", err)
	}

	var records int
	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		var payload map[string]any
		if err := json.Unmarshal([]byte(line), &payload); err != nil {
			t.Fatalf("line is not JSON: %q: %v", line, err)
		}
		if payload["msg"] != "hook output" {
			t.Fatalf("msg = %#v, want hook output", payload["msg"])
		}
		if payload["output"] == "" {
			t.Fatalf("output attr missing: %q", line)
		}
		records++
	}
	if records != 2 {
		t.Fatalf("records = %d, want 2", records)
	}
}

func TestJSONOutputWriterFlushesPartialLine(t *testing.T) {
	var out bytes.Buffer
	runtime, err := Start(context.Background(), Options{
		Command:   "test",
		LogFormat: "json",
		LogLevel:  "info",
		LogWriter: &out,
	})
	if err != nil {
		t.Fatalf("start observability: %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown observability: %v", err)
		}
	})

	writer := NewOutputWriter(&out, runtime, "hook output")
	if _, err := writer.Write([]byte("partial line")); err != nil {
		t.Fatalf("write hook output: %v", err)
	}
	flusher, ok := writer.(interface{ Flush() })
	if !ok {
		t.Fatalf("writer does not support Flush")
	}
	flusher.Flush()

	var payload map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &payload); err != nil {
		t.Fatalf("line is not JSON: %q: %v", out.String(), err)
	}
	if payload["output"] != "partial line" {
		t.Fatalf("output = %#v, want partial line", payload["output"])
	}
}

func TestMetricsDataRendersPrometheusCountersAndHistograms(t *testing.T) {
	data := &metricsData{
		counters:   make(map[string]*counterSeries),
		histograms: make(map[string]*histogramSeries),
	}
	attrs := []migrator.ObservationAttribute{
		{Key: "db.system", Value: "postgres"},
		{Key: "migration.direction", Value: "up"},
	}
	data.addCounter("ptah_migrations_applied_total", 1, attrs)
	data.recordDuration("ptah_migration_lock_wait_seconds", (250 * time.Millisecond).Seconds(), attrs)

	recorder := httptest.NewRecorder()
	data.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))

	body := recorder.Body.String()
	if !strings.Contains(body, "# TYPE ptah_migrations_applied_total counter") {
		t.Fatalf("counter type missing:\n%s", body)
	}
	if !strings.Contains(body, `ptah_migrations_applied_total{db_system="postgres",migration_direction="up"} 1`) {
		t.Fatalf("counter sample missing:\n%s", body)
	}
	if !strings.Contains(body, "# TYPE ptah_migration_lock_wait_seconds histogram") {
		t.Fatalf("histogram type missing:\n%s", body)
	}
	if !strings.Contains(body, `ptah_migration_lock_wait_seconds_count{db_system="postgres",migration_direction="up"} 1`) {
		t.Fatalf("histogram count missing:\n%s", body)
	}
}

func TestMetricsDataWritesTypeOncePerMetricFamily(t *testing.T) {
	data := &metricsData{
		counters:   make(map[string]*counterSeries),
		histograms: make(map[string]*histogramSeries),
	}
	firstAttrs := []migrator.ObservationAttribute{
		{Key: "db.system", Value: "postgres"},
		{Key: "migration.direction", Value: "up"},
		{Key: "migration.version", Value: 1},
	}
	secondAttrs := []migrator.ObservationAttribute{
		{Key: "db.system", Value: "postgres"},
		{Key: "migration.direction", Value: "up"},
		{Key: "migration.version", Value: 2},
	}
	data.addCounter("ptah_migrations_applied_total", 1, firstAttrs)
	data.addCounter("ptah_migrations_applied_total", 1, secondAttrs)
	data.recordDuration("ptah_migration_duration_seconds", (10 * time.Millisecond).Seconds(), firstAttrs)
	data.recordDuration("ptah_migration_duration_seconds", (20 * time.Millisecond).Seconds(), secondAttrs)

	recorder := httptest.NewRecorder()
	data.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))

	body := recorder.Body.String()
	if count := strings.Count(body, "# TYPE ptah_migrations_applied_total counter"); count != 1 {
		t.Fatalf("counter TYPE count = %d, want 1:\n%s", count, body)
	}
	if count := strings.Count(body, "# TYPE ptah_migration_duration_seconds histogram"); count != 1 {
		t.Fatalf("histogram TYPE count = %d, want 1:\n%s", count, body)
	}
}

func TestStartRejectsInvalidLogOptions(t *testing.T) {
	if _, err := Start(context.Background(), Options{LogFormat: "xml"}); err == nil {
		t.Fatalf("invalid log format was accepted")
	}
	if _, err := Start(context.Background(), Options{LogLevel: "trace"}); err == nil {
		t.Fatalf("invalid log level was accepted")
	}
}
