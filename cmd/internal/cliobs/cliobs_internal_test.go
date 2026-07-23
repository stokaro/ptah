package cliobs

// White-box testing required: metricsData is an internal Prometheus collector
// implementation that is only observable through unexported series maps and
// helper methods before the HTTP handler renders the final metrics text.

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/migrator"
)

func TestMetricsDataRendersPrometheusCountersAndHistograms(t *testing.T) {
	c := qt.New(t)

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
	c.Assert(body, qt.Contains, "# TYPE ptah_migrations_applied_total counter")
	c.Assert(body, qt.Contains, `ptah_migrations_applied_total{db_system="postgres",migration_direction="up"} 1`)
	c.Assert(body, qt.Contains, "# TYPE ptah_migration_lock_wait_seconds histogram")
	c.Assert(body, qt.Contains, `ptah_migration_lock_wait_seconds_count{db_system="postgres",migration_direction="up"} 1`)
}

func TestMetricsDataWritesTypeOncePerMetricFamily(t *testing.T) {
	c := qt.New(t)

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
	c.Assert(strings.Count(body, "# TYPE ptah_migrations_applied_total counter"), qt.Equals, 1)
	c.Assert(strings.Count(body, "# TYPE ptah_migration_duration_seconds histogram"), qt.Equals, 1)
}
