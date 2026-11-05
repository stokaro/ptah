package schemastats_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/schemastats"
)

// ownerMetricCounts answers every value with one count of one metric: the
// owner of an engine that holds indexes outside the common model counts them
// so. The domain declarations stand in for the values that hold them.
type ownerMetricCounts struct{ name string }

func (o ownerMetricCounts) ReportValues(_ context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	result := make([]schemaext.ValueReport, len(request.Values))
	for i, value := range request.Values {
		result[i] = schemaext.ValueReport{Kind: value.Kind(), Counts: []schemaext.MetricCount{{Name: o.name, Value: 2}}}
	}
	return result, nil
}

// ownerMetricRuntime is a runtime whose domain owner reports metric for each
// domain, counting two of it.
func ownerMetricRuntime(metric schemaext.MetricDefinition) schemaext.ReportingRuntime {
	return must.Must(engine.New(engine.Provider{ID: string(pgtype.DomainKind), Codecs: codecsOfKind(pgtype.DomainKind),
		Reporting: []engine.Reporting{{Representation: schemaext.Desired, Service: ownerMetricCounts{name: metric.Name},
			Definitions: []schemaext.ReportDefinition{{Kind: pgtype.DomainKind, DisplayName: "indexed values", Metrics: []schemaext.MetricDefinition{metric}}}}}}))
}

// ownerIndexesDatabase holds one common index and two values whose owner
// counts the indexes they hold.
func ownerIndexesDatabase() *schemamodel.Database {
	return &schemamodel.Database{
		Indexes: []schemamodel.Index{{Name: "by_name"}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			ownerType(pgtype.DomainKind, "app", "a", &pgtype.DesiredDomain{BaseType: "int4"}),
			ownerType(pgtype.DomainKind, "app", "b", &pgtype.DesiredDomain{BaseType: "int4"}),
		)),
	}
}

// An owner's count of the indexes its engine holds outside the common model,
// such as YDB's vector indexes, adds to the common count of indexes when it
// names the common metric with its help, and the scrape holds the metric once.
func TestCollect_OwnerIndexes_HappyPath(t *testing.T) {
	c := qt.New(t)

	stats, err := schemastats.Collect(c.Context(), ownerIndexesDatabase(), "", ownerMetricRuntime(schemaext.MetricDefinition{Name: "indexes", Help: "Indexes"}))

	c.Assert(err, qt.IsNil)
	var out strings.Builder
	c.Assert(schemastats.WriteOpenMetrics(&out, stats, nil), qt.IsNil)
	c.Assert(metricValue(c, out.String(), "ptah_schema_indexes"), qt.Equals, "5")
	c.Assert(strings.Count(out.String(), "# TYPE ptah_schema_indexes "), qt.Equals, 1)
}

// An owner's metric named like a common one is refused when it gives the
// indexes another help, which says it counts something else, and for any
// other common metric, such as the tables, whatever its help.
func TestCollect_OwnerIndexes_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		metric schemaext.MetricDefinition
	}{
		{"indexes with another help", schemaext.MetricDefinition{Name: "indexes", Help: "Indexes an owner holds"}},
		{"the tables", schemaext.MetricDefinition{Name: "tables", Help: "Tables"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stats, err := schemastats.Collect(c.Context(), ownerIndexesDatabase(), "", ownerMetricRuntime(test.metric))

			c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
			c.Assert(err, qt.ErrorMatches, `.*feature metric conflicts with "`+test.metric.Name+`"`)
			c.Assert(stats, qt.DeepEquals, schemastats.Stats{})
		})
	}
}
