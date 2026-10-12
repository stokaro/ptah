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

type sharedTypeCounts struct{}

func (sharedTypeCounts) ReportValues(_ context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	result := make([]schemaext.ValueReport, len(request.Values))
	for i, value := range request.Values {
		result[i] = schemaext.ValueReport{Kind: value.Kind(), Counts: []schemaext.MetricCount{{Name: "named_types", Value: 1}}}
	}
	return result, nil
}

func TestCollect_AggregatesSharedOwnerMetrics(t *testing.T) {
	c := qt.New(t)
	kinds := []schemaext.Kind{pgtype.DomainKind, pgtype.RangeKind}
	var providers []engine.Provider
	for _, kind := range kinds {
		providers = append(providers, engine.Provider{ID: string(kind), Codecs: codecsOfKind(kind),
			Reporting: []engine.Reporting{{Representation: schemaext.Desired, Service: sharedTypeCounts{},
				Definitions: []schemaext.ReportDefinition{{Kind: kind, DisplayName: "types", Metrics: []schemaext.MetricDefinition{{Name: "named_types", Help: "Named types"}}}}}}})
	}
	runtime := must.Must(engine.New(providers...))
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ownerType(pgtype.DomainKind, "app", "a", &pgtype.DesiredDomain{BaseType: "int4"}),
		ownerType(pgtype.DomainKind, "app", "b", &pgtype.DesiredDomain{BaseType: "int4"}),
		ownerType(pgtype.RangeKind, "app", "c", &pgtype.DesiredRange{Subtype: "int4"}),
	))}
	stats, err := schemastats.Collect(t.Context(), db, "", runtime)
	c.Assert(err, qt.IsNil)
	var out strings.Builder
	c.Assert(schemastats.WriteOpenMetrics(&out, stats, nil), qt.IsNil)
	c.Assert(metricValue(c, out.String(), "ptah_schema_named_types"), qt.Equals, "3")
	c.Assert(strings.Count(out.String(), "# TYPE ptah_schema_named_types "), qt.Equals, 1)
}

func codecsOfKind(kind schemaext.Kind) []schemaext.Codec {
	var result []schemaext.Codec
	for _, codec := range pgtype.Codecs() {
		if codec.Prototype.Kind() == kind {
			result = append(result, codec)
		}
	}
	return result
}
