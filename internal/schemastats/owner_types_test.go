package schemastats_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/schemastats"
)

// ownerType is the PostgreSQL type owner's declaration of kind named
// schema.name.
func ownerType(kind schemaext.Kind, schema, name string, value schemaext.Value) schemaext.Object {
	return must.Must(pgtype.DesiredObject(pgtype.Ref(kind, schema, name), value))
}

// TestCollect_CountsTheOwnersTypes adds the type owner's count to the type
// metric of the same name, which the scrape holds once. Two types of one name
// and different kinds, the domain app.a and the range type app.a, are two
// objects.
func TestCollect_CountsTheOwnersTypes(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ownerType(pgtype.DomainKind, "app", "a", &pgtype.DesiredDomain{BaseType: "int4"}),
			ownerType(pgtype.DomainKind, "app", "b", &pgtype.DesiredDomain{BaseType: "int4"}),
			ownerType(pgtype.DomainKind, "", "c", &pgtype.DesiredDomain{BaseType: "int4"}),
			ownerType(pgtype.CompositeKind, "", "c", &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "f", Type: "int4"}}}),
			ownerType(pgtype.RangeKind, "app", "a", &pgtype.DesiredRange{Subtype: "int4"}),
		)),
	}

	body := render(c, db, nil)

	c.Assert(metricValue(c, body, "ptah_schema_domains"), qt.Equals, "3")
	c.Assert(metricValue(c, body, "ptah_schema_composite_types"), qt.Equals, "1")
	c.Assert(metricValue(c, body, "ptah_schema_range_types"), qt.Equals, "1")
	c.Assert(strings.Count(body, "# TYPE ptah_schema_domains "), qt.Equals, 1)
	c.Assert(strings.Count(body, "# TYPE ptah_schema_range_types "), qt.Equals, 1)
}

// TestCollect_KeepsTheTypeMetricsAmongTheCommonOnes pins that the type
// metrics keep their place among the common metrics, between the sequences
// and the views, though the type owners count them.
func TestCollect_KeepsTheTypeMetricsAmongTheCommonOnes(t *testing.T) {
	c := qt.New(t)

	stats, err := schemastats.Collect(c.Context(), &schemamodel.Database{}, "postgres", must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(stats.Metrics))
	for _, metric := range stats.Metrics {
		names = append(names, metric.Name)
	}
	c.Assert(names[:12], qt.DeepEquals, []string{
		"schemas", "tables", "columns", "indexes", "constraints", "enums", "functions", "sequences",
		"domains", "composite_types", "views", "materialized_views",
	})
}
