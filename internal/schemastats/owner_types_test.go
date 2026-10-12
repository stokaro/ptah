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

// TestCollect_CountsATypeWhoeverOwnsIt adds the type owner's count to the
// common metric of the same name: a metric counts its objects whoever owns
// them, and the scrape holds each type metric once. Two types of one name and
// different kinds, the common domain app.a and the owner's range type app.a,
// are two objects.
func TestCollect_CountsATypeWhoeverOwnsIt(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Domains:        []schemamodel.Domain{{Schema: "app", Name: "a", BaseType: "int4"}},
		CompositeTypes: []schemamodel.CompositeType{{Name: "c", Fields: []schemamodel.CompositeField{{Name: "f", Type: "int4"}}}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			ownerType(pgtype.DomainKind, "app", "b", &pgtype.DesiredDomain{BaseType: "int4"}),
			ownerType(pgtype.DomainKind, "", "c", &pgtype.DesiredDomain{BaseType: "int4"}),
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

// TestCollect_RefusesATypeCountedTwice refuses a type the common model holds
// and the type owner declares too, rather than counting it twice.
func TestCollect_RefusesATypeCountedTwice(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Domains:        []schemamodel.Domain{{Schema: "app", Name: "d", BaseType: "int4"}},
		FeatureObjects: must.Must(schemaext.NewObjects(ownerType(pgtype.DomainKind, "app", "d", &pgtype.DesiredDomain{BaseType: "int4"}))),
	}

	stats, err := schemastats.Collect(c.Context(), db, "postgres", must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(err, qt.ErrorMatches, `.*: domain "app.d" is counted both in the common model and as an owner's object`)
	c.Assert(stats, qt.DeepEquals, schemastats.Stats{})
}
