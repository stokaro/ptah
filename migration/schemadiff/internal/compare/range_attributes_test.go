package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

func compareRanges(c *qt.C, desired *schemamodel.Database, observed *catalog.Database) *difftypes.SchemaDiff {
	c.Helper()
	desired.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Desired))
	observed.FeatureCoverage = must.Must(pgtype.CompleteCoverage(schemaext.Observed))
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, observed, "postgres", builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	return diff
}

// A modification carries both definitions and names every changed attribute.
func TestRanges_AttributeChangesAreReported(t *testing.T) {
	current := pgtype.ObservedRange{Subtype: "timestamp with time zone", SubtypeOpClass: "timestamptz_ops"}
	for _, test := range []struct {
		name   string
		target pgtype.DesiredRange
		key    string
	}{
		{name: "subtype", target: pgtype.DesiredRange{Subtype: "int8"}, key: pgtype.DiffSubtype},
		{name: "subtype_opclass", target: pgtype.DesiredRange{Subtype: "timestamptz", SubtypeOpClass: "custom_ops"}, key: pgtype.RangeSubtypeOpClass},
		{name: "collation", target: pgtype.DesiredRange{Subtype: "timestamptz", Collation: "en_US"}, key: pgtype.RangeCollation},
		{name: "canonical", target: pgtype.DesiredRange{Subtype: "timestamptz", Canonical: "audit_range_canonical"}, key: pgtype.RangeCanonical},
		{name: "subtype_diff", target: pgtype.DesiredRange{Subtype: "timestamptz", SubtypeDiff: "audit_range_diff"}, key: pgtype.RangeSubtypeDiff},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := pgtype.Ref(pgtype.RangeKind, "", "audit_range")
			desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &test.target}))}
			database := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &current}))}
			diff := compareRanges(c, desired, database)
			c.Assert(diff.FeatureChanges, qt.HasLen, 1)
			c.Assert(diff.FeatureChanges[0].Subject.Key(), qt.Equals, ref.Key())
			c.Assert(diff.FeatureChanges[0].Value, qt.DeepEquals, &pgtype.RangeChange{Before: &current, After: &test.target, Differs: []string{test.key}})
		})
	}
}

// A subtype alias and an undeclared catalog operator class are agreement.
func TestRanges_UnchangedRangeReportsNothing(t *testing.T) {
	c := qt.New(t)
	ref := pgtype.Ref(pgtype.RangeKind, "", "audit_range")
	desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &pgtype.DesiredRange{Subtype: "timestamptz"}}))}
	database := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &pgtype.ObservedRange{Subtype: "timestamp with time zone", SubtypeOpClass: "timestamptz_ops"}}))}
	diff := compareRanges(c, desired, database)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

func TestRanges_AddRemoveAndUnchangedAlias(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "", "fresh"), Value: &pgtype.DesiredRange{Subtype: "int4"}},
		schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "", "floatrange"), Value: &pgtype.DesiredRange{Subtype: "float8"}},
	))}
	database := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "", "legacy"), Value: &pgtype.ObservedRange{Subtype: "int4"}},
		schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "", "floatrange"), Value: &pgtype.ObservedRange{Subtype: "double precision"}},
	))}
	diff := compareRanges(c, desired, database)
	c.Assert(diff.FeatureChanges, qt.HasLen, 2)
	c.Assert(diff.FeatureChanges[0].Subject.Name.Source, qt.Equals, "fresh")
	c.Assert(diff.FeatureChanges[0].Value, qt.DeepEquals, &pgtype.RangeChange{After: &pgtype.DesiredRange{Subtype: "int4"}})
	c.Assert(diff.FeatureChanges[1].Subject.Name.Source, qt.Equals, "legacy")
	c.Assert(diff.FeatureChanges[1].Value, qt.DeepEquals, &pgtype.RangeChange{Before: &pgtype.ObservedRange{Subtype: "int4"}})
}
