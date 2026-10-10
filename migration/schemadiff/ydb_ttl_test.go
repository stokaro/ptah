package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// eventsTable is the identity of the table the fixtures below declare.
var eventsTable = objectidentity.NewBuilder(identifier.ForDialect(platform.YDB)).TableParts("", "events")

// ydbTTLDeclaration declares a table whose TTL is policy, nil for none, from a
// source with the given knowledge of TTLs; nil leaves TTLs unenrolled, as an
// HCL document does.
func ydbTTLDeclaration(policy *ydbschema.TTL, knowledge *schemaext.Knowledge) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Event", Name: "events"}},
		Fields: []schemamodel.Field{
			{StructName: "Event", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Event", Name: "ts", Type: "TIMESTAMP", Nullable: true},
			{StructName: "Event", Name: "expires", Type: "BIGINT UNSIGNED", Nullable: true},
		},
	}
	if policy != nil {
		database.Tables[0].Facets = must.Must(must.Must(schemaext.NewFacets(&ydbschema.DesiredTTL{Policy: *policy})).WithTargetScope(ydbschema.TTLKind, platform.YDB))
	}
	if knowledge != nil {
		database.FeatureCoverage = must.Must(ydbschema.TTLCoverage(schemaext.Desired, *knowledge, nil))
	}
	return database
}

// completeTTL is the knowledge a source that can declare a TTL holds.
var completeTTL = &schemaext.Knowledge{State: schemaext.Complete}

// ydbTTLCatalog is the table as the YDB reader reports it, with the TTL read
// back as policy, nil for none, and complete TTL knowledge for the table.
func ydbTTLCatalog(policy *ydbschema.TTL) *catalog.Database {
	table := catalog.Table{Name: "events", Type: "TABLE", Columns: []catalog.Column{
		{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		{Name: "ts", DataType: "Timestamp64", ColumnType: "Timestamp64", IsNullable: "YES", OrdinalPosition: 2},
		{Name: "expires", DataType: "Uint64", ColumnType: "Uint64", IsNullable: "YES", OrdinalPosition: 3},
	}}
	if policy != nil {
		table.Facets = must.Must(must.Must(schemaext.NewFacets(&ydbschema.ObservedTTL{Policy: *policy})).WithTargetScope(ydbschema.TTLKind, platform.YDB))
	}
	ttl := must.Must(ydbschema.TTLCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were read"},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.TTLKind, Subject: eventsTable, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}))
	return &catalog.Database{
		FeatureCoverage: must.Must(ydbFixtureCoverageExceptTTL().Combine(ttl)),
		Tables:          []catalog.Table{table},
		Constraints: []catalog.Constraint{{Name: "events_pkey", TableName: "events", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}
}

// TestCompare_YDBTTL_HappyPath reads a declared TTL and the TTL YDB reads back
// as one when they delete the same rows on the same schedule, whatever the
// spelling of the interval, and plans nothing, against the database and
// against the same document alike.
func TestCompare_YDBTTL_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared *ydbschema.TTL
		read     *ydbschema.TTL
	}{
		{
			name:     "an interval in hours read back in days",
			declared: &ydbschema.TTL{Column: "ts", Interval: "PT720H"},
			read:     &ydbschema.TTL{Column: "ts", Interval: "P30D"},
		},
		{
			name:     "a week read back in days",
			declared: &ydbschema.TTL{Column: "ts", Interval: "P1W"},
			read:     &ydbschema.TTL{Column: "ts", Interval: "P7D"},
		},
		{
			name:     "an integer column",
			declared: &ydbschema.TTL{Column: "expires", Interval: "PT90M", Unit: "SECONDS"},
			read:     &ydbschema.TTL{Column: "expires", Interval: "PT1H30M", Unit: "SECONDS"},
		},
		{name: "no TTL on either side"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			against := must.Must(schemadiff.CompareWithDialect(t.Context(), ydbTTLDeclaration(test.declared, completeTTL), ydbTTLCatalog(test.read), platform.YDB, must.Must(builtin.New())))
			c.Assert(against.TablesModified, qt.HasLen, 0)
			itself := must.Must(schemadiff.CompareSchemas(t.Context(), ydbTTLDeclaration(test.declared, completeTTL), ydbTTLDeclaration(test.declared, completeTTL), platform.YDB, must.Must(builtin.New())))
			c.Assert(itself.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestCompare_YDBTTL_Changes reports a table whose TTL differs in any part as
// modified, with the owner's change carrying both sides, so the planner knows
// what to set or reset.
func TestCompare_YDBTTL_Changes(t *testing.T) {
	tests := []struct {
		name     string
		declared *ydbschema.TTL
		read     *ydbschema.TTL
	}{
		{name: "a TTL added", declared: &ydbschema.TTL{Column: "ts", Interval: "P30D"}},
		{name: "a TTL removed", read: &ydbschema.TTL{Column: "ts", Interval: "P30D"}},
		{name: "another interval", declared: &ydbschema.TTL{Column: "ts", Interval: "P31D"}, read: &ydbschema.TTL{Column: "ts", Interval: "P30D"}},
		{
			name:     "another column",
			declared: &ydbschema.TTL{Column: "expires", Interval: "P30D", Unit: "SECONDS"},
			read:     &ydbschema.TTL{Column: "ts", Interval: "P30D"},
		},
		{
			name:     "another unit",
			declared: &ydbschema.TTL{Column: "expires", Interval: "P30D", Unit: "MILLISECONDS"},
			read:     &ydbschema.TTL{Column: "expires", Interval: "P30D", Unit: "SECONDS"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), ydbTTLDeclaration(test.declared, completeTTL), ydbTTLCatalog(test.read), platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, 1)
			change, ok := diff.TablesModified[0].FeatureChanges[0].Value.(*ydbdiff.TTL)
			c.Assert(ok, qt.IsTrue)
			c.Assert(sides(change), qt.Equals, [2]ydbschema.TTL{policyOf(test.read), policyOf(test.declared)})
		})
	}
}

// sides is a change's observed and requested TTL, each the zero TTL for none.
func sides(change *ydbdiff.TTL) [2]ydbschema.TTL {
	var result [2]ydbschema.TTL
	if change.Before != nil {
		result[0] = change.Before.Policy
	}
	if change.After != nil {
		result[1] = change.After.Policy
	}
	return result
}

// policyOf is a policy, or the zero policy for none, so absence compares too.
func policyOf(policy *ydbschema.TTL) ydbschema.TTL {
	if policy == nil {
		return ydbschema.TTL{}
	}
	return *policy
}

// A desired state that cannot describe TTLs -- an HCL document, whose loader
// enrolls no TTL knowledge -- is silent about a table's TTL rather than asking
// for its removal, so nothing is planned for it, and the TTL is adopted into
// the effective declaration.
func TestCompare_YDBTTL_UndescribedIsNotRemoved(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), ydbTTLDeclaration(nil, nil),
		ydbTTLCatalog(&ydbschema.TTL{Column: "ts", Interval: "P30D"}), platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.TablesModified, qt.HasLen, 0)
}

// The control for the test above: a source that can describe TTLs and declares
// none asks for the TTL's removal, and so does a declaration of another TTL
// where TTLs are otherwise undescribed.
func TestCompare_YDBTTL_DescribedAbsenceIsARemoval(t *testing.T) {
	read := &ydbschema.TTL{Column: "ts", Interval: "P30D"}
	tests := []struct {
		name      string
		declared  *ydbschema.TTL
		knowledge *schemaext.Knowledge
	}{
		{name: "every TTL described", knowledge: completeTTL},
		{name: "a declared TTL where TTLs are undescribed", declared: &ydbschema.TTL{Column: "ts", Interval: "P1D"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), ydbTTLDeclaration(test.declared, test.knowledge), ydbTTLCatalog(read), platform.YDB, must.Must(builtin.New())))

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			change, ok := diff.TablesModified[0].FeatureChanges[0].Value.(*ydbdiff.TTL)
			c.Assert(ok, qt.IsTrue)
			c.Assert(sides(change), qt.Equals, [2]ydbschema.TTL{*read, policyOf(test.declared)})
		})
	}
}
