package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// changefeedDeclaration declares table events with the given changefeeds.
func changefeedDeclaration(changefeeds ...ast.ChangefeedSpec) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Event", Name: "events", Changefeeds: changefeeds}},
		Fields: []schemamodel.Field{{StructName: "Event", Name: "id", Type: "BIGINT", Primary: true}},
	}
}

// changefeedCatalog is table events as the YDB reader reports it, with the
// given changefeeds.
func changefeedCatalog(changefeeds ...ast.ChangefeedSpec) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "events", Type: "TABLE", Changefeeds: changefeeds, Columns: []catalog.Column{
			{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		}}},
		Constraints: []catalog.Constraint{{Name: "events_pkey", TableName: "events", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}
}

// declared is a changefeed as a declaration states it, and read the same one
// as the reader reports it: the retention in its own spelling, and the
// consumer's start as the epoch YDB reports for none.
var (
	declared = ast.ChangefeedSpec{Name: "updates", Mode: "updates", Format: "json", RetentionPeriod: "PT720M",
		Consumers: []ast.TopicConsumerSpec{{Name: "audit", SupportedCodecs: []string{"GZIP", "raw"}}}}
	read = ast.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", RetentionPeriod: "PT12H",
		Consumers: []ast.TopicConsumerSpec{{Name: "audit", ReadFrom: "1970-01-01T00:00:00Z", SupportedCodecs: []string{"raw", "gzip"}}}}
)

// TestCompare_YDBChangefeedReadsBackAsDeclared holds a changefeed the database
// holds as declared equal to its declaration, against the database and
// against the same document alike: neither plans anything.
func TestCompare_YDBChangefeedReadsBackAsDeclared(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "against the database", diff: schemadiff.CompareWithDialect(
			changefeedDeclaration(declared), changefeedCatalog(read), platform.YDB)},
		{name: "against the same document", diff: schemadiff.CompareSchemas(
			changefeedDeclaration(declared), changefeedDeclaration(declared), platform.YDB)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.diff.HasChanges(), qt.IsFalse)
		})
	}
}

// TestCompare_YDBChangefeedChange carries both sides whole where they differ,
// so a planner can pair them by name.
func TestCompare_YDBChangefeedChange(t *testing.T) {
	other := ast.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"}
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		current  *catalog.Database
		wantDiff *difftypes.ChangefeedsChange
	}{
		{name: "one added", desired: changefeedDeclaration(declared, other), current: changefeedCatalog(read),
			wantDiff: &difftypes.ChangefeedsChange{Desired: []ast.ChangefeedSpec{declared, other},
				Current: []ast.ChangefeedSpec{read}}},
		{name: "one removed", desired: changefeedDeclaration(), current: changefeedCatalog(read),
			wantDiff: &difftypes.ChangefeedsChange{Current: []ast.ChangefeedSpec{read}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(test.desired, test.current, platform.YDB)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ChangefeedsChange, qt.DeepEquals, test.wantDiff)
		})
	}
}

// TestCompare_YDBChangefeedCoverage reads each side's silence through what it
// said it does not describe: a changefeed the read recorded as not described
// is neither added nor dropped, and one the declaration does not describe is
// kept as it is.
func TestCompare_YDBChangefeedCoverage(t *testing.T) {
	c := qt.New(t)
	unread := changefeedCatalog()
	unread.NotDescribed = coverage.Set{}.With(coverage.Object{Kind: coverage.Changefeed, Name: "events/updates",
		Reason: coverage.Unsupported, Provenance: coverage.Observed})
	undeclared := changefeedDeclaration()
	undeclared.NotDescribed = coverage.Set{}.WithKind(coverage.Changefeed)

	opts := config.DefaultCompareOptions()
	opts.Dialect = platform.YDB

	withheld, undecided := schemadiff.CompareReportingUndecidedAdditions(changefeedDeclaration(declared), unread, opts)
	kept := schemadiff.CompareWithDialect(undeclared, changefeedCatalog(read), platform.YDB)

	c.Assert(withheld.HasChanges(), qt.IsFalse)
	c.Assert(undecided, qt.DeepEquals, []coverage.Object{{Kind: coverage.Changefeed,
		Name: "events/updates", Reason: coverage.Unsupported, Provenance: coverage.Observed}})
	c.Assert(kept.HasChanges(), qt.IsFalse)
}

// TestCompare_YDBChangefeedsADeclarationDoesNotDescribeAreTheDatabases takes
// the changefeeds a declaration does not describe from the database, onto the
// declaration of the table the diff carries, so a rebuild adds them back after
// the swap rather than dropping them with the old table. A record naming one
// changefeed takes only that one, and a declaration that describes every
// changefeed and declares none drops them, which is the control on the
// first row. The declaration passed in is not changed.
func TestCompare_YDBChangefeedsADeclarationDoesNotDescribeAreTheDatabases(t *testing.T) {
	other := ast.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"}
	tests := []struct {
		name         string
		notDescribed coverage.Set
		want         []ast.ChangefeedSpec
		wantChange   *difftypes.ChangefeedsChange
	}{
		{name: "the whole kind", notDescribed: coverage.Set{}.WithKind(coverage.Changefeed),
			want: []ast.ChangefeedSpec{read, other}},
		{name: "one changefeed by name", notDescribed: coverage.Set{}.WithObject(coverage.Changefeed, "events/updates"),
			want: []ast.ChangefeedSpec{read},
			wantChange: &difftypes.ChangefeedsChange{Desired: []ast.ChangefeedSpec{read},
				Current: []ast.ChangefeedSpec{read, other}}},
		{name: "none", notDescribed: coverage.Set{},
			wantChange: &difftypes.ChangefeedsChange{Current: []ast.ChangefeedSpec{read, other}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := changefeedDeclaration()
			desired.Fields = append(desired.Fields, schemamodel.Field{StructName: "Event", Name: "n", Type: "BIGINT"})
			desired.NotDescribed = test.notDescribed
			current := changefeedCatalog(read, other)
			current.Tables[0].Columns = append(current.Tables[0].Columns, catalog.Column{Name: "n", DataType: "Int32",
				ColumnType: "Int32", IsNullable: "YES", OrdinalPosition: 2})

			diff := schemadiff.CompareWithDialect(desired, current, platform.YDB)

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].Desired.Table.Changefeeds, qt.DeepEquals, test.want)
			c.Assert(diff.TablesModified[0].ChangefeedsChange, qt.DeepEquals, test.wantChange)
			c.Assert(desired.Tables[0].Changefeeds, qt.IsNil)
		})
	}
}
