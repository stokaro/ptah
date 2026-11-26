package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// computedColumnStatements compares a table holding column m, as live
// describes it, with one that declares m as declared, and returns the
// statements the plan writes.
func computedColumnStatements(c *qt.C, declared schemamodel.Field, live catalog.Column) []string {
	c.Helper()
	runtime := must.Must(builtin.New())
	storage := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id"}
	declared.Name, declared.StructName, declared.Type = "m", "Event", "Int64"
	live.Name, live.DataType, live.ColumnType, live.IsNullable = "m", "Int64", "Int64", "NO"
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event", Facets: must.Must(schemaext.NewFacets(storage.Desired()))}},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "Event", Type: "UInt64"},
			{Name: "a", StructName: "Event", Type: "Int64"},
			{Name: "b", StructName: "Event", Type: "Int64"},
			declared,
		},
	}
	current := &catalog.Database{FeatureCoverage: observedStorageCoverage(), Tables: []catalog.Table{{
		Name: "events", Facets: must.Must(schemaext.NewFacets(storage)),
		Columns: []catalog.Column{
			{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO"},
			{Name: "a", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO"},
			{Name: "b", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO"},
			live,
		},
	}}}
	diff, err := schemadiff.CompareWithDialect(c.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	return statements
}

// TestGenerateSchemaDiffSQLStatements_ComputedColumns compares a declared
// computed column with the one system.columns describes, and plans what
// differs: the kind, the expression as ClickHouse groups its operators, and
// the comment. A kind and an expression the server stores in another spelling
// plan nothing.
func TestGenerateSchemaDiffSQLStatements_ComputedColumns(t *testing.T) {
	tests := []struct {
		name     string
		declared schemamodel.Field
		live     catalog.Column
		want     []string
	}{
		{
			name:     "an expression ClickHouse stores with its grouping",
			declared: schemamodel.Field{GeneratedExpression: "a + b * 2", GeneratedKind: "MATERIALIZED"},
			live:     catalog.Column{GeneratedExpression: new("a + (b * 2)"), GeneratedKind: "MATERIALIZED"},
			want:     nil,
		},
		{
			name:     "STORED against MATERIALIZED",
			declared: schemamodel.Field{GeneratedExpression: "a", GeneratedKind: "STORED"},
			live:     catalog.Column{GeneratedExpression: new("a"), GeneratedKind: "MATERIALIZED"},
			want:     nil,
		},
		{
			name:     "VIRTUAL against ALIAS",
			declared: schemamodel.Field{GeneratedExpression: "a", GeneratedKind: "VIRTUAL"},
			live:     catalog.Column{GeneratedExpression: new("a"), GeneratedKind: "ALIAS"},
			want:     nil,
		},
		{
			name:     "no kind against MATERIALIZED",
			declared: schemamodel.Field{GeneratedExpression: "a"},
			live:     catalog.Column{GeneratedExpression: new("a"), GeneratedKind: "MATERIALIZED"},
			want:     nil,
		},
		{
			name:     "EPHEMERAL without an expression against the type's default",
			declared: schemamodel.Field{GeneratedKind: "EPHEMERAL"},
			live:     catalog.Column{GeneratedExpression: new("defaultValueOfTypeName('Int64')"), GeneratedKind: "EPHEMERAL"},
			want:     nil,
		},
		{
			name:     "another expression",
			declared: schemamodel.Field{GeneratedExpression: "a * 3", GeneratedKind: "MATERIALIZED"},
			live:     catalog.Column{GeneratedExpression: new("a + (b * 2)"), GeneratedKind: "MATERIALIZED"},
			want:     []string{"ALTER TABLE `events` MODIFY COLUMN `m` Int64 MATERIALIZED a * 3"},
		},
		{
			name:     "MATERIALIZED to ALIAS",
			declared: schemamodel.Field{GeneratedExpression: "a", GeneratedKind: "ALIAS"},
			live:     catalog.Column{GeneratedExpression: new("a"), GeneratedKind: "MATERIALIZED"},
			want:     []string{"ALTER TABLE `events` MODIFY COLUMN `m` Int64 ALIAS a"},
		},
		{
			name:     "ALIAS to ordinary",
			declared: schemamodel.Field{},
			live:     catalog.Column{GeneratedExpression: new("a"), GeneratedKind: "ALIAS"},
			want:     []string{"ALTER TABLE `events` MODIFY COLUMN `m` REMOVE ALIAS", "ALTER TABLE `events` MODIFY COLUMN `m` Int64"},
		},
		{
			name:     "EPHEMERAL to ordinary",
			declared: schemamodel.Field{},
			live:     catalog.Column{GeneratedExpression: new("5"), GeneratedKind: "EPHEMERAL"},
			want: []string{
				"ALTER TABLE `events` MODIFY COLUMN `m` Int64 DEFAULT defaultValueOfTypeName('Int64')",
				"ALTER TABLE `events` MODIFY COLUMN `m` REMOVE DEFAULT",
				"ALTER TABLE `events` MODIFY COLUMN `m` Int64",
			},
		},
		{
			name:     "ordinary to EPHEMERAL without an expression",
			declared: schemamodel.Field{GeneratedKind: "EPHEMERAL"},
			live:     catalog.Column{},
			want:     []string{"ALTER TABLE `events` MODIFY COLUMN `m` Int64 EPHEMERAL defaultValueOfTypeName('Int64')"},
		},
		{
			name:     "a comment alone",
			declared: schemamodel.Field{GeneratedExpression: "a", GeneratedKind: "MATERIALIZED", Comment: "it's m"},
			live:     catalog.Column{GeneratedExpression: new("a"), GeneratedKind: "MATERIALIZED", Comment: "m"},
			want:     []string{"ALTER TABLE `events` COMMENT COLUMN `m` 'it''s m'"},
		},
		{
			name:     "a comment removed with the kind",
			declared: schemamodel.Field{},
			live:     catalog.Column{GeneratedExpression: new("a"), GeneratedKind: "MATERIALIZED", Comment: "m"},
			want: []string{
				"ALTER TABLE `events` COMMENT COLUMN `m` ''",
				"ALTER TABLE `events` MODIFY COLUMN `m` REMOVE MATERIALIZED",
				"ALTER TABLE `events` MODIFY COLUMN `m` Int64",
			},
		},
		{
			name:     "the same comment",
			declared: schemamodel.Field{Comment: "m"},
			live:     catalog.Column{Comment: "m"},
			want:     nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(computedColumnStatements(c, test.declared, test.live), qt.CmpEquals(cmpopts.EquateEmpty()), test.want)
		})
	}
}
