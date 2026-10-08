package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// keyedTable is a ClickHouse table t (id, n) with the engine clauses given as a
// declaration states them, and no column marked primary.
func keyedTable(overrides map[string]string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t",
			Overrides: map[string]map[string]string{platform.ClickHouse: overrides},
		}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "INTEGER"},
			{StructName: "T", Name: "n", Type: "INTEGER"},
		},
	}
}

// liveTable is the same table as the reader describes it: the engine clauses,
// and each column the primary key uses marked primary, as is_in_primary_key
// marks it.
func liveTable(overrides map[string]string, keyColumns ...string) *schemamodel.Database {
	database := keyedTable(overrides)
	for i := range database.Fields {
		for _, key := range keyColumns {
			if database.Fields[i].Name == key {
				database.Fields[i].Primary = true
			}
		}
	}
	return database
}

// A ClickHouse table identical to its declaration plans nothing. The
// declaration states the key as PRIMARY KEY or ORDER BY on the engine, and the
// comparison reads the key columns from those clauses as the server does. Read
// from the columns, it reported `primary_key: true -> false` and planned
// `MODIFY COLUMN id Int32` on every run (stokaro/ptah#4104).
func TestGenerateSchemaDiffSQLStatements_ClickHouseSortingKey_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]string
		keys      []string
	}{
		{name: "ORDER BY one column", overrides: map[string]string{"engine": "MergeTree", "order_by": "id"}, keys: []string{"id"}},
		{name: "ORDER BY a tuple", overrides: map[string]string{"engine": "MergeTree", "order_by": "(id, n)"}, keys: []string{"id", "n"}},
		{
			name:      "PRIMARY KEY narrower than ORDER BY",
			overrides: map[string]string{"engine": "MergeTree", "primary_key": "id", "order_by": "(id, n)"},
			keys:      []string{"id"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareSchemas(t.Context(), keyedTable(test.overrides), liveTable(test.overrides, test.keys...), platform.ClickHouse, must.Must(builtin.New())))

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				context.Background(), must.Must(builtin.New()),
				diff, platform.ClickHouse, planner.Options{Capabilities: capability.ClickHouse2411()},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.HasLen, 0)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// A declared key that moves a column into or out of the table's primary key
// is refused. ClickHouse has no ALTER that changes a MergeTree table's primary
// key, and the MODIFY COLUMN the change would plan changes nothing.
func TestGenerateSchemaDiffSQLStatements_ClickHouseSortingKey_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		declared map[string]string
		live     map[string]string
		keys     []string
		want     string
	}{
		{
			name:     "a column joins the key",
			declared: map[string]string{"engine": "MergeTree", "order_by": "(id, n)"},
			live:     map[string]string{"engine": "MergeTree", "order_by": "id"},
			keys:     []string{"id"},
			want:     "column n: false -> true",
		},
		{
			name:     "a column leaves the key",
			declared: map[string]string{"engine": "MergeTree", "primary_key": "id", "order_by": "(id, n)"},
			live:     map[string]string{"engine": "MergeTree", "order_by": "(id, n)"},
			keys:     []string{"id", "n"},
			want:     "column n: true -> false",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				context.Background(), must.Must(builtin.New()),
				must.Must(schemadiff.CompareSchemas(t.Context(), keyedTable(test.declared), liveTable(test.live, test.keys...), platform.ClickHouse, must.Must(builtin.New()))),
				platform.ClickHouse, planner.Options{Capabilities: capability.ClickHouse2411()},
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `the primary key of t changes \(`+test.want+`\): ClickHouse fixes a MergeTree table's`+
				` primary key when the table is created, and no ALTER changes it; create the table again with the new key and copy its rows`)
			c.Assert(got, qt.IsNil)
		})
	}
}
