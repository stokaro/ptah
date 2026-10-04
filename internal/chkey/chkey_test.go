package chkey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/chkey"
)

// clickHouseTable is a table whose ClickHouse key clauses are the overrides
// given.
func clickHouseTable(overrides map[string]string) schemamodel.Table {
	return schemamodel.Table{Name: "t", Overrides: map[string]map[string]string{platform.ClickHouse: overrides}}
}

// The primary key is PRIMARY KEY, or ORDER BY without one, and a column counts
// when the clause uses it, directly or inside an expression, as the server's
// is_in_primary_key does (measured on 24.10 and 26.9).
func TestPrimaryKeyColumns_HappyPath(t *testing.T) {
	columns := []string{"id", "n", "ts", "toDate"}
	tests := []struct {
		name      string
		overrides map[string]string
		want      map[string]bool
	}{
		{name: "ORDER BY one column", overrides: map[string]string{"order_by": "id"}, want: map[string]bool{"id": true}},
		{name: "ORDER BY a tuple", overrides: map[string]string{"order_by": "(id, n)"}, want: map[string]bool{"id": true, "n": true}},
		{
			name:      "PRIMARY KEY narrower than ORDER BY",
			overrides: map[string]string{"primary_key": "id", "order_by": "(id, n)"},
			want:      map[string]bool{"id": true},
		},
		{
			// toDate names a column here too, and is still read as the function.
			name:      "a column inside an expression",
			overrides: map[string]string{"order_by": "(toDate(ts), id)"},
			want:      map[string]bool{"ts": true, "id": true},
		},
		{name: "quoted names", overrides: map[string]string{"order_by": "(`id`, \"n\")"}, want: map[string]bool{"id": true, "n": true}},
		{name: "a name that is no column", overrides: map[string]string{"order_by": "tuple()"}, want: make(map[string]bool)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, declared := chkey.PrimaryKeyColumns(clickHouseTable(test.overrides), columns)

			c.Assert(declared, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A table that states neither key clause has no ClickHouse key to read: its
// key, if any, is the one its fields declare.
func TestPrimaryKeyColumns_NoKeyClause(t *testing.T) {
	tests := []struct {
		name  string
		table schemamodel.Table
	}{
		{name: "no overrides", table: schemamodel.Table{Name: "t"}},
		{name: "an engine alone", table: clickHouseTable(map[string]string{"engine": "MergeTree"})},
		{name: "blank clauses", table: clickHouseTable(map[string]string{"order_by": " ", "primary_key": ""})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, declared := chkey.PrimaryKeyColumns(test.table, []string{"id"})

			c.Assert(declared, qt.IsFalse)
			c.Assert(got, qt.IsNil)
		})
	}
}
