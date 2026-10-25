package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestMaterializedViewQuery_DropsTheDataClause pins the spellings of the
// clause the server does not store.
func TestMaterializedViewQuery_DropsTheDataClause(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "WITH NO DATA", body: "SELECT total FROM orders WITH NO DATA"},
		{name: "WITH DATA", body: "SELECT total FROM orders WITH DATA"},
		{name: "lower case", body: "SELECT total FROM orders with no data"},
		{name: "a line break inside the clause", body: "SELECT total FROM orders\nWITH NO\n  DATA"},
		{name: "a semicolon after the clause", body: "SELECT total FROM orders WITH NO DATA;"},
		{name: "a comment after the clause", body: "SELECT total FROM orders WITH NO DATA -- refreshed nightly\n"},
		{name: "a comment inside the clause", body: "SELECT total FROM orders WITH /* empty */ NO DATA"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(compare.MaterializedViewQuery(test.body), qt.Equals, "SELECT total FROM orders")
		})
	}
}

// TestMaterializedViewQuery_KeepsTheQuery pins the bodies that end in no such
// clause, which come back unchanged.
func TestMaterializedViewQuery_KeepsTheQuery(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "no clause", body: "SELECT total FROM orders"},
		{name: "the clause in a string", body: "SELECT 'WITH NO DATA' AS note FROM orders"},
		{name: "the clause in a trailing comment", body: "SELECT total FROM orders -- WITH NO DATA"},
		{name: "a quoted name that spells data", body: `SELECT total FROM orders WITH NO "data"`},
		{name: "a column named data", body: "SELECT data FROM orders"},
		{name: "the clause before the end", body: "SELECT total FROM orders WITH NO DATA WHERE false"},
		{name: "an empty body", body: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(compare.MaterializedViewQuery(test.body), qt.Equals, test.body)
		})
	}
}

// TestMaterializedViewsWithDialect_ComparesTheQuery pins the comparison that
// converges: a materialized view declared WITH NO DATA against the definition
// PostgreSQL reports for it, which has no clause. Compared with the clause, the
// view is changed on every plan, which drops and creates it and every view
// that reads it (stokaro/ptah#5006).
func TestMaterializedViewsWithDialect_ComparesTheQuery(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
	}{
		{name: "WITH NO DATA", declared: "SELECT sum(amount) AS total FROM orders WITH NO DATA", stored: "SELECT sum(amount) AS total FROM orders"},
		{name: "WITH DATA", declared: "SELECT sum(amount) AS total FROM orders WITH DATA", stored: "SELECT sum(amount) AS total FROM orders"},
		{
			// A comparison of two declarations, as a file-to-file diff makes,
			// holds the clause on both sides.
			name: "the clause on both sides", declared: "SELECT sum(amount) AS total FROM orders WITH NO DATA",
			stored: "SELECT sum(amount) AS total FROM orders WITH DATA",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.MaterializedViewsWithDialect(&schemamodel.Database{
				MaterializedViews: []schemamodel.MaterializedView{{Name: "order_snapshot", Body: test.declared}},
			}, &catalog.Database{
				MatViews: []catalog.MaterializedView{{Name: "order_snapshot", Body: test.stored}},
			}, diff, platform.Postgres)

			c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
			c.Assert(diff.Matched.MaterializedViews, qt.Equals, 1)
		})
	}
}

// TestMaterializedViewsWithDialect_DetectsAChangedQueryWithTheClause is the
// control: the clause hides no change to the query, and the change keeps the
// declared clause, so the view the plan creates again is created as declared.
func TestMaterializedViewsWithDialect_DetectsAChangedQueryWithTheClause(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	declared := "SELECT sum(amount) AS total, count(*) AS n FROM orders WITH NO DATA"

	compare.MaterializedViewsWithDialect(&schemamodel.Database{
		MaterializedViews: []schemamodel.MaterializedView{{Name: "order_snapshot", Body: declared}},
	}, &catalog.Database{
		MatViews: []catalog.MaterializedView{{Name: "order_snapshot", Body: "SELECT sum(amount) AS total FROM orders"}},
	}, diff, platform.Postgres)

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(diff.MaterializedViewsModified[0].Changes["body"], qt.Equals,
		"SELECT sum(amount) AS total FROM orders -> "+declared)
	c.Assert(diff.MaterializedViewsModified[0].Desired.Body, qt.Equals, declared)
}
