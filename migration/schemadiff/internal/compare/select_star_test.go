package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// ordersSchema declares the table orders(id, total) and one view with body.
func ordersSchema(body string, columns ...string) *schemamodel.Database {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Views:  []schemamodel.View{{Name: "big", Body: body}},
	}
	for _, column := range columns {
		desired.Fields = append(desired.Fields, schemamodel.Field{StructName: "Order", Name: column})
	}
	return desired
}

// storedView is the database side: the view as PostgreSQL 18.6 stores it.
func storedView(body string) *catalog.Database {
	return &catalog.Database{Views: []catalog.View{{Name: "big", Schema: "public", Body: body}}}
}

// TestViewsWithDialect_ASelectStarMatchesTheColumnsTheServerStores pins
// stokaro/ptah#3633: a view declared with `*` and the column list PostgreSQL
// stores in its place are the same view. The stored bodies are what
// `pg_get_viewdef` printed on PostgreSQL 18.6 for each declaration.
func TestViewsWithDialect_ASelectStarMatchesTheColumnsTheServerStores(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
	}{
		{
			name:     "a lone star with a filter",
			declared: "SELECT * FROM orders WHERE total > 100",
			stored:   "SELECT id, total FROM orders WHERE (total > 100);",
		},
		{
			name:     "a star beside another item",
			declared: "SELECT *, 1 AS one FROM orders",
			stored:   "SELECT id, total, 1 AS one FROM orders;",
		},
		{
			name:     "a distinct star",
			declared: "SELECT DISTINCT * FROM orders",
			stored:   "SELECT DISTINCT id, total FROM orders;",
		},
		{
			name:     "an alias star with an order",
			declared: "SELECT o.* FROM orders o ORDER BY id",
			stored:   "SELECT id, total FROM orders o ORDER BY id;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.ViewsWithDialect(ordersSchema(test.declared, "id", "total"), storedView(test.stored), diff, platform.Postgres)

			c.Assert(diff.ViewsModified, qt.HasLen, 0)
		})
	}
}

// TestViewsWithDialect_ASelectStarStillReportsARealChange is the other half: a
// star is expanded to the DESIRED columns, so a view created before the table
// gained a column is different, and a star the comparison cannot resolve to
// one declared relation is compared as written.
//
// The last two rows are the scope, not a goal: a star over a join and a star in
// a subquery are left as written, so such a view still re-plans. Their stored
// bodies are what PostgreSQL 18.6 printed.
func TestViewsWithDialect_ASelectStarStillReportsARealChange(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
		columns  []string
	}{
		{
			name:     "the table gained a column the view does not have",
			declared: "SELECT * FROM orders WHERE total > 100",
			stored:   "SELECT id, total FROM orders WHERE (total > 100);",
			columns:  []string{"id", "total", "discount"},
		},
		{
			name:     "the stored view selects fewer columns",
			declared: "SELECT * FROM orders",
			stored:   "SELECT id FROM orders;",
			columns:  []string{"id", "total"},
		},
		{
			name:     "a relation the schema does not declare",
			declared: "SELECT * FROM archive",
			stored:   "SELECT id, total FROM archive;",
			columns:  []string{"id", "total"},
		},
		{
			name:     "a join is not one relation",
			declared: "SELECT * FROM orders JOIN lines USING (id)",
			stored:   "SELECT orders.id, orders.total, lines.qty FROM (orders JOIN lines USING (id));",
			columns:  []string{"id", "total"},
		},
		{
			name:     "a star inside a subquery is not the view's own",
			declared: "SELECT id FROM orders WHERE EXISTS (SELECT * FROM orders)",
			stored:   "SELECT id FROM orders WHERE (EXISTS ( SELECT orders_1.id, orders_1.total FROM orders orders_1));",
			columns:  []string{"id", "total"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.ViewsWithDialect(ordersSchema(test.declared, test.columns...), storedView(test.stored), diff, platform.Postgres)

			c.Assert(diff.ViewsModified, qt.HasLen, 1)
			c.Assert(diff.ViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
		})
	}
}

// TestMaterializedViewsWithDialect_ASelectStarMatchesTheStoredColumns pins the
// materialized view, where a body mismatch plans a drop and a create: on
// PostgreSQL that discards the view's data on every run.
func TestMaterializedViewsWithDialect_ASelectStarMatchesTheStoredColumns(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables:            []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields:            []schemamodel.Field{{StructName: "Order", Name: "id"}, {StructName: "Order", Name: "total"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "big_mv", Body: "SELECT * FROM orders WHERE total > 100"}},
	}
	current := &catalog.Database{MatViews: []catalog.MaterializedView{{
		Name: "big_mv", Schema: "public", Body: "SELECT id, total FROM orders WHERE (total > 100);",
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.MaterializedViewsWithDialect(desired, current, diff, platform.Postgres)

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
}
