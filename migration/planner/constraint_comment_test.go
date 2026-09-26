package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// orderConstraints declares app.orders and app.customers with a CHECK and a
// FOREIGN KEY on orders, each carrying comment. check is the CHECK's
// expression.
func orderConstraints(check, comment string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Customer", Name: "customers", Schema: "app"},
			{StructName: "Order", Name: "orders", Schema: "app"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Customer", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "customer_id", Type: "INTEGER", Nullable: true},
			{StructName: "Order", Name: "total", Type: "INTEGER", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName: "Order", Name: "orders_total_positive", Type: "CHECK", Table: "app.orders",
				CheckExpression: check, Comment: comment,
			},
			{
				StructName: "Order", Name: "orders_customer_fk", Type: "FOREIGN KEY", Table: "app.orders",
				Columns: []string{"customer_id"}, ForeignTable: "app.customers", ForeignColumns: []string{"id"},
				Comment: comment,
			},
		},
	}
}

// ordersTables is the database orderConstraints describes, with the two tables
// and their primary keys and neither of the declared constraints.
func ordersTables() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{
				Name: "customers", Schema: "app", Type: "TABLE",
				Columns: []catalog.Column{{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1}},
			},
			{
				Name: "orders", Schema: "app", Type: "TABLE",
				Columns: []catalog.Column{
					{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
					{Name: "customer_id", DataType: "integer", IsNullable: "YES", OrdinalPosition: 2},
					{Name: "total", DataType: "integer", IsNullable: "YES", OrdinalPosition: 3},
				},
			},
		},
		Constraints: []catalog.Constraint{
			{Name: "customers_pkey", TableName: "customers", Schema: "app", Type: "PRIMARY KEY", ColumnNames: []string{"id"}},
			{Name: "orders_pkey", TableName: "orders", Schema: "app", Type: "PRIMARY KEY", ColumnNames: []string{"id"}},
		},
	}
}

// ordersInDatabase is ordersTables with both declared constraints, each
// carrying comment.
func ordersInDatabase(comment string) *catalog.Database {
	database := ordersTables()
	database.Constraints = append(database.Constraints,
		catalog.Constraint{
			Name: "orders_total_positive", TableName: "orders", Schema: "app", Type: "CHECK",
			CheckClause: new("total > 0"), Comment: comment,
		},
		catalog.Constraint{
			Name: "orders_customer_fk", TableName: "orders", Schema: "app", Type: "FOREIGN KEY",
			ColumnNames: []string{"customer_id"}, ColumnName: "customer_id",
			ForeignTable: new("customers"), ForeignSchema: "app", ForeignColumns: []string{"id"},
			DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION"), Comment: comment,
		},
	)
	return database
}

// constraintCommentLines lists every COMMENT ON CONSTRAINT line of a plan.
func constraintCommentLines(sql string) []string {
	var lines []string
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.HasPrefix(line, "COMMENT ON CONSTRAINT") {
			lines = append(lines, line)
		}
	}
	return lines
}

// planOrders compares desired with database on PostgreSQL and renders the
// plan.
func planOrders(c *qt.C, desired *schemamodel.Database, database *catalog.Database) string {
	c.Helper()
	diff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)
	sql, err := planner.GenerateSchemaDiffSQL(diff, platform.Postgres)
	c.Assert(err, qt.IsNil)
	return sql
}

// A changed constraint comment is planned as COMMENT ON CONSTRAINT on its
// table, once, and the constraint is neither dropped nor added
// (stokaro/ptah#3678).
func TestGenerateSchemaDiffSQL_AConstraintCommentIsSetInPlace(t *testing.T) {
	tests := []struct {
		name     string
		current  string
		declared string
		want     []string
	}{
		{
			name: "the comment changed", current: "old", declared: "new",
			want: []string{
				`COMMENT ON CONSTRAINT "orders_customer_fk" ON "app"."orders" IS 'new';`,
				`COMMENT ON CONSTRAINT "orders_total_positive" ON "app"."orders" IS 'new';`,
			},
		},
		{
			name: "the comment removed", current: "old", declared: "",
			want: []string{
				`COMMENT ON CONSTRAINT "orders_customer_fk" ON "app"."orders" IS NULL;`,
				`COMMENT ON CONSTRAINT "orders_total_positive" ON "app"."orders" IS NULL;`,
			},
		},
		{name: "the comment kept", current: "same", declared: "same", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := planOrders(c, orderConstraints("total > 0", test.declared), ordersInDatabase(test.current))

			c.Assert(constraintCommentLines(sql), qt.DeepEquals, test.want, qt.Commentf("plan:\n%s", sql))
			c.Assert(sql, qt.Not(qt.Contains), "CONSTRAINT \"orders_total_positive\" CHECK", qt.Commentf("plan:\n%s", sql))
			c.Assert(sql, qt.Not(qt.Contains), "DROP CONSTRAINT", qt.Commentf("plan:\n%s", sql))
		})
	}
}

// A constraint the plan adds is written with its comment, the foreign key
// included, and a constraint whose definition changed is written again with
// the declared comment. Either way the comment is written once, after the
// constraint.
func TestGenerateSchemaDiffSQL_AnAddedConstraintIsWrittenWithItsComment(t *testing.T) {
	tests := []struct {
		name     string
		check    string
		database *catalog.Database
	}{
		{name: "both constraints added", check: "total > 0", database: ordersTables()},
		{name: "the CHECK redefined", check: "total > 1", database: ordersInDatabase("old")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := planOrders(c, orderConstraints(test.check, "new"), test.database)

			check := `COMMENT ON CONSTRAINT "orders_total_positive" ON "app"."orders" IS 'new';`
			c.Assert(strings.Count(sql, check), qt.Equals, 1, qt.Commentf("plan:\n%s", sql))
			c.Assert(strings.Index(sql, check) > strings.Index(sql, `ADD CONSTRAINT "orders_total_positive"`), qt.IsTrue,
				qt.Commentf("plan:\n%s", sql))
		})
	}
}

// A foreign key the plan adds to an existing table carries its comment. The
// statement that adds it is built apart from the other constraint kinds, and
// without the comment on it the key is added bare and the next comparison
// plans the comment on its own.
func TestGenerateSchemaDiffSQL_AnAddedForeignKeyIsWrittenWithItsComment(t *testing.T) {
	c := qt.New(t)

	sql := planOrders(c, orderConstraints("total > 0", "new"), ordersTables())

	fk := `COMMENT ON CONSTRAINT "orders_customer_fk" ON "app"."orders" IS 'new';`
	c.Assert(strings.Count(sql, fk), qt.Equals, 1, qt.Commentf("plan:\n%s", sql))
	c.Assert(strings.Index(sql, fk) > strings.Index(sql, `ADD CONSTRAINT "orders_customer_fk" FOREIGN KEY`), qt.IsTrue,
		qt.Commentf("plan:\n%s", sql))
}
