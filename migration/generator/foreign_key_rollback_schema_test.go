package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// droppedForeignKeyRollback plans the drop of orders_customer_fk on table in
// both directions, against a database whose key references customers in
// foreignSchema, and renders the rollback on PostgreSQL 18.
func droppedForeignKeyRollback(c *qt.C, table, schema, foreignSchema string) string {
	c.Helper()
	diff := &difftypes.SchemaDiff{ConstraintsRemoved: difftypes.ConstraintRemovals{{
		Name: "orders_customer_fk", TableName: table, Type: "FOREIGN KEY",
	}}}
	current := &catalog.Database{Constraints: []catalog.Constraint{{
		Name: "orders_customer_fk", TableName: "orders", Schema: schema, Type: "FOREIGN KEY",
		ColumnNames: []string{"customer_id"}, ColumnName: "customer_id",
		ForeignTable: new("customers"), ForeignSchema: foreignSchema, ForeignColumns: []string{"id"},
	}}}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &schemamodel.Database{},
		CurrentSchema: current,
		Dialect:       platform.Postgres,
		Capabilities:  capability.Postgres18(),
	})
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// A rollback that adds back a dropped foreign key names the referenced table
// with the schema the database reported it in (stokaro/ptah#3688). The bare
// name resolves through the search_path, which is where PostgreSQL 18
// answered `relation "customers" does not exist` for a key into a schema off
// the path. A key into the default schema, which the reader reports with no
// schema, keeps the bare name.
func TestPlanBidirectionalSchemaDiff_DroppedForeignKeyComesBackIntoItsSchema(t *testing.T) {
	tests := []struct {
		name          string
		table         string
		schema        string
		foreignSchema string
		want          string
	}{
		{
			name: "a key into another schema", table: "app.orders", schema: "app", foreignSchema: "app",
			want: `ALTER TABLE "app"."orders" ADD CONSTRAINT "orders_customer_fk" FOREIGN KEY ("customer_id") REFERENCES "app"."customers"("id");`,
		},
		{
			name: "a key across two schemas", table: "app.orders", schema: "app", foreignSchema: "crm",
			want: `ALTER TABLE "app"."orders" ADD CONSTRAINT "orders_customer_fk" FOREIGN KEY ("customer_id") REFERENCES "crm"."customers"("id");`,
		},
		{
			name: "a key in the default schema", table: "orders", schema: "", foreignSchema: "",
			want: `ALTER TABLE "orders" ADD CONSTRAINT "orders_customer_fk" FOREIGN KEY ("customer_id") REFERENCES "customers"("id");`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := droppedForeignKeyRollback(c, test.table, test.schema, test.foreignSchema)

			c.Assert(sql, qt.Contains, test.want, qt.Commentf("rollback:\n%s", sql))
		})
	}
}
