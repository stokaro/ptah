//go:build integration

package dbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// orderDeclaration declares customers and orders in schemaName, with the
// table constraints key.
func orderDeclaration(schemaName string, key []schemamodel.Constraint) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Customer", Name: "customers", Schema: schemaName},
			{StructName: "Order", Name: "orders", Schema: schemaName},
		},
		Fields: []schemamodel.Field{
			{StructName: "Customer", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "customer_id", Type: "INTEGER", Nullable: true},
		},
		Constraints: key,
	}
}

// customerKey is the foreign key from orders to customers in schemaName.
func customerKey(schemaName string) []schemamodel.Constraint {
	return []schemamodel.Constraint{{
		StructName: "Order", Name: "orders_customer_fk", Type: "FOREIGN KEY", Table: schemaName + ".orders",
		Columns: []string{"customer_id"}, ForeignTable: schemaName + ".customers", ForeignColumns: []string{"id"},
	}}
}

// A rollback that adds back a dropped foreign key into a schema off the
// search_path applies, and the key references the table it referenced before
// (stokaro/ptah#3688). With the referenced table named bare, PostgreSQL 18
// answers `relation "customers" does not exist`.
func TestPostgresLiveDroppedForeignKeyRollbackApplies(t *testing.T) {
	c := qt.New(t)
	conn := returnTypeConnection(c, dbtarget.URL(t, dbtarget.PostgreSQL))
	schemaName := returnTypeSchema(c, conn, "ptah_fk_rollback")
	applyDeclaration(c, conn, orderDeclaration(schemaName, customerKey(schemaName)))
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	wanted := orderDeclaration(schemaName, nil)
	diff := schemadiff.CompareWithDialect(wanted, live, platform.Postgres)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 1)
	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: wanted,
		CurrentSchema: live,
		Dialect:       platform.Postgres,
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})
	c.Assert(err, qt.IsNil)
	forward, err := planner.GenerateSchemaDiffSQLStatements(plan.Forward.Diff, platform.Postgres)
	c.Assert(err, qt.IsNil)
	reverse, err := planner.GenerateSchemaDiffSQLStatements(plan.Reverse.Diff, platform.Postgres)
	c.Assert(err, qt.IsNil)
	applyStatements(c, conn, forward)

	applyStatements(c, conn, reverse)

	var referenced string
	c.Assert(conn.QueryRowContext(c.Context(), `
		SELECT n.nspname || '.' || t.relname FROM pg_constraint k
		JOIN pg_class t ON t.oid = k.confrelid JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE k.conname = 'orders_customer_fk' AND k.connamespace = $1::regnamespace`, schemaName,
	).Scan(&referenced), qt.IsNil)
	c.Assert(referenced, qt.Equals, schemaName+".customers")
	restored, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	settled := schemadiff.CompareWithDialect(orderDeclaration(schemaName, customerKey(schemaName)), restored, platform.Postgres)
	c.Assert(settled.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(settled.ConstraintsRemoved, qt.HasLen, 0)
}
