//go:build integration

package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/dbschema/mssql"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestReadSynonyms_Live reads the three target shapes a synonym can have back
// out of a live server.
//
// The external case is the one that cannot be checked offline. SQL Server does
// not resolve a synonym's target when the synonym is created, so an alias for
// an object in a database that does not exist is created successfully -- and
// that is exactly what makes the shape reachable in a test without standing up
// a second instance or a linked server.
func TestReadSynonyms_Live(t *testing.T) {
	c := qt.New(t)
	db := openLiveSQLServerRealmDatabase(t)

	for _, statement := range []string{
		"CREATE SCHEMA [app]",
		"CREATE SCHEMA [sales]",
		"CREATE TABLE [dbo].[orders] ([id] bigint NOT NULL PRIMARY KEY)",
		"CREATE TABLE [sales].[invoices] ([id] bigint NOT NULL PRIMARY KEY)",
		"CREATE SYNONYM [dbo].[orders_alias] FOR [dbo].[orders]",
		"CREATE SYNONYM [app].[invoices_alias] FOR [sales].[invoices]",
		"CREATE SYNONYM [app].[remote_alias] FOR [other_db].[dbo].[orders]",
	} {
		_, err := db.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	reader := mssql.NewSQLServerReader(db, "dbo")
	reader.SetSchemas([]string{"dbo", "app", "sales"})
	schema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	byName := map[string]int{}
	for i, synonym := range schema.Synonyms {
		byName[synonym.QualifiedName()] = i
	}
	c.Assert(byName, qt.HasLen, 3)

	local := schema.Synonyms[byName["dbo.orders_alias"]]
	c.Assert(local.TargetSchema, qt.Equals, "dbo")
	c.Assert(local.TargetObject, qt.Equals, "orders")
	c.Assert(local.IsExternal(), qt.IsFalse)
	c.Assert(local.TargetQualifiedName(), qt.Equals, "dbo.orders")

	crossSchema := schema.Synonyms[byName["app.invoices_alias"]]
	c.Assert(crossSchema.TargetSchema, qt.Equals, "sales")
	c.Assert(crossSchema.TargetObject, qt.Equals, "invoices")
	c.Assert(crossSchema.IsExternal(), qt.IsFalse)
	c.Assert(crossSchema.TargetQualifiedName(), qt.Equals, "sales.invoices")

	external := schema.Synonyms[byName["app.remote_alias"]]
	c.Assert(external.TargetDatabase, qt.Equals, "other_db")
	c.Assert(external.TargetSchema, qt.Equals, "dbo")
	c.Assert(external.TargetObject, qt.Equals, "orders")
	c.Assert(external.IsExternal(), qt.IsTrue)
	c.Assert(external.TargetQualifiedName(), qt.Equals, "",
		qt.Commentf("an external target is not a local dependency this plan can order against"))
}

// TestSynonymsDiffToZero_Live is the convergence check the whole object exists
// for. The declared targets are written the way a schema author writes them,
// unbracketed, and the catalog returns them in the server's own quoting -- so a
// comparison that did not normalize would report a modification here on every
// run and the plan would never converge.
func TestSynonymsDiffToZero_Live(t *testing.T) {
	c := qt.New(t)
	db := openLiveSQLServerRealmDatabase(t)

	for _, statement := range []string{
		"CREATE SCHEMA [app]",
		"CREATE TABLE [dbo].[orders] ([id] bigint NOT NULL PRIMARY KEY)",
		"CREATE SYNONYM [app].[orders_alias] FOR [dbo].[orders]",
		"CREATE SYNONYM [app].[remote_alias] FOR [other_db].[dbo].[orders]",
	} {
		_, err := db.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	reader := mssql.NewSQLServerReader(db, "dbo")
	reader.SetSchemas([]string{"dbo", "app"})
	schema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	declared := &goschema.Database{Synonyms: []goschema.Synonym{
		{StructName: "OrdersAlias", Name: "orders_alias", Schema: "app", Target: "dbo.orders"},
		{StructName: "RemoteAlias", Name: "remote_alias", Schema: "app", Target: "other_db.dbo.orders"},
	}}

	diff := schemadiff.CompareWithDialect(declared, schema, platform.SQLServer)

	c.Assert(diff.SynonymsAdded, qt.HasLen, 0)
	c.Assert(diff.SynonymsRemoved, qt.HasLen, 0)
	c.Assert(diff.SynonymsModified, qt.HasLen, 0,
		qt.Commentf("declared and stored targets differ only in the server's bracket quoting"))
}

// TestSynonymRetarget_Live pins the ordered drop and create. T-SQL has no
// ALTER SYNONYM, and CREATE SYNONYM refuses a name that already exists, so the
// two statements are not interchangeable and their order is the whole
// operation.
func TestSynonymRetarget_Live(t *testing.T) {
	c := qt.New(t)
	db := openLiveSQLServerRealmDatabase(t)

	for _, statement := range []string{
		"CREATE TABLE [dbo].[orders] ([id] bigint NOT NULL PRIMARY KEY)",
		"CREATE TABLE [dbo].[archived_orders] ([id] bigint NOT NULL PRIMARY KEY)",
		"CREATE SYNONYM [dbo].[orders_alias] FOR [dbo].[orders]",
	} {
		_, err := db.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	_, err := db.ExecContext(t.Context(), "CREATE SYNONYM [dbo].[orders_alias] FOR [dbo].[archived_orders]")
	c.Assert(err, qt.IsNotNil, qt.Commentf("the server must refuse a create over an existing synonym"))

	for _, statement := range []string{
		"DROP SYNONYM [dbo].[orders_alias]",
		"CREATE SYNONYM [dbo].[orders_alias] FOR [dbo].[archived_orders]",
	} {
		_, err := db.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	reader := mssql.NewSQLServerReader(db, "dbo")
	schema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Synonyms, qt.HasLen, 1)
	c.Assert(schema.Synonyms[0].TargetObject, qt.Equals, "archived_orders")
}
