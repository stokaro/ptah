//go:build integration

package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/synonym"
	"ptah.run/internal/dbschema/mssql"
	"ptah.run/migration/schemadiff"
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
	schema, err := reader.ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)

	synonyms := readSynonyms(c, schema)
	c.Assert(synonyms, qt.HasLen, 3)
	c.Assert(synonyms["dbo.orders_alias"], qt.Equals, "dbo.orders")
	c.Assert(synonyms["app.invoices_alias"], qt.Equals, "sales.invoices")
	c.Assert(synonyms["app.remote_alias"], qt.Equals, "other_db.dbo.orders",
		qt.Commentf("the server's bracket quoting is read into the spelling a declaration uses"))
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
	schema, err := reader.ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)

	declared := declaredSynonyms(
		synonym.DesiredSynonym{StructName: "OrdersAlias", Synonym: synonym.Synonym{Name: "orders_alias", Schema: "app", Target: "dbo.orders"}},
		synonym.DesiredSynonym{StructName: "RemoteAlias", Synonym: synonym.Synonym{Name: "remote_alias", Schema: "app", Target: "other_db.dbo.orders"}},
	)

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), declared, schema, platform.SQLServer, must.Must(builtin.New())))

	c.Assert(diff.FeatureChanges, qt.HasLen, 0,
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
	schema, err := reader.ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(readSynonyms(c, schema), qt.DeepEquals, map[string]string{"orders_alias": "dbo.archived_orders"},
		qt.Commentf("a read with no schema list reports the default schema as no schema"))

	// Pointing the declaration back is one change -- the alias written with
	// the default schema is the one read without it -- and the owner's
	// statements for it are the same ordered pair, which the server accepts.
	declared := declaredSynonyms(synonym.DesiredSynonym{Synonym: synonym.Synonym{Name: "orders_alias", Schema: "dbo", Target: "dbo.orders"}})
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), declared, schema, platform.SQLServer, must.Must(builtin.New())))
	c.Assert(diff.FeatureChanges, qt.HasLen, 1)
	change := diff.FeatureChanges[0].Value.(*synonym.Change)
	statements := must.Must(builtin.RenderSQL(platform.SQLServer, &ast.ExtensionStatement{
		Payload: &synonym.Operation{Action: synonym.Retarget, Synonym: *change.After},
	}))
	_, err = db.ExecContext(t.Context(), statements)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statements))
	schema, err = reader.ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(readSynonyms(c, schema), qt.DeepEquals, map[string]string{"orders_alias": "dbo.orders"})
}

// readSynonyms maps each synonym a read found, as schema.name, to its target.
func readSynonyms(c *qt.C, schema *catalog.Database) map[string]string {
	c.Helper()
	objects, err := schema.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	targets := make(map[string]string)
	for _, object := range objects {
		if observed, ok := object.Value.(*synonym.ObservedSynonym); ok {
			targets[observed.QualifiedName()] = observed.Target
		}
	}
	return targets
}

// declaredSynonyms is a declaration of exactly these synonyms, from a source
// that can declare them, so a synonym it leaves out is one to drop.
func declaredSynonyms(synonyms ...synonym.DesiredSynonym) *schemamodel.Database {
	objects := schemaext.Objects{}
	for _, declared := range synonyms {
		objects = must.Must(objects.With(synonym.DeclaredObject(declared)))
	}
	return &schemamodel.Database{FeatureObjects: objects,
		FeatureCoverage: must.Must(synonym.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}
}
