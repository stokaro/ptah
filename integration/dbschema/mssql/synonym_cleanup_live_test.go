//go:build integration

package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/mssql"
)

// TestDropDatabaseRealm_SynonymsDoNotBlockSchemaDrop is the scenario
// stokaro/ptah#1030 was opened for: cleanup reaching a schema while a synonym
// still lives in it, and failing.
//
// The realm enumeration is ordered alphabetically rather than by dependency, so
// the ordering is not guaranteed by construction and has to be measured. The
// fixture is built so alphabetical order is the WRONG order in both directions:
// `aaa` holds a synonym pointing into `zzz`, and `zzz` holds one pointing back,
// so whichever schema the sweep reaches first still contains an object that
// names something in the other.
func TestDropDatabaseRealm_SynonymsDoNotBlockSchemaDrop(t *testing.T) {
	c := qt.New(t)
	db := openLiveSQLServerRealmDatabase(t)
	writer := mssql.NewSQLServerWriter(db, "dbo")

	for _, statement := range []string{
		"CREATE SCHEMA [aaa]",
		"CREATE SCHEMA [zzz]",
		"CREATE TABLE [aaa].[orders] ([id] bigint NOT NULL PRIMARY KEY)",
		"CREATE TABLE [zzz].[invoices] ([id] bigint NOT NULL PRIMARY KEY)",
		"CREATE VIEW [zzz].[order_view] AS SELECT [id] FROM [aaa].[orders]",
		"CREATE SYNONYM [aaa].[invoices_alias] FOR [zzz].[invoices]",
		"CREATE SYNONYM [aaa].[view_alias] FOR [zzz].[order_view]",
		"CREATE SYNONYM [zzz].[orders_alias] FOR [aaa].[orders]",
		"CREATE SYNONYM [zzz].[missing_alias] FOR [other_db].[dbo].[gone]",
	} {
		_, err := db.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)

	reader := mssql.NewSQLServerReader(db, "dbo")
	reader.SetSchemas([]string{"dbo", "aaa", "zzz"})
	schema, readErr := reader.ReadSchema()
	c.Assert(readErr, qt.IsNil)
	c.Assert(schema.Synonyms, qt.HasLen, 0)
	c.Assert(schema.Views, qt.HasLen, 0)
	c.Assert(schema.Tables, qt.HasLen, 0)
}

// TestDropDatabaseRealm_IsIdempotentWithSynonyms runs the sweep twice. The
// second pass has nothing to drop, and a cleanup that only works on a populated
// database is one that fails the moment it is retried after a partial run.
func TestDropDatabaseRealm_IsIdempotentWithSynonyms(t *testing.T) {
	c := qt.New(t)
	db := openLiveSQLServerRealmDatabase(t)
	writer := mssql.NewSQLServerWriter(db, "dbo")

	for _, statement := range []string{
		"CREATE TABLE [dbo].[orders] ([id] bigint NOT NULL PRIMARY KEY)",
		"CREATE SYNONYM [dbo].[orders_alias] FOR [dbo].[orders]",
	} {
		_, err := db.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	c.Assert(writer.DropDatabaseRealm(t.Context()), qt.IsNil)
	c.Assert(writer.DropDatabaseRealm(t.Context()), qt.IsNil)
}
