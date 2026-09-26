//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestReaderHiddenKey_LiveLeavesOutTheEnginesKey reads three CockroachDB tables
// whose keys differ in how many of their columns the engine hides.
//
// A table that declares no primary key gets a hidden rowid and a key over it,
// `<table>_pkey`, which pg_constraint reports as an ordinary PRIMARY KEY. The
// read leaves the column out, and described alone the key named a column the
// description does not have, so every comparison planned `DROP CONSTRAINT
// <table>_pkey` and the server refused it (stokaro/ptah#3738).
//
// The hash-sharded table is the control on "every column": its key spans the
// hidden shard column and the declared one, and it is the author's key. The
// CHECK CockroachDB adds on the shard column alone is the engine's and goes,
// like the key over rowid. The keyed table is the plain control.
func TestReaderHiddenKey_LiveLeavesOutTheEnginesKey(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()
	conn, schemaName := prepareHiddenKeyFixture(c, ctx)

	var engineKeys int
	c.Assert(conn.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_constraint con
		JOIN pg_class cls ON cls.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = cls.relnamespace
		WHERE n.nspname = $1 AND con.conname IN ('keyless_pkey', 'check_crdb_internal_id_shard_16')`,
		schemaName,
	).Scan(&engineKeys), qt.IsNil)
	c.Assert(engineKeys, qt.Equals, 2, qt.Commentf("the engine did not create the constraints the read has to leave out"))

	schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})

	c.Assert(err, qt.IsNil)
	described := make(map[string]string)
	for _, constraint := range schema.Constraints {
		described[constraint.TableName+"."+constraint.Name] = constraint.Type
	}
	c.Assert(described, qt.DeepEquals, map[string]string{
		"keyed.keyed_pkey":     "PRIMARY KEY",
		"sharded.sharded_pkey": "PRIMARY KEY",
	})
}

// prepareHiddenKeyFixture creates a schema of its own holding a keyless table,
// a table with a hash-sharded key and a table with a declared key, and drops it
// when the test ends.
func prepareHiddenKeyFixture(c *qt.C, ctx context.Context) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.CockroachDB))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	schemaName := fmt.Sprintf("ptah_hidden_key_%d", time.Now().UnixNano())
	schema := pgx.Identifier{schemaName}.Sanitize()
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE SCHEMA " + schema,
		"CREATE TABLE " + schema + ".keyless (id INT8)",
		"CREATE TABLE " + schema + ".sharded (id INT8 PRIMARY KEY USING HASH)",
		"CREATE TABLE " + schema + ".keyed (id INT8, CONSTRAINT keyed_pkey PRIMARY KEY (id))",
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
	return conn, schemaName
}
