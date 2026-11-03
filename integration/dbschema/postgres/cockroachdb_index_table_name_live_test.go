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

// TestReaderIndexTableName_LiveReadsEachTableOnce reads a CockroachDB schema
// where two tables carry the names of indexes on other tables.
//
// CockroachDB names an index per table, so `CREATE INDEX i ON t` and `CREATE
// TABLE i` both succeed, and so does a table named p_pkey beside the primary
// key index of p. Both indexes are listed in pg_class under the schema, beside
// the tables of their names. PostgreSQL refuses both tables with `relation
// already exists`. A read that finds a table's pg_class row by name alone
// finds two rows for each: the table read fails with `duplicate feature
// identity: subject coverage for table <schema>.i`, and the column and grant
// reads double what they describe (stokaro/ptah#5043).
//
// Table t and its index i are the control on the other side of the collision:
// the index is still read, on the table that holds it.
func TestReaderIndexTableName_LiveReadsEachTableOnce(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()
	conn, schemaName := prepareIndexTableNameFixture(c, ctx)

	schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})

	c.Assert(err, qt.IsNil)
	columns := make(map[string][]string)
	for _, table := range schema.Tables {
		names := make([]string, 0, len(table.Columns))
		for _, column := range table.Columns {
			names = append(names, column.Name)
		}
		columns[table.Name] = append(columns[table.Name], fmt.Sprint(names))
	}
	c.Assert(columns, qt.DeepEquals, map[string][]string{
		"i":      {"[a b]"},
		"p":      {"[id]"},
		"p_pkey": {"[a]"},
		"t":      {"[a]"},
	})
	c.Assert(grantedPrivileges(schema.Grants), qt.DeepEquals, []string{
		"PUBLIC SELECT on TABLE " + schemaName + ".i",
	})
	indexes := make(map[string]string)
	for _, index := range schema.Indexes {
		indexes[index.TableName+"."+index.Name] = fmt.Sprint(index.Columns)
	}
	c.Assert(indexes["t.i"], qt.Equals, "[a]")
}

// prepareIndexTableNameFixture creates a schema of its own holding the tables
// whose names collide with indexes, and drops it when the test ends. The grant
// goes to public so the fixture creates no cluster-wide role.
func prepareIndexTableNameFixture(c *qt.C, ctx context.Context) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.CockroachDB))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	schemaName := fmt.Sprintf("ptah_index_table_name_%d", time.Now().UnixNano())
	schema := pgx.Identifier{schemaName}.Sanitize()
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE SCHEMA " + schema,
		"CREATE TABLE " + schema + ".t (a INT8)",
		"CREATE INDEX i ON " + schema + ".t (a)",
		"CREATE TABLE " + schema + ".i (a INT8, b STRING)",
		"GRANT SELECT ON TABLE " + schema + ".i TO public",
		"CREATE TABLE " + schema + ".p (id INT8 PRIMARY KEY)",
		"CREATE TABLE " + schema + ".p_pkey (a INT8)",
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
	return conn, schemaName
}
