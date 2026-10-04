//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// An index that only gains payload columns -- INCLUDE on the PostgreSQL family,
// STORING on CockroachDB -- was planned on PostgreSQL alone. CockroachDB,
// YugabyteDB and Spanner compared indexes by name past the predicate, so the
// desired covering index and the live plain one read as equal and the database
// never got the payload (stokaro/ptah#4112). SQL Server takes INCLUDE too;
// without the reader's payload columns its live index reads back plain, and
// every diff plans the rebuild again (stokaro/ptah#4114).

// coveringIndexCurrent is the table and the index the database starts with.
var coveringIndexCurrent = []string{
	"CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL, name VARCHAR(100))",
	"CREATE INDEX users_name_ix ON users (name)",
}

// coveringIndexDesired is the same table with the index carrying email as its
// payload.
const coveringIndexDesired = `CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL, name VARCHAR(100));
CREATE INDEX users_name_ix ON users (name) INCLUDE (email);
`

// postgresCoveringDatabase is a scratch PostgreSQL database, the control: its
// comparison planned the payload before this change.
func postgresCoveringDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	devURL := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL),
		"DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
	return devDialectDatabase{url: devURL, conn: connectDevDialect(c, devURL)}
}

// coveringIndexEngines are the engines whose preset renders and reads an
// index's payload. dropIndex removes users_name_ix in the engine's spelling.
var coveringIndexEngines = []struct {
	name      string
	server    func(c *qt.C) devDialectDatabase
	dropIndex string
}{
	{name: "PostgreSQL", server: postgresCoveringDatabase, dropIndex: "DROP INDEX IF EXISTS users_name_ix"},
	{name: "CockroachDB", server: cockroachDevDatabase, dropIndex: "DROP INDEX IF EXISTS users_name_ix"},
	{name: "YugabyteDB", server: yugabyteDevDatabase, dropIndex: "DROP INDEX IF EXISTS users_name_ix"},
	{name: "Spanner", server: spannerDevDatabase, dropIndex: "DROP INDEX IF EXISTS users_name_ix"},
	{name: "SQL Server", server: sqlServerDevDatabase, dropIndex: "DROP INDEX IF EXISTS users_name_ix ON users"},
}

// coveringIndexPayload reads back the payload columns of users_name_ix.
func coveringIndexPayload(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	read, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, nil)
	c.Assert(err, qt.IsNil)
	var payload []string
	for _, index := range read.Indexes {
		payload = append(payload, coveringPayloadOf(index)...)
	}
	return payload
}

// coveringPayloadOf returns index's payload when it is users_name_ix.
func coveringPayloadOf(index catalog.Index) []string {
	return payloadOfIndex(index, "users_name_ix")
}

// payloadOfIndex returns index's payload when it is the index called name.
func payloadOfIndex(index catalog.Index, name string) []string {
	if index.Name != name {
		return nil
	}
	return index.IncludeColumns
}

// TestSchemaApplyAddsAnIndexPayloadE2E applies a desired state whose only
// change is an index gaining a payload column. The server stores the payload,
// and a second diff against the same file is in sync.
func TestSchemaApplyAddsAnIndexPayloadE2E(t *testing.T) {
	for _, engine := range coveringIndexEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dev := engine.server(c)
			dev.exec(c, coveringIndexCurrent)
			// The scratch databases go with the test, except Spanner's, which
			// the emulator keeps.
			c.Cleanup(func() {
				for _, statement := range []string{engine.dropIndex, "DROP TABLE IF EXISTS users"} {
					_, err := dev.conn.ExecContext(context.Background(), statement)
					c.Check(err, qt.IsNil, qt.Commentf("%s", statement))
				}
			})
			desired := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(desired, []byte(coveringIndexDesired), 0o600), qt.IsNil)
			c.Assert(coveringIndexPayload(c, dev.conn), qt.HasLen, 0)

			out, err := runPtahNativeWithError("schema", "apply", "--db-url", dev.url,
				"--schema-file", desired, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(coveringIndexPayload(c, dev.conn), qt.DeepEquals, []string{"email"})

			out, err = runPtahNativeWithError("schema", "diff", "--from", dev.url, "--to", desired)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Schemas are synced")
		})
	}
}

// TestSchemaApplyKeepsTheIndexPayloadOrderSQLServerE2E declares a payload in
// an order that is not the table's column order. sys.index_columns numbers the
// payload in the order written, and the reader orders by that number, so the
// read-back keeps it and a second diff is in sync. Read in column order, the
// payload comes back as b, c, and every diff plans the rebuild again.
func TestSchemaApplyKeepsTheIndexPayloadOrderSQLServerE2E(t *testing.T) {
	c := qt.New(t)
	dev := sqlServerDevDatabase(c)
	desired := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(desired, []byte(`CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT, c INT);
CREATE INDEX t_a_ix ON t (a) INCLUDE (c, b);
`), 0o600), qt.IsNil)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", dev.url,
		"--schema-file", desired, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	read, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), dev.conn, nil)
	c.Assert(err, qt.IsNil)
	var payload []string
	for _, index := range read.Indexes {
		payload = append(payload, payloadOfIndex(index, "t_a_ix")...)
	}
	c.Assert(payload, qt.DeepEquals, []string{"c", "b"})

	out, err = runPtahNativeWithError("schema", "diff", "--from", dev.url, "--to", desired)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced")
}
