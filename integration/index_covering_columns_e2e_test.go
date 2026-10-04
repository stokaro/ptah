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
// never got the payload (stokaro/ptah#4112).

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
// index's payload.
var coveringIndexEngines = []struct {
	name   string
	server func(c *qt.C) devDialectDatabase
}{
	{name: "PostgreSQL", server: postgresCoveringDatabase},
	{name: "CockroachDB", server: cockroachDevDatabase},
	{name: "YugabyteDB", server: yugabyteDevDatabase},
	{name: "Spanner", server: spannerDevDatabase},
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
	if index.Name != "users_name_ix" {
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
				for _, statement := range []string{"DROP INDEX IF EXISTS users_name_ix", "DROP TABLE IF EXISTS users"} {
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
