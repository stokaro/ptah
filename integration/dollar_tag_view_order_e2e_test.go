//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// dollarTagViewOrderSchema declares the view label_source before the view
// labels it reads, and labels holds the text `FROM label_source` only inside a
// string quoted with a tag outside ASCII, the euro sign. The dependency order
// reads the tag as the server does, so labels names no relation and is
// created first. Read with a tag rule that allows only letters, the text
// inside the string reads as a reference to label_source, labels is placed
// after it, and the server refuses label_source, whose relation labels does
// not exist yet.
const dollarTagViewOrderSchema = "CREATE VIEW label_source AS SELECT label FROM labels;\n" +
	"CREATE VIEW labels AS SELECT $€$ FROM label_source $€$ AS label;\n"

// heredocViewOrderSchema is [dollarTagViewOrderSchema] on ClickHouse, whose
// string is a heredoc with a tag that starts with a digit, as ClickHouse 26.10
// reads it.
const heredocViewOrderSchema = "CREATE VIEW label_source AS SELECT label FROM labels;\n" +
	"CREATE VIEW labels AS SELECT $1abc$ FROM label_source $1abc$ AS label;\n"

// TestSchemaApplyOrdersAClickHouseViewByItsCodeNotItsHeredocsE2E applies
// heredocViewOrderSchema in a scratch database of each ClickHouse server CI
// starts, 26.10 and 24.10, and reads the label through both views.
func TestSchemaApplyOrdersAClickHouseViewByItsCodeNotItsHeredocsE2E(t *testing.T) {
	for _, server := range clickHouseServers(qt.New(t)) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			database := createdDevDialectDatabase(c, server.url, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(heredocViewOrderSchema), 0o600), qt.IsNil)

			applied, err := runPtahNativeWithError("schema", "apply", "--db-url", database, "--schema-file", schema,
				"--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			conn, err := dbschema.ConnectToDatabase(c.Context(), database)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			var got string
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT label FROM label_source").Scan(&got), qt.IsNil)
			c.Assert(got, qt.Equals, " FROM label_source ")
		})
	}
}

// TestSchemaApplyOrdersAViewByItsCodeNotItsStringsE2E applies
// dollarTagViewOrderSchema on each PostgreSQL-family engine and reads the
// label through both views.
func TestSchemaApplyOrdersAViewByItsCodeNotItsStringsE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "CockroachDB", engine: dbtarget.CockroachDB},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := probeEscapeTargets[test.engine](c)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(dollarTagViewOrderSchema), 0o600), qt.IsNil)

			applied, err := runPtahNativeWithError("schema", "apply", "--db-url", target.url, "--schema-file", schema,
				"--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			db, err := sql.Open(target.driver, target.driverURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			var got string
			c.Assert(db.QueryRowContext(c.Context(), "SELECT label FROM label_source").Scan(&got), qt.IsNil)
			c.Assert(got, qt.Equals, " FROM label_source ")
		})
	}
}
