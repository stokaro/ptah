//go:build integration

package integration_test

// The spellings an engine writes an inline index name in, and whether Ptah
// reads the same DDL.
//
// The reader took a bare word only, so `INDEX "idx_b" (b)` left the quoted name
// in front of the column list and the element was refused with
// `expected Operator, got String` — for a statement CockroachDB accepts
// (stokaro/ptah#3328). Each row executes its own CREATE TABLE first, so the
// engine is what says the spelling is legal.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemaload"
)

type inlineIndexNameRow struct {
	name     string
	engine   dbtarget.Engine
	dialect  string
	idColumn string
	bColumn  string
	element  string
}

var inlineIndexNameRows = []inlineIndexNameRow{
	{
		name: "cockroachdb quoted", engine: dbtarget.CockroachDB, dialect: platform.CockroachDB,
		idColumn: "a INT PRIMARY KEY", bColumn: "b TEXT", element: `INDEX "idx_b_%[1]s" (b)`,
	},
	{
		name: "cockroachdb bare", engine: dbtarget.CockroachDB, dialect: platform.CockroachDB,
		idColumn: "a INT PRIMARY KEY", bColumn: "b TEXT", element: "INDEX idx_b_%[1]s (b)",
	},
	{
		name: "sqlserver bracketed", engine: dbtarget.SQLServer, dialect: platform.SQLServer,
		idColumn: "a int PRIMARY KEY", bColumn: "b nvarchar(32)", element: "INDEX [idx_b_%[1]s] (b)",
	},
	{
		name: "mysql backticked", engine: dbtarget.MySQL, dialect: platform.MySQL,
		idColumn: "a INT PRIMARY KEY", bColumn: "b varchar(32)", element: "KEY `idx_b_%[1]s` (b)",
	},
}

// TestInlineIndexNameLive reads back what the engine accepted.
func TestInlineIndexNameLive(t *testing.T) {
	for _, row := range inlineIndexNameRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(t.Context(), dbtarget.URL(c, row.engine))
			c.Assert(err, qt.IsNil)
			// Closed through Cleanup rather than a defer, so the drop below --
			// registered later and therefore run first -- still has a
			// connection. Closed by a defer, the drop reaches a closed
			// connection and the table stays in a database every other live
			// test shares.
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

			suffix := fmt.Sprintf("%d", time.Now().UnixNano()%100000000)
			table := "ptah_iix_" + suffix
			ddl := fmt.Sprintf("CREATE TABLE %s (%s, %s, %s)",
				table, row.idColumn, row.bColumn, fmt.Sprintf(row.element, suffix))

			_, execErr := conn.ExecContext(t.Context(), ddl)
			c.Assert(execErr, qt.IsNil, qt.Commentf("the engine refused %s", ddl))
			c.Cleanup(func() {
				// t.Context() is canceled by the time a cleanup runs, and the
				// drop is asserted rather than discarded: a table left behind
				// is a plan against the live database that wants to drop it,
				// which fails a test that never created it.
				_, dropErr := conn.ExecContext(context.Background(), "DROP TABLE "+table)
				c.Check(dropErr, qt.IsNil, qt.Commentf("%s stays in the shared database", table))
			})

			// The engine took it, so Ptah has to read it, and read the index
			// with a name rather than as an unnamed one.
			path := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(path, []byte(ddl+";\n"), 0o600), qt.IsNil)
			db, loadErr := schemaload.LoadContext(c.Context(), schemaload.Options{
				SchemaFiles: []string{path},
				Dialect:     row.dialect,
			})
			c.Assert(loadErr, qt.IsNil, qt.Commentf("the engine accepted %s", ddl))
			c.Assert(db.Indexes, qt.HasLen, 1)
			c.Assert(db.Indexes[0].Name, qt.Contains, "idx_b_"+suffix)
		})
	}
}
