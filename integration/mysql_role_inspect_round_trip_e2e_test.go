//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// roleRoundTripEngines are the engines whose renderer writes every role as
// CREATE ROLE IF NOT EXISTS.
var roleRoundTripEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "MySQL", engine: dbtarget.MySQLAdmin},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin},
}

// TestSchemaInspectSQLReadsBackWithARoleE2E inspects a database as SQL on a
// server that holds a role, and compares the file with the database: synced.
// Roles are server-wide, so the SQL `schema inspect` writes for any database
// carries each one as the `CREATE ROLE IF NOT EXISTS` the renderer writes,
// and the parser read IF as the role's name, so the file could not be read
// back (stokaro/ptah#3902).
func TestSchemaInspectSQLReadsBackWithARoleE2E(t *testing.T) {
	for _, engine := range roleRoundTripEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			role := fmt.Sprintf("ptah_r_%d", time.Now().UnixNano()%1e12)               // MySQL caps a role name at 32 characters
			_, err := scratch.admin.ExecContext(c.Context(), "CREATE ROLE `"+role+"`") // #nosec G202 -- a generated role name this test creates and drops
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				_, dropErr := scratch.admin.ExecContext(context.Background(), "DROP ROLE `"+role+"`") // #nosec G202 -- the role this test created
				c.Check(dropErr, qt.IsNil)
			})
			_, database := scratch.builtFrom(c, "CREATE TABLE t (id int PRIMARY KEY);")

			inspected := runPtahNative(c, "schema", "inspect", "--db-url", database, "--format", "sql")
			file := filepath.Join(c.TempDir(), "inspected.sql")
			c.Assert(os.WriteFile(file, []byte(inspected), 0o600), qt.IsNil)
			compared, err := runPtahNativeWithError("schema", "compare", "--db-url", database, "--schema-file", file, "--exit-code")

			c.Assert(inspected, qt.Contains, "CREATE ROLE IF NOT EXISTS `"+role+"`")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", compared))
		})
	}
}
