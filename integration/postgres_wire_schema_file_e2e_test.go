//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// postgresWireSchemaFile declares a CHECK and a UNIQUE constraint without a
// name, which the server names. CockroachDB names the CHECK check_c, where
// PostgreSQL names it t_c_check.
const postgresWireSchemaFile = "CREATE TABLE t (id int PRIMARY KEY, c int CHECK (c > 0), n text UNIQUE);\n"

// TestSchemaDiffOfAPostgresWireFileWithTheDatabaseBuiltFromItE2E diffs a
// schema file with a database the server built from it, through a postgres://
// URL and through the engine's own scheme. A postgres:// URL on CockroachDB or
// YugabyteDB reaches a server that answers for its own dialect, so the file is
// read again for that dialect before it is created on the dev database, and
// both spellings answer alike. Read for PostgreSQL alone, the file names the
// CockroachDB CHECK t_c_check, and the plan renames the check the server built
// as check_c.
func TestSchemaDiffOfAPostgresWireFileWithTheDatabaseBuiltFromItE2E(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "cockroachdb", engine: dbtarget.CockroachDB},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	}
	for _, engine := range engines {
		for _, scheme := range []string{"engine", "postgres"} {
			t.Run(engine.name+"/"+scheme, func(t *testing.T) {
				c := qt.New(t)
				adminURL := dbtarget.URL(c, engine.engine)
				admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
				c.Assert(err, qt.IsNil)
				c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
				suffix := time.Now().UnixNano()
				builtName, devName := fmt.Sprintf("ptah_wire_built_%d", suffix), fmt.Sprintf("ptah_wire_dev_%d", suffix)
				createE2EDatabase(c, c.Context(), admin, builtName)
				c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, builtName) })
				createE2EDatabase(c, c.Context(), admin, devName)
				c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, devName) })
				built, dev := replaceDatabaseName(c, adminURL, builtName), replaceDatabaseName(c, adminURL, devName)
				execPostgresFamilySQL(c, c.Context(), built, postgresWireSchemaFile)
				spell := map[string]func(string) string{
					"engine":   func(u string) string { return u },
					"postgres": func(u string) string { return postgresFamilyDriverURL(c, u) },
				}[scheme]
				schema := filepath.Join(c.TempDir(), "schema.sql")
				c.Assert(os.WriteFile(schema, []byte(postgresWireSchemaFile), 0o600), qt.IsNil)

				diffed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
					"schema", "diff", "--from", "file://"+filepath.ToSlash(schema), "--to", spell(built), "--dev-url", spell(dev))

				c.Assert(diffed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diffed.Stdout, diffed.Stderr))
				c.Assert(diffed.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
			})
		}
	}
}
