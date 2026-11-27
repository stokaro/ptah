//go:build integration

package excludedschema_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// Replaying a migration directory does not turn its excluded table into
// permission to drop the table's database in the next migration.
func TestMySQLFamilyMigrateDiffRefusesExcludedDatabaseDropE2E(t *testing.T) {
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(t)
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			directory := filepath.Join(root, "migrations")
			c.Assert(os.Mkdir(directory, 0o700), qt.IsNil)
			database := fmt.Sprintf("ptah_excluded_history_%d", time.Now().UnixNano())
			migration := fmt.Sprintf("CREATE DATABASE `%[1]s`;\nCREATE TABLE `%[1]s`.t (id INT PRIMARY KEY);\nCREATE TABLE `%[1]s`.u (id INT PRIMARY KEY);\n", database)
			file := filepath.Join(directory, "1_init.sql")
			c.Assert(os.WriteFile(file, []byte(migration), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(root, "empty.hcl"), []byte("// No database is declared.\n"), 0o600), qt.IsNil)
			project := fmt.Sprintf(`env "protected" {
  src = "file://empty.hcl"
  dev = %q
  exclude = [%q]
  migration {
    dir = "file://migrations"
  }
}
`, dbtarget.URL(c, engine.dev), database+".t")
			c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
			hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root}, "migrate", "hash", "--dir", "file://migrations")
			c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", hashed.Stdout, hashed.Stderr))
			checksumFile := filepath.Join(directory, "atlas.sum")
			checksum, err := os.ReadFile(checksumFile)
			c.Assert(err, qt.IsNil)

			result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root}, "migrate", "diff", "remove", "--env", "protected")

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stderr, qt.Contains, "cannot drop database")
			c.Assert(result.Stderr, qt.Contains, database+".t")
			files, err := os.ReadDir(directory)
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 2)
			retained, err := os.ReadFile(file)
			c.Assert(err, qt.IsNil)
			c.Assert(string(retained), qt.Equals, migration)
			retainedChecksum, err := os.ReadFile(checksumFile)
			c.Assert(err, qt.IsNil)
			c.Assert(retainedChecksum, qt.DeepEquals, checksum)
		})
	}
}
