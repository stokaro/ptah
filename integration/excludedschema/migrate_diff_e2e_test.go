//go:build integration

package excludedschema_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// Replaying a migration directory does not turn its excluded objects into
// permission to drop what holds them in the next migration: an excluded table
// keeps its database, and an excluded column keeps its table.
func TestMySQLFamilyMigrateDiffRefusesABlockedDropE2E(t *testing.T) {
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(t)
	for _, engine := range mysqlServerEngines {
		for _, test := range []struct {
			name, source, exclude string
			want                  []string
		}{
			{
				name: "database", source: "// No database is declared.\n", exclude: "<database>.t",
				want: []string{`cannot drop database "<database>"`, "<database>.t"},
			},
			{
				name: "table", source: childServerHCL, exclude: "<database>.u.c",
				want: []string{`cannot drop table "<database>.u"`, "column <database>.u.c"},
			},
		} {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				root := c.TempDir()
				directory := filepath.Join(root, "migrations")
				c.Assert(os.Mkdir(directory, 0o700), qt.IsNil)
				database := fmt.Sprintf("ptah_excluded_history_%d", time.Now().UnixNano())
				names := strings.NewReplacer("<database>", database)
				migration := names.Replace("CREATE DATABASE `<database>`;\nCREATE TABLE `<database>`.t (id INT PRIMARY KEY);\n" +
					"CREATE TABLE `<database>`.u (id INT PRIMARY KEY, c INT);\n")
				file := filepath.Join(directory, "1_init.sql")
				c.Assert(os.WriteFile(file, []byte(migration), 0o600), qt.IsNil)
				writeFile(c, root, "source.hcl", database, test.source)
				project := fmt.Sprintf(`env "protected" {
  src = "file://source.hcl"
  dev = %q
  exclude = [%q]
  migration {
    dir = "file://migrations"
  }
}
`, dbtarget.URL(c, engine.dev), names.Replace(test.exclude))
				c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
				hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root}, "migrate", "hash", "--dir", "file://migrations")
				c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", hashed.Stdout, hashed.Stderr))
				checksumFile := filepath.Join(directory, "atlas.sum")
				checksum, err := os.ReadFile(checksumFile)
				c.Assert(err, qt.IsNil)

				result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root}, "migrate", "diff", "remove", "--env", "protected")

				c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
				for _, want := range test.want {
					c.Assert(result.Stderr, qt.Contains, names.Replace(want))
				}
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
}
