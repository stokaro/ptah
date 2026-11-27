//go:build integration

// Package excludedschema_test verifies that a selection protects objects from
// a database drop through every planning surface.
package excludedschema_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the administrative connection's driver

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

func TestMain(m *testing.M) {
	clirun.Main(m)
}

var mysqlServerEngines = []struct {
	name       string
	admin, dev dbtarget.Engine
}{
	{name: "MySQL", admin: dbtarget.MySQLAdmin, dev: dbtarget.MySQLDevServer},
	{name: "MariaDB", admin: dbtarget.MariaDBAdmin, dev: dbtarget.MariaDBDevServer},
}

func excludedTableDatabase(c *qt.C, engine dbtarget.Engine) (*sql.DB, string, string) {
	c.Helper()
	admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	database := fmt.Sprintf("ptah_excluded_database_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE `"+database+"`")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+database+"`")
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE TABLE `%s`.t (id INT NOT NULL PRIMARY KEY)",
		"CREATE TABLE `%s`.u (id INT NOT NULL PRIMARY KEY)",
		"INSERT INTO `%s`.t VALUES (1)",
		"INSERT INTO `%s`.u VALUES (1)",
	} {
		_, err := admin.ExecContext(c.Context(), fmt.Sprintf(statement, database))
		c.Assert(err, qt.IsNil)
	}
	server, err := atlasurl.WithDatabaseName(dbtarget.URL(c, engine), "")
	c.Assert(err, qt.IsNil)
	return admin, database, server
}

// A table exclusion also protects the table from DROP DATABASE. Both the
// excluded table and its neighbor retain their rows after every refusal.
func TestMySQLFamilyExcludedTablePreventsDatabaseDropE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			admin, database, server := excludedTableDatabase(c, engine.admin)
			root := c.TempDir()
			sqlFile := filepath.Join(root, "empty.sql")
			hclFile := filepath.Join(root, "empty.hcl")
			c.Assert(os.WriteFile(sqlFile, []byte("-- No database is declared.\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(hclFile, []byte("// No database is declared.\n"), 0o600), qt.IsNil)
			for _, test := range []struct {
				name   string
				target clirun.Target
				args   []string
				want   []string
			}{
				{name: "native apply", target: clirun.Ptah, args: []string{
					"schema", "apply", "--db-url", "<server>", "--schema-file", "<sql>", "--schemas", "<database>", "--exclude", "<database>.t", "--auto-approve",
				}, want: []string{"cannot drop database", "<database>.t"}},
				{name: "native diff", target: clirun.Ptah, args: []string{
					"schema", "diff", "--from", "<server>", "--to", "<sql>", "--schemas", "<database>", "--exclude", "<database>.t",
				}, want: []string{"cannot drop database", "<database>.t"}},
				{name: "native plan", target: clirun.Ptah, args: []string{
					"migrations", "plan", "--db-url", "<server>", "--schema-file", "<sql>", "--schemas", "<database>", "--exclude", "<database>.t",
				}, want: []string{"the database URL names no database"}},
				{name: "native generate", target: clirun.Ptah, args: []string{
					"migrations", "generate", "--db-url", "<server>", "--schema-file", "<sql>", "--schemas", "<database>", "--exclude", "<database>.t", "--migrations-dir", "<output>",
				}, want: []string{"the database URL names no database"}},
				{name: "compat apply", target: clirun.Compat, args: []string{
					"schema", "apply", "--url", "<server>", "--to", "<hcl>", "--schema", "<database>", "--exclude", "<database>.t", "--auto-approve",
				}, want: []string{"cannot drop database", "<database>.t"}},
				{name: "compat diff", target: clirun.Compat, args: []string{
					"schema", "diff", "--from", "<server>", "--to", "<hcl>", "--dev-url", "<dev>", "--schema", "<database>", "--exclude", "<database>.t",
				}, want: []string{"cannot drop database", "<database>.t"}},
			} {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					output := c.TempDir()
					replacements := strings.NewReplacer("<server>", server, "<database>", database, "<sql>", sqlFile,
						"<hcl>", "file://"+filepath.ToSlash(hclFile), "<output>", output, "<dev>", dbtarget.URL(c, engine.dev))
					args := make([]string, len(test.args))
					for i, argument := range test.args {
						args[i] = replacements.Replace(argument)
					}
					result := clirun.Run(c, test.target, clirun.Options{Dir: root}, args...)
					c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
					for _, want := range test.want {
						c.Assert(result.Stderr, qt.Contains, replacements.Replace(want), qt.Commentf("%s", result.Stdout))
					}
					var kept, neighboring int
					c.Assert(admin.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+database+"`.t").Scan(&kept), qt.IsNil)
					c.Assert(admin.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+database+"`.u").Scan(&neighboring), qt.IsNil)
					c.Assert(kept, qt.Equals, 1)
					c.Assert(neighboring, qt.Equals, 1)
					files, err := os.ReadDir(output)
					c.Assert(err, qt.IsNil)
					c.Assert(files, qt.HasLen, 0)
				})
			}
		})
	}
}
