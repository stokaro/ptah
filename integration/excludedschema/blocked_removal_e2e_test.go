//go:build integration

package excludedschema_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// invocation is one command of a table row, with placeholders the fixture
// fills: <server>, <db-url>, <database>, <sql>, <hcl>, <output> and <dev>.
type invocation struct {
	target clirun.Target
	args   []string
}

// runReplaced runs the invocation with every placeholder replaced.
func runReplaced(c *qt.C, root string, replacements *strings.Replacer, run invocation) clirun.Result {
	c.Helper()
	args := make([]string, len(run.args))
	for i, argument := range run.args {
		args[i] = replacements.Replace(argument)
	}
	return clirun.Run(c, run.target, clirun.Options{Dir: root}, args...)
}

// writeFile writes contents, with <database> replaced, to name under root and
// returns the path.
func writeFile(c *qt.C, root, name, database, contents string) string {
	c.Helper()
	path := filepath.Join(root, name)
	c.Assert(os.WriteFile(path, []byte(strings.ReplaceAll(contents, "<database>", database)), 0o600), qt.IsNil)
	return path
}

// childTableDatabase is a database holding t, with one row, and u, whose row
// (1, 42, 1) sits beside the column c, the index ix on it, the column d with
// the index ixd, the foreign key u_d_fk to t and the check ck, and the trigger
// trg.
// It returns the administrative connection, the database, the URL naming the
// whole server and the URL naming the database.
func childTableDatabase(c *qt.C, engine dbtarget.Engine) (admin *sql.DB, database, server, single string) {
	c.Helper()
	admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	database = fmt.Sprintf("ptah_blocked_table_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE `"+database+"`")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+database+"`")
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE TABLE `%s`.t (id INT NOT NULL PRIMARY KEY)",
		"CREATE TABLE `%[1]s`.u (id INT NOT NULL PRIMARY KEY, c INT, d INT, KEY ix (c), KEY ixd (d), " +
			"CONSTRAINT u_d_fk FOREIGN KEY (d) REFERENCES `%[1]s`.t (id), CONSTRAINT ck CHECK (d > 0))",
		"CREATE TRIGGER `%[1]s`.trg BEFORE INSERT ON `%[1]s`.u FOR EACH ROW SET NEW.c = NEW.c",
		"INSERT INTO `%s`.t VALUES (1)",
		"INSERT INTO `%s`.u VALUES (1, 42, 1)",
	} {
		_, err := admin.ExecContext(c.Context(), fmt.Sprintf(statement, database))
		c.Assert(err, qt.IsNil)
	}
	server, err = atlasurl.WithDatabaseName(dbtarget.URL(c, engine), "")
	c.Assert(err, qt.IsNil)
	single, err = atlasurl.WithDatabaseName(dbtarget.URL(c, engine), database)
	c.Assert(err, qt.IsNil)
	return admin, database, server, single
}

// assertChildTableKept reads u back: its row, its column value, its trigger,
// its three constraints and its three indexes are all still there.
func assertChildTableKept(c *qt.C, admin *sql.DB, database string) {
	c.Helper()
	var value, triggers, constraints, indexes int
	c.Assert(admin.QueryRowContext(c.Context(), "SELECT c FROM `"+database+"`.u WHERE id = 1").Scan(&value), qt.IsNil)
	c.Assert(admin.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ? AND TRIGGER_NAME = 'trg'", database,
	).Scan(&triggers), qt.IsNil)
	c.Assert(admin.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'u'", database,
	).Scan(&constraints), qt.IsNil)
	c.Assert(admin.QueryRowContext(c.Context(),
		"SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'u'", database,
	).Scan(&indexes), qt.IsNil)
	c.Assert(value, qt.Equals, 42)
	c.Assert(triggers, qt.Equals, 1)
	c.Assert(constraints, qt.Equals, 3)
	c.Assert(indexes, qt.Equals, 3)
}

const (
	// childServerSQL declares the database and t, and not u.
	childServerSQL = "CREATE DATABASE `<database>`;\nCREATE TABLE `<database>`.t (id INT NOT NULL PRIMARY KEY);\n"
	// childSingleSQL declares t, and not u, on a URL naming the database.
	childSingleSQL = "CREATE TABLE t (id INT NOT NULL PRIMARY KEY);\n"
	// childServerHCL is childServerSQL for the compatibility binary.
	childServerHCL = `schema "<database>" {}
table "t" {
  schema = schema.<database>
  column "id" {
    null = false
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`
)

// An excluded column, index or trigger of a table the desired schema does not
// declare is the selection's to keep, and dropping the table would remove it.
// Every command that plans or applies SQL refuses, names the child, and
// leaves the table, its row and its trigger in place.
func TestMySQLFamilyExcludedChildKeepsItsTableE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			for _, test := range []struct {
				name string
				run  invocation
				want []string
			}{
				{name: "native apply, column", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "apply", "--db-url", "<server>", "--schema-file", "<sql>", "--schemas", "<database>", "--exclude", "<database>.u.c", "--auto-approve",
				}}, want: []string{`cannot drop table "<database>.u"`, "column <database>.u.c", "--exclude <database>.u)"}},
				{name: "native apply, key column", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "apply", "--db-url", "<server>", "--schema-file", "<sql>", "--schemas", "<database>", "--exclude", "<database>.u.id", "--auto-approve",
				}}, want: []string{`cannot drop table "<database>.u"`, "column <database>.u.id"}},
				{name: "native apply, index", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "apply", "--db-url", "<server>", "--schema-file", "<sql>", "--schemas", "<database>", "--exclude", "<database>.u.ix", "--auto-approve",
				}}, want: []string{`cannot drop table "<database>.u"`, "index <database>.u.ix"}},
				{name: "native apply, trigger", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "apply", "--db-url", "<server>", "--schema-file", "<sql>", "--schemas", "<database>", "--exclude", "<database>.u.trg", "--auto-approve",
				}}, want: []string{`cannot drop table "<database>.u"`, "trigger <database>.u.trg"}},
				{name: "native apply, one database", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "apply", "--db-url", "<db-url>", "--schema-file", "<single>", "--exclude", "u.c", "--auto-approve",
				}}, want: []string{`cannot drop table "u"`, "column <database>.u.c", "--exclude u)"}},
				{name: "native plan", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "plan", "--db-url", "<db-url>", "--schema-file", "<single>", "--exclude", "u.c", "--output", "<output>/blocked.plan.json",
				}}, want: []string{`cannot drop table "u"`, "column <database>.u.c"}},
				{name: "migrations plan", run: invocation{target: clirun.Ptah, args: []string{
					"migrations", "plan", "--db-url", "<db-url>", "--schema-file", "<single>", "--exclude", "u.c",
				}}, want: []string{`cannot drop table "u"`, "column <database>.u.c"}},
				{name: "migrations generate", run: invocation{target: clirun.Ptah, args: []string{
					"migrations", "generate", "--db-url", "<db-url>", "--schema-file", "<single>", "--exclude", "u.c", "--migrations-dir", "<output>",
				}}, want: []string{`cannot drop table "u"`, "column <database>.u.c"}},
				{name: "compat apply", run: invocation{target: clirun.Compat, args: []string{
					"schema", "apply", "--url", "<server>", "--to", "<hcl>", "--schema", "<database>", "--exclude", "<database>.u.c", "--auto-approve",
				}}, want: []string{`cannot drop table "<database>.u"`, "column <database>.u.c"}},
			} {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					admin, database, server, single := childTableDatabase(c, engine.admin)
					root, output := c.TempDir(), c.TempDir()
					replacements := strings.NewReplacer("<server>", server, "<db-url>", single, "<database>", database,
						"<sql>", writeFile(c, root, "server.sql", database, childServerSQL),
						"<single>", writeFile(c, root, "single.sql", database, childSingleSQL),
						"<hcl>", "file://"+filepath.ToSlash(writeFile(c, root, "server.hcl", database, childServerHCL)),
						"<output>", output)

					result := runReplaced(c, root, replacements, test.run)

					c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
					for _, want := range test.want {
						c.Assert(result.Stderr, qt.Contains, replacements.Replace(want), qt.Commentf("%s", result.Stdout))
					}
					assertChildTableKept(c, admin, database)
					files, err := os.ReadDir(output)
					c.Assert(err, qt.IsNil)
					c.Assert(files, qt.HasLen, 0)
				})
			}
		})
	}
}

// The commands that report a comparison report the blocked table drop with
// their usual exit code. The table stays as the server holds it, so neither
// it nor any of its parts reaches the SQL or the findings: no key, foreign key,
// check, index or trigger of u is dropped.
func TestMySQLFamilyBlockedTableDropIsReportedE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			for _, test := range []struct {
				name           string
				run            invocation
				exit           int
				stdout, stderr string
			}{
				{name: "native drift", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "drift", "--db-url", "<db-url>", "--schema-file", "<single>", "--exclude", "u.c",
				}}, exit: 1, stdout: "removals_blocked: 1 (destructive)"},
				{name: "native compare", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "compare", "--db-url", "<db-url>", "--schema-file", "<single>", "--exclude", "u.c",
				}}, stdout: "removals_blocked (1): table u", stderr: `warning: cannot drop table "u"`},
				{name: "native diff", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "diff", "--from", "<db-url>", "--to", "<single>", "--exclude", "u.c",
				}}, stderr: `warning: cannot drop table "u"`},
				{name: "compat diff", run: invocation{target: clirun.Compat, args: []string{
					"schema", "diff", "--from", "<server>", "--to", "<hcl>", "--dev-url", "<dev>", "--schema", "<database>", "--exclude", "<database>.u.c",
				}}, stderr: `warning: cannot drop table "<database>.u"`},
			} {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					admin, database, server, single := childTableDatabase(c, engine.admin)
					root := c.TempDir()
					replacements := strings.NewReplacer("<server>", server, "<db-url>", single, "<database>", database,
						"<single>", writeFile(c, root, "single.sql", database, childSingleSQL),
						"<hcl>", "file://"+filepath.ToSlash(writeFile(c, root, "server.hcl", database, childServerHCL)),
						"<dev>", dbtarget.URL(c, engine.dev))

					result := runReplaced(c, root, replacements, test.run)

					c.Assert(result.ExitCode, qt.Equals, test.exit, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
					c.Assert(result.Stdout, qt.Contains, replacements.Replace(test.stdout))
					c.Assert(result.Stderr, qt.Contains, replacements.Replace(test.stderr))
					c.Assert(result.Stdout, qt.Not(qt.Contains), "DROP")
					c.Assert(result.Stdout, qt.Not(qt.Contains), "_removed")
					assertChildTableKept(c, admin, database)
				})
			}
		})
	}
}

// Each way out the refusal names works: excluding the whole table leaves it
// alone, and declaring it keeps it.
func TestMySQLFamilyExcludedChildRemediesE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			for _, test := range []struct {
				name     string
				contents string
				excludes []string
			}{
				{name: "exclude the whole table", contents: childSingleSQL, excludes: []string{"--exclude", "u"}},
				{
					// The trigger is excluded too, since the declaration does not
					// describe it and the apply would otherwise drop it.
					name: "declare the table",
					contents: childSingleSQL + "CREATE TABLE u (id INT NOT NULL PRIMARY KEY, c INT, d INT, KEY ix (c), KEY ixd (d), " +
						"CONSTRAINT u_d_fk FOREIGN KEY (d) REFERENCES t (id), CONSTRAINT ck CHECK (d > 0));\n",
					excludes: []string{"--exclude", "u.c", "--exclude", "u.trg"},
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					admin, database, _, single := childTableDatabase(c, engine.admin)
					root := c.TempDir()
					schema := writeFile(c, root, "single.sql", database, test.contents)
					args := append([]string{"schema", "apply", "--db-url", single, "--schema-file", schema, "--auto-approve"}, test.excludes...)

					result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root}, args...)

					c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
					assertChildTableKept(c, admin, database)
				})
			}
		})
	}
}

// A database the desired schema records as not described is left alone,
// whatever it holds, on both binaries.
func TestMySQLFamilyNotDescribedDatabaseIsKeptE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			for _, test := range []struct {
				name     string
				contents string
				run      invocation
			}{
				{name: "native, unnamed", contents: "-- ptah:not-described schema\n", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "apply", "--db-url", "<server>", "--schema-file", "<sql>", "--schemas", "<database>", "--auto-approve",
				}}},
				{name: "native, by name", contents: "-- ptah:not-described schema \"<database>\"\n", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "apply", "--db-url", "<server>", "--schema-file", "<sql>", "--schemas", "<database>", "--auto-approve",
				}}},
				{name: "compat, by name", contents: "-- ptah:not-described schema \"<database>\"\n", run: invocation{target: clirun.Compat, args: []string{
					"schema", "apply", "--url", "<server>", "--to", "file://<sql>", "--dev-url", "<dev>", "--schema", "<database>", "--auto-approve",
				}}},
			} {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					admin, database, server := excludedTableDatabase(c, engine.admin)
					root := c.TempDir()
					replacements := strings.NewReplacer("<server>", server, "<database>", database,
						"<sql>", filepath.ToSlash(writeFile(c, root, "described.sql", database, test.contents)),
						"<dev>", dbtarget.URL(c, engine.dev))

					result := runReplaced(c, root, replacements, test.run)

					c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
					c.Assert(result.Stdout, qt.Not(qt.Contains), "DROP")
					var kept, neighboring int
					c.Assert(admin.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+database+"`.t").Scan(&kept), qt.IsNil)
					c.Assert(admin.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+database+"`.u").Scan(&neighboring), qt.IsNil)
					c.Assert(kept, qt.Equals, 1)
					c.Assert(neighboring, qt.Equals, 1)
				})
			}
		})
	}
}

// With --include naming one table, declaring its database keeps the database
// and the tables the selection leaves out, and drops only the selected table
// the desired schema does not declare. A table in the database declares it as
// a schema block does.
func TestMySQLFamilyIncludeKeepsADeclaredDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			for _, test := range []struct {
				name string
				run  invocation
			}{
				{name: "native", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "apply", "--db-url", "<server>", "--schema-file", "<sql>", "--schemas", "<database>", "--include", "<database>.u", "--auto-approve",
				}}},
				{name: "native, declared through a table", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "apply", "--db-url", "<server>", "--schema-file", "<table-sql>", "--include", "<database>.u", "--auto-approve",
				}}},
				{name: "compat", run: invocation{target: clirun.Compat, args: []string{
					"schema", "apply", "--url", "<server>", "--to", "<hcl>", "--schema", "<database>", "--include", "<database>.u", "--auto-approve",
				}}},
			} {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					admin, database, server := excludedTableDatabase(c, engine.admin)
					root := c.TempDir()
					replacements := strings.NewReplacer("<server>", server, "<database>", database,
						"<sql>", writeFile(c, root, "declared.sql", database, "CREATE DATABASE `<database>`;\n"),
						"<table-sql>", writeFile(c, root, "table.sql", database, throughTableSQL),
						"<hcl>", "file://"+filepath.ToSlash(writeFile(c, root, "declared.hcl", database, "schema \"<database>\" {}\n")))

					result := runReplaced(c, root, replacements, test.run)

					c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
					var kept, selected int
					c.Assert(admin.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+database+"`.t").Scan(&kept), qt.IsNil)
					c.Assert(admin.QueryRowContext(c.Context(),
						"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'u'", database,
					).Scan(&selected), qt.IsNil)
					c.Assert(kept, qt.Equals, 1)
					c.Assert(selected, qt.Equals, 0)
				})
			}
		})
	}
}

// A diff under the same selection prints the drop of the selected table
// alone, with no warning: the declared database is paired on both sides.
func TestMySQLFamilyIncludeDiffKeepsADeclaredDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			for _, test := range []struct {
				name string
				run  invocation
			}{
				{name: "native", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "diff", "--from", "<server>", "--to", "<sql>", "--schemas", "<database>", "--include", "<database>.u",
				}}},
				{name: "native, declared through a table", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "diff", "--from", "<server>", "--to", "<table-sql>", "--include", "<database>.u",
				}}},
				{name: "compat", run: invocation{target: clirun.Compat, args: []string{
					"schema", "diff", "--from", "<server>", "--to", "<hcl>", "--dev-url", "<dev>", "--schema", "<database>", "--include", "<database>.u",
				}}},
			} {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					_, database, server := excludedTableDatabase(c, engine.admin)
					root := c.TempDir()
					replacements := strings.NewReplacer("<server>", server, "<database>", database,
						"<sql>", writeFile(c, root, "declared.sql", database, "CREATE DATABASE `<database>`;\n"),
						"<table-sql>", writeFile(c, root, "table.sql", database, throughTableSQL),
						"<hcl>", "file://"+filepath.ToSlash(writeFile(c, root, "declared.hcl", database, "schema \"<database>\" {}\n")),
						"<dev>", dbtarget.URL(c, engine.dev))

					result := runReplaced(c, root, replacements, test.run)

					c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
					c.Assert(result.Stdout, qt.Contains, "DROP TABLE `"+database+"`.`u`")
					c.Assert(result.Stdout, qt.Not(qt.Contains), "DROP DATABASE")
					c.Assert(result.Stderr, qt.Not(qt.Contains), "cannot drop")
				})
			}
		})
	}
}

// throughTableSQL declares the database only through its table t, which
// --include <database>.u leaves out.
const throughTableSQL = "CREATE TABLE `<database>`.t (id INT NOT NULL PRIMARY KEY);\n"

// A table the selection takes into a database the server holds, whose other
// tables the selection leaves out, is created there; the database is not
// created a second time.
func TestMySQLFamilyIncludeCreatesATableInAHeldDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			admin, database, server := excludedTableDatabase(c, engine.admin)
			root := c.TempDir()
			schema := writeFile(c, root, "added.sql", database, "CREATE TABLE `<database>`.w (id INT NOT NULL PRIMARY KEY);\n")

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root},
				"schema", "apply", "--db-url", server, "--schema-file", schema, "--include", database+".w", "--auto-approve")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stdout, qt.Contains, "CREATE TABLE `"+database+"`.`w`")
			c.Assert(result.Stdout, qt.Not(qt.Contains), "CREATE SCHEMA")
			c.Assert(result.Stdout, qt.Not(qt.Contains), "CREATE DATABASE")
			var tables int
			c.Assert(admin.QueryRowContext(c.Context(),
				"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", database,
			).Scan(&tables), qt.IsNil)
			c.Assert(tables, qt.Equals, 3)
		})
	}
}

// emptyDatabase is a database that holds nothing, on a server reached by the
// URL it returns.
func emptyDatabase(c *qt.C, engine dbtarget.Engine) (admin *sql.DB, database, server string) {
	c.Helper()
	admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	database = fmt.Sprintf("ptah_unread_kind_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE `"+database+"`")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+database+"`")
		c.Check(err, qt.IsNil)
	})
	server, err = atlasurl.WithDatabaseName(dbtarget.URL(c, engine), "")
	c.Assert(err, qt.IsNil)
	return admin, database, server
}

func databaseCount(c *qt.C, admin *sql.DB, database string) int {
	c.Helper()
	var count int
	c.Assert(admin.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", database).Scan(&count), qt.IsNil)
	return count
}

// A whole-kind exclusion leaves the kind unread, so even an empty database is
// not dropped: the refusal says which kind was not read.
func TestMySQLFamilyUnreadKindBlocksADatabaseDropE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			for _, test := range []struct {
				name string
				run  invocation
			}{
				{name: "native", run: invocation{target: clirun.Ptah, args: []string{
					"schema", "apply", "--db-url", "<server>", "--schema-file", "<sql>", "--schemas", "<database>",
					"--exclude", "*[type=function]", "--auto-approve",
				}}},
				{name: "compat", run: invocation{target: clirun.Compat, args: []string{
					"schema", "apply", "--url", "<server>", "--to", "<hcl>", "--schema", "<database>",
					"--exclude", "*[type=function]", "--auto-approve",
				}}},
			} {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					admin, database, server := emptyDatabase(c, engine.admin)
					root := c.TempDir()
					replacements := strings.NewReplacer("<server>", server, "<database>", database,
						"<sql>", writeFile(c, root, "empty.sql", database, "-- No database is declared.\n"),
						"<hcl>", "file://"+filepath.ToSlash(writeFile(c, root, "empty.hcl", database, "// No database is declared.\n")))

					result := runReplaced(c, root, replacements, test.run)

					c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
					c.Assert(result.Stderr, qt.Contains, `--exclude "*[type=function]" keeps routines out of the read`)
					c.Assert(result.Stderr, qt.Contains, "--exclude "+database+")")
					c.Assert(databaseCount(c, admin, database), qt.Equals, 1)
				})
			}
		})
	}
}

// Each way out the unread-kind refusal names works: excluding the database
// leaves it alone, declaring it keeps it, and dropping the selector drops it.
func TestMySQLFamilyUnreadKindRemediesE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			for _, test := range []struct {
				name     string
				contents string
				selector []string
				left     int
			}{
				{
					name: "exclude the database", contents: "-- No database is declared.\n",
					selector: []string{"--exclude", "*[type=function]", "--exclude", "<database>"}, left: 1,
				},
				{
					name: "declare the database", contents: "CREATE DATABASE `<database>`;\n",
					selector: []string{"--exclude", "*[type=function]"}, left: 1,
				},
				{name: "run without the selector", contents: "-- No database is declared.\n", left: 0},
			} {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					admin, database, server := emptyDatabase(c, engine.admin)
					root := c.TempDir()
					schema := writeFile(c, root, "schema.sql", database, test.contents)
					args := []string{"schema", "apply", "--db-url", server, "--schema-file", schema, "--schemas", database, "--auto-approve"}
					for _, argument := range test.selector {
						args = append(args, strings.ReplaceAll(argument, "<database>", database))
					}

					result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root}, args...)

					c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
					c.Assert(databaseCount(c, admin, database), qt.Equals, test.left)
				})
			}
		})
	}
}

// postgresChildTable is a schema in the PostgreSQL test database holding t,
// with one row, and u, whose row (1, 42, 1) sits beside the column c and the
// index ix, the column d with the index ixd, a foreign key to t, a check and
// the trigger trg. The trigger's function lives in public, outside the schema
// a run reads. It returns the connection and the schema.
func postgresChildTable(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	schema := fmt.Sprintf("ptah_blocked_table_%d", time.Now().UnixNano())
	for _, statement := range []string{
		"CREATE SCHEMA %s",
		"CREATE TABLE %s.t (id int PRIMARY KEY)",
		"CREATE TABLE %[1]s.u (id int PRIMARY KEY, c int, d int REFERENCES %[1]s.t (id), CHECK (d > 0))",
		"CREATE INDEX ix ON %s.u (c)",
		"CREATE INDEX ixd ON %s.u (d)",
		"CREATE FUNCTION public.%s_trg() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$",
		"CREATE TRIGGER trg BEFORE INSERT ON %[1]s.u FOR EACH ROW EXECUTE FUNCTION public.%[1]s_trg()",
		"INSERT INTO %s.t VALUES (1)",
		"INSERT INTO %s.u VALUES (1, 42, 1)",
	} {
		_, err := conn.ExecContext(c.Context(), fmt.Sprintf(statement, schema))
		c.Assert(err, qt.IsNil)
	}
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		c.Check(err, qt.IsNil)
		_, err = conn.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS public."+schema+"_trg()")
		c.Check(err, qt.IsNil)
	})
	return conn, schema
}

// rowValue reads the c of u's row 1, through statement, from conn.
func rowValue(c *qt.C, conn *dbschema.DatabaseConnection, statement string) int {
	c.Helper()
	var value int
	c.Assert(conn.QueryRowContext(c.Context(), statement).Scan(&value), qt.IsNil)
	return value
}

// The PostgreSQL schema keeps the table that holds an excluded column or
// index on both binaries, and excluding the whole table lets the run go on.
func TestPostgreSQLExcludedChildKeepsItsTableE2E(t *testing.T) {
	for _, test := range []struct {
		name string
		run  invocation
		want string
	}{
		{name: "native, column", run: invocation{target: clirun.Ptah, args: []string{
			"schema", "apply", "--db-url", "<db-url>", "--schema-file", "<sql>", "--schemas", "<database>", "--exclude", "<database>.u.c", "--auto-approve",
		}}, want: "column <database>.u.c"},
		{name: "native, index", run: invocation{target: clirun.Ptah, args: []string{
			"schema", "apply", "--db-url", "<db-url>", "--schema-file", "<sql>", "--schemas", "<database>", "--exclude", "<database>.u.ix", "--auto-approve",
		}}, want: "index <database>.u.ix"},
		{name: "compat, column", run: invocation{target: clirun.Compat, args: []string{
			"schema", "apply", "--url", "<db-url>", "--to", "<hcl>", "--schema", "<database>", "--exclude", "<database>.u.c", "--auto-approve",
		}}, want: "column <database>.u.c"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schema := postgresChildTable(c)
			root := c.TempDir()
			replacements := strings.NewReplacer("<db-url>", dbtarget.URL(c, dbtarget.PostgreSQL), "<database>", schema,
				"<sql>", writeFile(c, root, "schema.sql", schema, "CREATE SCHEMA <database>;\nCREATE TABLE <database>.t (id int PRIMARY KEY);\n"),
				"<hcl>", "file://"+filepath.ToSlash(writeFile(c, root, "schema.hcl", schema, postgresChildHCL)))

			result := runReplaced(c, root, replacements, test.run)

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stderr, qt.Contains, replacements.Replace(`cannot drop table "<database>.u"`))
			c.Assert(result.Stderr, qt.Contains, replacements.Replace(test.want))
			c.Assert(rowValue(c, conn, "SELECT c FROM "+schema+".u WHERE id = 1"), qt.Equals, 42)
		})
	}
}

func TestPostgreSQLExcludedChildRemedyE2E(t *testing.T) {
	c := qt.New(t)
	conn, schema := postgresChildTable(c)
	root := c.TempDir()
	file := writeFile(c, root, "schema.sql", schema, "CREATE SCHEMA <database>;\nCREATE TABLE <database>.t (id int PRIMARY KEY);\n")

	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root}, "schema", "apply", "--db-url", dbtarget.URL(c, dbtarget.PostgreSQL),
		"--schema-file", file, "--schemas", schema, "--exclude", schema+".u", "--auto-approve")

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
	c.Assert(rowValue(c, conn, "SELECT c FROM "+schema+".u WHERE id = 1"), qt.Equals, 42)
}

// The commands that report a comparison report the blocked table drop on
// PostgreSQL too, and print no statement and count no removal for u's key,
// foreign key, check, indexes or trigger.
func TestPostgreSQLBlockedTableDropIsReportedE2E(t *testing.T) {
	for _, test := range []struct {
		name           string
		args           []string
		exit           int
		stdout, stderr string
	}{
		{
			name: "drift", args: []string{"schema", "drift", "--db-url", "<db-url>", "--schema-file", "<sql>", "--schemas", "<database>", "--exclude", "<database>.u.c"},
			exit: 1, stdout: "removals_blocked: 1 (destructive)",
		},
		{
			name: "compare", args: []string{"schema", "compare", "--db-url", "<db-url>", "--schema-file", "<sql>", "--schemas", "<database>", "--exclude", "<database>.u.c"},
			stdout: "removals_blocked (1): table <database>.u", stderr: `warning: cannot drop table "<database>.u"`,
		},
		{
			name: "diff", args: []string{"schema", "diff", "--from", "<db-url>", "--to", "<sql>", "--schemas", "<database>", "--exclude", "<database>.u.c"},
			stderr: `warning: cannot drop table "<database>.u"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schema := postgresChildTable(c)
			root := c.TempDir()
			replacements := strings.NewReplacer("<db-url>", dbtarget.URL(c, dbtarget.PostgreSQL), "<database>", schema,
				"<sql>", writeFile(c, root, "schema.sql", schema, "CREATE SCHEMA <database>;\nCREATE TABLE <database>.t (id int PRIMARY KEY);\n"))

			result := runReplaced(c, root, replacements, invocation{target: clirun.Ptah, args: test.args})

			c.Assert(result.ExitCode, qt.Equals, test.exit, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stdout, qt.Contains, replacements.Replace(test.stdout))
			c.Assert(result.Stderr, qt.Contains, replacements.Replace(test.stderr))
			c.Assert(result.Stdout, qt.Not(qt.Contains), "DROP")
			c.Assert(result.Stdout, qt.Not(qt.Contains), "_removed")
			c.Assert(rowValue(c, conn, "SELECT c FROM "+schema+".u WHERE id = 1"), qt.Equals, 42)
			// PostgreSQL 18 records each NOT NULL as a constraint row too, so the
			// count names the primary key, the foreign key and the check.
			c.Assert(rowValue(c, conn,
				"SELECT COUNT(*) FROM pg_constraint WHERE conrelid = '"+schema+".u'::regclass AND contype IN ('p', 'f', 'c')"), qt.Equals, 3)
			c.Assert(rowValue(c, conn, "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = '"+schema+"' AND tablename = 'u'"), qt.Equals, 3)
			c.Assert(rowValue(c, conn, "SELECT COUNT(*) FROM pg_trigger WHERE tgrelid = '"+schema+".u'::regclass AND NOT tgisinternal"), qt.Equals, 1)
		})
	}
}

// postgresChildHCL declares the schema and t, and not u.
const postgresChildHCL = `schema "<database>" {}
table "t" {
  schema = schema.<database>
  column "id" {
    null = false
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`

// sqlServerChildTable is a scratch SQL Server database whose dbo.u row
// (1, 42, 1) sits beside the column c and the index ix, the column d with the
// index ixd, the foreign key u_d_fk to dbo.t, the check ck and the trigger
// trg. It returns a connection to it and the URL naming it.
func sqlServerChildTable(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, target := sqlServerScratchDatabase(c)
	for _, statement := range []string{
		"CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY)",
		"CREATE TABLE dbo.u (id int NOT NULL PRIMARY KEY, c int, d int, " +
			"CONSTRAINT u_d_fk FOREIGN KEY (d) REFERENCES dbo.t (id), CONSTRAINT ck CHECK (d > 0))",
		"CREATE INDEX ix ON dbo.u (c)",
		"CREATE INDEX ixd ON dbo.u (d)",
		"CREATE TRIGGER dbo.trg ON dbo.u AFTER INSERT AS SET NOCOUNT ON",
		"INSERT INTO dbo.t VALUES (1)",
		"INSERT INTO dbo.u VALUES (1, 42, 1)",
	} {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	return conn, target
}

// sqlServerScratchDatabase is an empty SQL Server database, dropped when the
// test ends. It returns a connection to it and the URL naming it.
func sqlServerScratchDatabase(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.SQLServer)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	database := fmt.Sprintf("ptah_blocked_table_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+database)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(),
			fmt.Sprintf("ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", database))
		c.Check(err, qt.IsNil)
	})
	target := sqlServerDatabaseURL(c, adminURL, database)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn, target
}

// sqlServerDatabaseURL names database in a SQL Server URL's query.
func sqlServerDatabaseURL(c *qt.C, raw, database string) string {
	c.Helper()
	parsed, err := url.Parse(raw)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("database", database)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// The SQL Server table that holds an excluded column or index is kept, and
// excluding the whole table, by the name the connection's schema reaches it
// by, lets the run go on.
func TestSQLServerExcludedChildKeepsItsTableE2E(t *testing.T) {
	for _, test := range []struct {
		exclude, want string
	}{
		{exclude: "u.c", want: "column dbo.u.c"},
		{exclude: "u.ix", want: "index dbo.u.ix"},
	} {
		t.Run(test.exclude, func(t *testing.T) {
			c := qt.New(t)
			conn, target := sqlServerChildTable(c)
			root := c.TempDir()
			file := writeFile(c, root, "schema.sql", "", "CREATE TABLE t (id int NOT NULL PRIMARY KEY);\n")

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root},
				"schema", "apply", "--db-url", target, "--schema-file", file, "--exclude", test.exclude, "--auto-approve")

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stderr, qt.Contains, `cannot drop table "dbo.u"`)
			c.Assert(result.Stderr, qt.Contains, test.want)
			c.Assert(result.Stderr, qt.Contains, "(--exclude u)")
			c.Assert(rowValue(c, conn, "SELECT c FROM dbo.u WHERE id = 1"), qt.Equals, 42)
		})
	}
}

func TestSQLServerExcludedChildRemedyE2E(t *testing.T) {
	c := qt.New(t)
	conn, target := sqlServerChildTable(c)
	root := c.TempDir()
	file := writeFile(c, root, "schema.sql", "", "CREATE TABLE t (id int NOT NULL PRIMARY KEY);\n")

	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root},
		"schema", "apply", "--db-url", target, "--schema-file", file, "--exclude", "u", "--auto-approve")

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
	c.Assert(rowValue(c, conn, "SELECT c FROM dbo.u WHERE id = 1"), qt.Equals, 42)
}

// The commands that report a comparison report the blocked table drop on SQL
// Server too, and print no statement and count no removal for u's key,
// foreign key, check, indexes or trigger.
func TestSQLServerBlockedTableDropIsReportedE2E(t *testing.T) {
	for _, test := range []struct {
		name           string
		args           []string
		exit           int
		stdout, stderr string
	}{
		{
			name: "drift", args: []string{"schema", "drift", "--db-url", "<db-url>", "--schema-file", "<sql>", "--exclude", "u.c"},
			exit: 1, stdout: "removals_blocked: 1 (destructive)",
		},
		{
			name: "compare", args: []string{"schema", "compare", "--db-url", "<db-url>", "--schema-file", "<sql>", "--exclude", "u.c"},
			stdout: "removals_blocked (1): table dbo.u", stderr: `warning: cannot drop table "dbo.u"`,
		},
		{
			name: "diff", args: []string{"schema", "diff", "--from", "<db-url>", "--to", "<sql>", "--exclude", "u.c"},
			stderr: `warning: cannot drop table "dbo.u"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, target := sqlServerChildTable(c)
			root := c.TempDir()
			replacements := strings.NewReplacer("<db-url>", target,
				"<sql>", writeFile(c, root, "schema.sql", "", "CREATE TABLE t (id int NOT NULL PRIMARY KEY);\n"))

			result := runReplaced(c, root, replacements, invocation{target: clirun.Ptah, args: test.args})

			c.Assert(result.ExitCode, qt.Equals, test.exit, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stdout, qt.Contains, test.stdout)
			c.Assert(result.Stderr, qt.Contains, test.stderr)
			c.Assert(result.Stdout, qt.Not(qt.Contains), "DROP")
			c.Assert(result.Stdout, qt.Not(qt.Contains), "_removed")
			c.Assert(rowValue(c, conn, "SELECT c FROM dbo.u WHERE id = 1"), qt.Equals, 42)
			c.Assert(rowValue(c, conn,
				"SELECT COUNT(*) FROM sys.objects WHERE parent_object_id = OBJECT_ID('dbo.u') AND type IN ('PK', 'F', 'C', 'TR')"), qt.Equals, 4)
			c.Assert(rowValue(c, conn, "SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.u') AND name IS NOT NULL"), qt.Equals, 3)
		})
	}
}
