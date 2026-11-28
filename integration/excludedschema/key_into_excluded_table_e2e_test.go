//go:build integration

package excludedschema_test

import (
	"context"
	"fmt"
	"net/url"
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

// A foreign key into a table the run excludes names a table the server holds
// and the plan leaves alone. The first apply adds the key, the second plans
// nothing, and a declaration that leaves the key out plans nothing either: the
// key and the index it stands on stay. The excluded table keeps its row
// throughout (stokaro/ptah#6773).
func TestMySQLFamilyKeyIntoAnExcludedTableConvergesE2E(t *testing.T) {
	tests := []struct{ name, reference string }{
		{name: "qualified", reference: "`<database>`.t"},
		{name: "unqualified", reference: "t"},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				admin, database, _ := emptyDatabase(c, engine.admin)
				for _, statement := range []string{
					"CREATE TABLE `" + database + "`.t (id INT NOT NULL PRIMARY KEY)",
					"INSERT INTO `" + database + "`.t VALUES (1)",
				} {
					_, err := admin.ExecContext(c.Context(), statement)
					c.Assert(err, qt.IsNil)
				}
				single, err := atlasurl.WithDatabaseName(dbtarget.URL(c, engine.admin), database)
				c.Assert(err, qt.IsNil)
				root := c.TempDir()
				declared := writeFile(c, root, "declared.sql", database,
					"CREATE TABLE w (id INT NOT NULL PRIMARY KEY, t_id INT, CONSTRAINT w_t_fk FOREIGN KEY (t_id) REFERENCES "+
						test.reference+" (id));\n")
				undeclared := writeFile(c, root, "undeclared.sql", database, "CREATE TABLE w (id INT NOT NULL PRIMARY KEY, t_id INT);\n")
				apply := func(schema, mode string) clirun.Result {
					return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root},
						"schema", "apply", "--db-url", single, "--schema-file", schema, "--exclude", "t", mode)
				}

				first := apply(declared, "--auto-approve")
				second := apply(declared, "--auto-approve")
				left := apply(undeclared, "--dry-run")

				c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", first.Stdout, first.Stderr))
				c.Assert(first.Stdout, qt.Contains, "`w_t_fk`")
				c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", second.Stdout, second.Stderr))
				c.Assert(second.Stdout, qt.Contains, "no changes")
				c.Assert(left.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", left.Stdout, left.Stderr))
				c.Assert(left.Stdout, qt.Contains, "no changes")
				var keys, rows int
				c.Assert(admin.QueryRowContext(c.Context(),
					"SELECT COUNT(*) FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = 'w' "+
						"AND REFERENCED_TABLE_NAME = 't'", database,
				).Scan(&keys), qt.IsNil)
				c.Assert(admin.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+database+"`.t").Scan(&rows), qt.IsNil)
				c.Assert(keys, qt.Equals, 1)
				c.Assert(rows, qt.Equals, 1)
			})
		}
	}
}

// A plan that changes such a key drops it and adds it again, and the dev
// database rehearsal recreates the state the comparison read before it runs
// the plan. That state holds the key, so its DROP succeeds there and the
// target takes the new delete action, on both binaries (stokaro/ptah#6847).
// The excluded table keeps its row.
func TestMySQLFamilyKeyIntoAnExcludedTableChangesOnTheDevDatabaseE2E(t *testing.T) {
	tests := []struct {
		name string
		run  invocation
	}{
		{name: "native", run: invocation{target: clirun.Ptah, args: []string{
			"schema", "apply", "--db-url", "<db-url>", "--schema-file", "<sql>", "--dev-url", "<dev>", "--exclude", "t", "--auto-approve",
		}}},
		{name: "compat", run: invocation{target: clirun.Compat, args: []string{
			"schema", "apply", "--url", "<db-url>", "--to", "file://<sql>", "--dev-url", "<dev>", "--exclude", "t", "--auto-approve",
		}}},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				admin, database, _ := emptyDatabase(c, engine.admin)
				_, dev, _ := emptyDatabase(c, engine.admin)
				for _, statement := range []string{
					"CREATE TABLE `%s`.t (id INT NOT NULL PRIMARY KEY)",
					"CREATE TABLE `%[1]s`.w (id INT NOT NULL PRIMARY KEY, t_id INT, CONSTRAINT w_t_fk FOREIGN KEY (t_id) REFERENCES `%[1]s`.t (id))",
					"INSERT INTO `%s`.t VALUES (1)",
				} {
					_, err := admin.ExecContext(c.Context(), fmt.Sprintf(statement, database))
					c.Assert(err, qt.IsNil)
				}
				single, err := atlasurl.WithDatabaseName(dbtarget.URL(c, engine.admin), database)
				c.Assert(err, qt.IsNil)
				devURL, err := atlasurl.WithDatabaseName(dbtarget.URL(c, engine.admin), dev)
				c.Assert(err, qt.IsNil)
				root := c.TempDir()
				replacements := strings.NewReplacer("<db-url>", single, "<dev>", devURL, "<sql>", filepath.ToSlash(writeFile(c, root, "schema.sql", database,
					"CREATE TABLE w (id INT NOT NULL PRIMARY KEY, t_id INT, "+
						"CONSTRAINT w_t_fk FOREIGN KEY (t_id) REFERENCES t (id) ON DELETE CASCADE);\n")))

				result := runReplaced(c, root, replacements, test.run)

				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
				var rule string
				c.Assert(admin.QueryRowContext(c.Context(),
					"SELECT DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = ? AND CONSTRAINT_NAME = 'w_t_fk'",
					database).Scan(&rule), qt.IsNil)
				c.Assert(rule, qt.Equals, "CASCADE")
				c.Assert(countOf(c, admin, "SELECT COUNT(*) FROM `"+database+"`.t"), qt.Equals, 1)
			})
		}
	}
}

// scratchPostgresDatabase is an empty database on the PostgreSQL test server,
// dropped when the test ends. It returns the URL naming it.
func scratchPostgresDatabase(c *qt.C, admin *dbschema.DatabaseConnection) string {
	c.Helper()
	name := fmt.Sprintf("ptah_key_dev_%d", time.Now().UnixNano())
	_, err := admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		c.Check(err, qt.IsNil)
	})
	parsed, err := url.Parse(dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	return parsed.String()
}

// The same change on PostgreSQL, with the key into a table of the schema the
// run reads.
func TestPostgreSQLKeyIntoAnExcludedTableChangesOnTheDevDatabaseE2E(t *testing.T) {
	tests := []struct {
		name string
		run  invocation
	}{
		{name: "native", run: invocation{target: clirun.Ptah, args: []string{
			"schema", "apply", "--db-url", "<db-url>", "--schema-file", "<sql>", "--dev-url", "<dev>",
			"--schemas", "<database>", "--exclude", "<database>.t", "--auto-approve",
		}}},
		{name: "compat", run: invocation{target: clirun.Compat, args: []string{
			"schema", "apply", "--url", "<db-url>", "--to", "file://<sql>", "--dev-url", "<dev>",
			"--schema", "<database>", "--exclude", "<database>.t", "--auto-approve",
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			schema := scratchPostgresSchema(c, conn)
			for _, statement := range []string{
				"CREATE TABLE %s.t (id int PRIMARY KEY)",
				"CREATE TABLE %[1]s.w (id int PRIMARY KEY, t_id int, CONSTRAINT w_t_fk FOREIGN KEY (t_id) REFERENCES %[1]s.t (id))",
				"INSERT INTO %s.t VALUES (1)",
			} {
				_, err := conn.ExecContext(c.Context(), fmt.Sprintf(statement, schema))
				c.Assert(err, qt.IsNil)
			}
			root := c.TempDir()
			replacements := strings.NewReplacer("<db-url>", dbtarget.URL(c, dbtarget.PostgreSQL), "<dev>", scratchPostgresDatabase(c, conn),
				"<database>", schema, "<sql>", filepath.ToSlash(writeFile(c, root, "schema.sql", schema,
					"CREATE SCHEMA <database>;\nCREATE TABLE <database>.w (id int PRIMARY KEY, t_id int, "+
						"CONSTRAINT w_t_fk FOREIGN KEY (t_id) REFERENCES <database>.t (id) ON DELETE CASCADE);\n")))

			result := runReplaced(c, root, replacements, test.run)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
			c.Assert(rowValue(c, conn, "SELECT COUNT(*) FROM pg_constraint WHERE conname = 'w_t_fk' AND confdeltype = 'c' "+
				"AND conrelid = '"+schema+".w'::regclass"), qt.Equals, 1)
			c.Assert(rowValue(c, conn, "SELECT COUNT(*) FROM "+schema+".t"), qt.Equals, 1)
		})
	}
}

// assertKeyConverges applies declared to target three ways with the selector
// arguments scope: twice with the key, then as a dry run without it. The
// first adds the key, and the others plan nothing. Afterwards count runs keys,
// which counts the key, and the excluded schema.t keeps its row.
func assertKeyConverges(c *qt.C, count func(query string) int, target, schema, declared, undeclared, keys string, scope ...string) {
	c.Helper()
	root := c.TempDir()
	apply := func(contents, mode string) clirun.Result {
		args := append([]string{"schema", "apply", "--db-url", target, "--schema-file", writeFile(c, root, "schema.sql", schema, contents)}, scope...)
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root}, append(args, mode)...)
	}

	first := apply(declared, "--auto-approve")
	second := apply(declared, "--auto-approve")
	left := apply(undeclared, "--dry-run")

	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", first.Stdout, first.Stderr))
	c.Assert(first.Stdout, qt.Contains, "w_t_fk")
	c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", second.Stdout, second.Stderr))
	c.Assert(second.Stdout, qt.Contains, "no changes")
	c.Assert(left.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", left.Stdout, left.Stderr))
	c.Assert(left.Stdout, qt.Contains, "no changes")
	c.Assert(count(keys), qt.Equals, 1)
	c.Assert(count("SELECT COUNT(*) FROM "+schema+".t"), qt.Equals, 1)
}

// On a whole server, --schemas selects the database, and the key into the table
// the exclusion leaves out converges as it does on a URL that names the
// database. The positive selection refused the key the first apply created
// (stokaro/ptah#6851).
func TestMySQLFamilyKeyIntoAnExcludedTableConvergesOnAWholeServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			admin, database, server := emptyDatabase(c, engine.admin)
			for _, statement := range []string{
				"CREATE TABLE `" + database + "`.t (id INT NOT NULL PRIMARY KEY)",
				"INSERT INTO `" + database + "`.t VALUES (1)",
			} {
				_, err := admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil)
			}
			count := func(query string) int { return countOf(c, admin, query) }

			assertKeyConverges(c, count, server, database,
				"CREATE DATABASE `<database>`;\nCREATE TABLE `<database>`.w (id INT NOT NULL PRIMARY KEY, t_id INT, "+
					"CONSTRAINT w_t_fk FOREIGN KEY (t_id) REFERENCES `<database>`.t (id));\n",
				"CREATE DATABASE `<database>`;\nCREATE TABLE `<database>`.w (id INT NOT NULL PRIMARY KEY, t_id INT);\n",
				"SELECT COUNT(*) FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '"+database+
					"' AND CONSTRAINT_NAME = 'w_t_fk'",
				"--schemas", database, "--exclude", database+".t")
		})
	}
}

// withSearchPath is the PostgreSQL test URL with its search_path set to schema.
func withSearchPath(c *qt.C, schema string) string {
	c.Helper()
	parsed, err := url.Parse(dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// A key into an excluded table converges on PostgreSQL as it does on the MySQL
// family: with --schemas, which the positive selection refused on the second
// run (stokaro/ptah#6851), and on a URL whose search_path names the schema.
func TestPostgreSQLKeyIntoAnExcludedTableConvergesE2E(t *testing.T) {
	tests := []struct {
		name, declared, undeclared string
		searchPath                 []string
		scope                      []string
	}{
		{
			name: "--schemas",
			declared: "CREATE SCHEMA <database>;\nCREATE TABLE <database>.w (id int PRIMARY KEY, t_id int, " +
				"CONSTRAINT w_t_fk FOREIGN KEY (t_id) REFERENCES <database>.t (id));\n",
			undeclared: "CREATE SCHEMA <database>;\nCREATE TABLE <database>.w (id int PRIMARY KEY, t_id int);\n",
			scope:      []string{"--schemas", "<database>", "--exclude", "<database>.t"},
		},
		{
			name:       "search_path",
			declared:   "CREATE TABLE w (id int PRIMARY KEY, t_id int, CONSTRAINT w_t_fk FOREIGN KEY (t_id) REFERENCES t (id));\n",
			undeclared: "CREATE TABLE w (id int PRIMARY KEY, t_id int);\n",
			searchPath: []string{"<database>"},
			scope:      []string{"--exclude", "t"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			schema := scratchPostgresSchema(c, conn)
			for _, statement := range []string{"CREATE TABLE %s.t (id int PRIMARY KEY)", "INSERT INTO %s.t VALUES (1)"} {
				_, err := conn.ExecContext(c.Context(), fmt.Sprintf(statement, schema))
				c.Assert(err, qt.IsNil)
			}
			target := dbtarget.URL(c, dbtarget.PostgreSQL)
			for _, path := range test.searchPath {
				target = withSearchPath(c, strings.ReplaceAll(path, "<database>", schema))
			}
			scope := make([]string, 0, len(test.scope))
			for _, value := range test.scope {
				scope = append(scope, strings.ReplaceAll(value, "<database>", schema))
			}

			count := func(query string) int { return rowValue(c, conn, query) }

			assertKeyConverges(c, count, target, schema, test.declared, test.undeclared,
				"SELECT COUNT(*) FROM pg_constraint WHERE conname = 'w_t_fk' AND contype = 'f' AND connamespace = '"+schema+"'::regnamespace",
				scope...)
		})
	}
}

// A key into an excluded table converges on SQL Server as it does on the MySQL
// family.
func TestSQLServerKeyIntoAnExcludedTableConvergesE2E(t *testing.T) {
	c := qt.New(t)
	conn, target := sqlServerScratchDatabase(c)
	for _, statement := range []string{"CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY)", "INSERT INTO dbo.t VALUES (1)"} {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}

	count := func(query string) int { return rowValue(c, conn, query) }

	assertKeyConverges(c, count, target, "dbo",
		"CREATE TABLE w (id int NOT NULL PRIMARY KEY, t_id int, CONSTRAINT w_t_fk FOREIGN KEY (t_id) REFERENCES t (id));\n",
		"CREATE TABLE w (id int NOT NULL PRIMARY KEY, t_id int);\n",
		"SELECT COUNT(*) FROM sys.foreign_keys WHERE name = 'w_t_fk' AND referenced_object_id = OBJECT_ID('dbo.t')",
		"--exclude", "t")
}
