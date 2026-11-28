//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A whole-server read takes the databases --schemas names by the server's own
// rule: byte for byte on a server started with lower_case_table_names=0, and
// lower-cased on one that folds names. MariaDB answers a name condition on
// SCHEMATA by neither rule, so the reader lists the databases and picks them
// itself (stokaro/ptah#6821). A whole-server apply names the database of each
// statement it sends, since its session selects none (stokaro/ptah#6822).

// wholeServerExactServers keep names apart by case, one per server CI starts
// that way and lets a test create databases on.
var wholeServerExactServers = []struct {
	name   string
	server dbtarget.Engine
}{
	{name: "MySQL 26.10", server: dbtarget.MySQLAdmin},
	{name: "MariaDB 12.3", server: dbtarget.MariaDBAdmin},
	{name: "MariaDB 11.8", server: dbtarget.MariaDB118},
}

// wholeServerScratch is a scratch server reached through a URL that names no
// database, after asserting the server's lower_case_table_names.
func wholeServerScratch(c *qt.C, server dbtarget.Engine, folding int) (scratch mysqlScratch, serverURL string) {
	c.Helper()
	scratch = newMySQLFamilyScratch(c, server)
	var setting int
	c.Assert(scratch.admin.QueryRowContext(c.Context(), "SELECT @@lower_case_table_names").Scan(&setting), qt.IsNil)
	c.Assert(setting, qt.Equals, folding)
	serverURL, err := atlasurl.WithDatabaseName(scratch.adminURL, "")
	c.Assert(err, qt.IsNil)
	return scratch, serverURL
}

// inspectedDatabaseRE matches the line that opens a database in the HCL
// `schema inspect` prints.
var inspectedDatabaseRE = regexp.MustCompile(`(?m)^schema "([^"]+)" \{$`)

// inspectedDatabases runs `ptah schema inspect` over the whole server for
// --schemas requested, and returns the databases it describes.
func inspectedDatabases(c *qt.C, serverURL, requested string) []string {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"schema", "inspect", "--db-url", serverURL, "--schemas", requested)
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
	var names []string
	for _, match := range inspectedDatabaseRE.FindAllStringSubmatch(result.Stdout, -1) {
		names = append(names, match[1])
	}
	return names
}

// On a server that keeps names apart, a request in another case names no
// database, whatever else the request holds or in what order.
func TestSchemaInspectMySQLFamilyWholeServerExactNamesE2E(t *testing.T) {
	tests := []struct {
		name      string
		requested string
		want      []string
	}{
		{name: "the other case first", requested: "<DB>,<db>", want: []string{"<db>"}},
		{name: "the other case alone", requested: "<DB>"},
		{name: "the other case beside another database", requested: "<DB>,<other>", want: []string{"<other>"}},
	}
	for _, server := range wholeServerExactServers {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch, serverURL := wholeServerScratch(c, server.server, 0)
				held, _ := scratch.database(c, "names")
				other, _ := scratch.database(c, "names_other")
				names := strings.NewReplacer("<db>", held, "<DB>", strings.ToUpper(held), "<other>", other)
				var want []string
				for _, name := range test.want {
					want = append(want, names.Replace(name))
				}

				got := inspectedDatabases(c, serverURL, names.Replace(test.requested))

				c.Assert(got, qt.DeepEquals, want)
			})
		}
	}
}

// On a server that folds names, a request in upper case names the database
// the server stores in lower case, and s names s alone: the long s is a lower
// case letter of its own, which case folding would join to s.
func TestSchemaInspectMySQLFamilyWholeServerFoldedNamesE2E(t *testing.T) {
	tests := []struct {
		name      string
		requested string
		want      string
	}{
		{name: "upper case", requested: "<DB>_S", want: "<db>_s"},
		{name: "s", requested: "<db>_s", want: "<db>_s"},
		{name: "the long s", requested: "<db>_ſ", want: "<db>_ſ"},
	}
	for _, server := range foldedScopeServers {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch, serverURL := wholeServerScratch(c, server.server, 1)
				held, _ := scratch.database(c, "folded")
				for _, name := range []string{held + "_s", held + "_ſ"} {
					createMySQLDatabase(c, c.Context(), scratch.admin, name)
					c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), scratch.admin, name) })
				}
				names := strings.NewReplacer("<db>", held, "<DB>", strings.ToUpper(held))

				got := inspectedDatabases(c, serverURL, names.Replace(test.requested))

				c.Assert(got, qt.DeepEquals, []string{names.Replace(test.want)})
			})
		}
	}
}

// heldDatabases is which of names the server holds a database under, compared
// as bytes, in byte order.
func heldDatabases(c *qt.C, scratch mysqlScratch, names ...string) []string {
	c.Helper()
	rows, err := scratch.admin.QueryContext(c.Context(),
		"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY CAST(SCHEMA_NAME AS BINARY)")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var found []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		if slices.Contains(names, name) {
			found = append(found, name)
		}
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// An apply whose --schemas spells the database in another case first still
// reads it, and drops it where the desired schema declares nothing, as MySQL
// does; the database beside it is not part of the run.
func TestSchemaApplyMySQLFamilyWholeServerExactNamesE2E(t *testing.T) {
	for _, server := range wholeServerExactServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch, serverURL := wholeServerScratch(c, server.server, 0)
			held, _ := scratch.builtAs(c, "names", "CREATE TABLE t (id INT NOT NULL PRIMARY KEY);\n")
			other, _ := scratch.database(c, "names_other")
			schema := filepath.Join(c.TempDir(), "empty.sql")
			c.Assert(os.WriteFile(schema, []byte("-- No database is declared.\n"), 0o600), qt.IsNil)

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				"schema", "apply", "--db-url", serverURL, "--schema-file", schema,
				"--schemas", strings.ToUpper(held)+","+held, "--auto-approve")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
			c.Assert(heldDatabases(c, scratch, held, other), qt.DeepEquals, []string{other})
		})
	}
}

// A whole-server apply that empties a database drops its trigger as well as
// its routines, views and tables, and says once that it keeps no apply log,
// since its session selects no database to keep the log in.
func TestSchemaApplyMySQLFamilyWholeServerDropsATriggerE2E(t *testing.T) {
	for _, server := range wholeServerExactServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch, serverURL := wholeServerScratch(c, server.server, 0)
			held, _ := scratch.builtAs(c, "trigger",
				"CREATE TABLE u (id INT NOT NULL PRIMARY KEY, c INT);\n"+
					"CREATE VIEW v AS SELECT id FROM u;\n"+
					"CREATE TRIGGER trg BEFORE INSERT ON u FOR EACH ROW SET NEW.c = 1;\n"+
					"CREATE FUNCTION f() RETURNS INT DETERMINISTIC RETURN 1;\n"+
					"CREATE PROCEDURE p() SELECT 1;\n")
			schema := filepath.Join(c.TempDir(), "declared.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE DATABASE `"+held+"`;\n"), 0o600), qt.IsNil)

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				"schema", "apply", "--db-url", serverURL, "--schema-file", schema, "--schemas", held, "--auto-approve")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stdout, qt.Contains, "`"+held+"`.`trg`")
			c.Assert(strings.Count(result.Stderr, "The schema apply log is not kept"), qt.Equals, 1, qt.Commentf("%s", result.Stderr))
			c.Assert(result.Stderr, qt.Not(qt.Contains), "1046")
			var objects int
			c.Assert(scratch.admin.QueryRowContext(c.Context(), `
				SELECT (SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ?)
				     + (SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ?)
				     + (SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?)`,
				held, held, held).Scan(&objects), qt.IsNil)
			c.Assert(objects, qt.Equals, 0)
			c.Assert(heldDatabases(c, scratch, held), qt.DeepEquals, []string{held})
		})
	}
}
