//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// foreignKeySelectorDesired declares both tables and neither the foreign key
// nor an index on kids.parent_id.
const foreignKeySelectorDesired = "CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));\n" +
	"CREATE TABLE kids (id int NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id));\n"

// foreignKeySelectorEngine is a MySQL-family server, the URL scheme that names
// its dialect, and the name it gives the first foreign key of kids written
// without one: `kids_ibfk_1` on MySQL, `1` on MariaDB 12.1 and later. The
// server names that key's index after its column, parent_id. The compatibility
// surface reads the dialect from the scheme, as the community binary does.
type foreignKeySelectorEngine struct {
	name, scheme, unnamedKey string
	admin                    dbtarget.Engine
}

var foreignKeySelectorEngines = []foreignKeySelectorEngine{
	{name: "mysql", scheme: "mysql://", unnamedKey: "kids_ibfk_1", admin: dbtarget.MySQLAdmin},
	{name: "mariadb", scheme: "mariadb://", unnamedKey: "1", admin: dbtarget.MariaDBAdmin},
}

// foreignKeySelectorFixture is a database built from live, the dev database a
// compatibility diff replays the desired schema on, and the desired schema
// file.
type foreignKeySelectorFixture struct {
	database, dev, schema string
}

func newForeignKeySelectorFixture(c *qt.C, engine foreignKeySelectorEngine, live string) foreignKeySelectorFixture {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine.admin)
	_, database := scratch.builtFrom(c, live)
	_, dev := scratch.database(c, "fk_dev")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(foreignKeySelectorDesired), 0o600), qt.IsNil)
	database = engine.scheme + strings.TrimPrefix(database, "mysql://")
	dev = engine.scheme + strings.TrimPrefix(dev, "mysql://")
	return foreignKeySelectorFixture{database: database, dev: dev, schema: schema}
}

func (f foreignKeySelectorFixture) dryRun(c *qt.C, exclude string) clirun.Result {
	c.Helper()
	return runPtahIn(c, "schema", "apply", "--db-url", f.database, "--schema-file", f.schema, "--exclude", exclude, "--dry-run")
}

func (f foreignKeySelectorFixture) compare(c *qt.C, exclude string) clirun.Result {
	c.Helper()
	return runPtahIn(c, "schema", "compare", "--db-url", f.database, "--schema-file", f.schema, "--exit-code", "--exclude", exclude)
}

func (f foreignKeySelectorFixture) compatDiff(c *qt.C, exclude string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "schema", "diff",
		"--from", f.database, "--to", "file://"+filepath.ToSlash(f.schema), "--dev-url", f.dev, "--exclude", exclude)
}

// TestExcludeLeavesOutTheIndexAServerBuiltForAForeignKeyE2E excludes a foreign
// key the desired schema leaves out. The server built an index for the key,
// under the key's name or, for a key written without one, under its column's.
// The index leaves the comparison with the key, so no surface reports it.
// Kept, it is planned for a DROP INDEX the server refuses with ERROR 1553
// while the key stands; the community binary v1.3.0 plans that statement for
// `[type=fk]` on both engines.
func TestExcludeLeavesOutTheIndexAServerBuiltForAForeignKeyE2E(t *testing.T) {
	for _, engine := range foreignKeySelectorEngines {
		rows := []struct {
			name, live, exclude string
		}{
			{
				name: "a named key",
				live: "CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));\n" +
					"CREATE TABLE kids (id int NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id), " +
					"CONSTRAINT kids_parent_fk FOREIGN KEY (parent_id) REFERENCES parents (id));",
				exclude: "kids.kids_parent_fk[type=foreign_key]",
			},
			{
				name: "a key written without a name",
				live: "CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));\n" +
					"CREATE TABLE kids (id int NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id), " +
					"FOREIGN KEY (parent_id) REFERENCES parents (id));",
				exclude: "kids." + engine.unnamedKey + "[type=foreign_key]",
			},
		}
		for _, row := range rows {
			t.Run(engine.name+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				fixture := newForeignKeySelectorFixture(c, engine, row.live)

				planned := fixture.dryRun(c, row.exclude)
				compared := fixture.compare(c, row.exclude)
				diffed := fixture.compatDiff(c, row.exclude)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
				c.Assert(compared.ExitCode, qt.Equals, 0, historyComment(compared))
				c.Assert(comparisonBody(c, compared.Stdout), qt.Equals, includeNoDiffs)
				c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
				c.Assert(diffed.Stdout, qt.Equals, includeSynced)
			})
		}
	}
}

// TestExcludeKeepsAnIndexTheAuthorDeclaredUnderAForeignKeyE2E excludes a
// foreign key that stands on an index the author declared, and the desired
// schema leaves out both. The index is the declaration's, not the key's, so it
// stays in the comparison and the plan drops it, as the desired schema asks.
// The server refuses that drop with ERROR 1553 while the key needs the index,
// which tells the author the two requests conflict.
func TestExcludeKeepsAnIndexTheAuthorDeclaredUnderAForeignKeyE2E(t *testing.T) {
	const live = "CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));\n" +
		"CREATE TABLE kids (id int NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id), KEY kids_parent_idx (parent_id), " +
		"CONSTRAINT kids_parent_fk FOREIGN KEY (parent_id) REFERENCES parents (id));"
	for _, engine := range foreignKeySelectorEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newForeignKeySelectorFixture(c, engine, live)

			planned := fixture.dryRun(c, "kids.kids_parent_fk[type=foreign_key]")

			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Equals, "Planned schema changes:\nDROP INDEX `kids_parent_idx` ON `kids`;\n")
		})
	}
}
