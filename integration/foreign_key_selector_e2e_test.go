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
// compatibility diff replays the desired schema on, the desired schema file,
// and a way to read one text value back from the database.
type foreignKeySelectorFixture struct {
	database, dev, schema string
	read                  func(c *qt.C, query string) string
}

func newForeignKeySelectorFixture(c *qt.C, engine foreignKeySelectorEngine, live string) foreignKeySelectorFixture {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine.admin)
	name, database := scratch.builtFrom(c, live)
	_, dev := scratch.database(c, "fk_dev")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(foreignKeySelectorDesired), 0o600), qt.IsNil)
	database = engine.scheme + strings.TrimPrefix(database, "mysql://")
	dev = engine.scheme + strings.TrimPrefix(dev, "mysql://")
	return foreignKeySelectorFixture{database: database, dev: dev, schema: schema, read: readMySQLFamily(scratch, name)}
}

func (f foreignKeySelectorFixture) apply(c *qt.C, exclude string) clirun.Result {
	c.Helper()
	return runPtahIn(c, "schema", "apply", "--db-url", f.database, "--schema-file", f.schema, "--exclude", exclude, "--auto-approve")
}

func (f foreignKeySelectorFixture) compatApply(c *qt.C, exclude string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "schema", "apply",
		"-u", f.database, "--to", "file://"+filepath.ToSlash(f.schema), "--dev-url", f.dev, "--exclude", exclude, "--auto-approve")
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
// while the key stands; the pinned community binary plans that statement for
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

// mySQLKidsKeysAndIndexes reads the foreign keys of kids and its indexes.
const mySQLKidsKeysAndIndexes = "SELECT CONCAT_WS(' ', " +
	"(SELECT GROUP_CONCAT(CONSTRAINT_NAME ORDER BY CONSTRAINT_NAME) FROM information_schema.REFERENTIAL_CONSTRAINTS " +
	"WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'kids'), " +
	"(SELECT GROUP_CONCAT(DISTINCT INDEX_NAME ORDER BY INDEX_NAME) FROM information_schema.STATISTICS " +
	"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kids'))"

// TestExcludeEveryConstraintKeepsTheIndexAForeignKeyStandsOnE2E applies a
// desired schema without the foreign key under a selector that leaves out
// every constraint. The key stays, and the index the server built for it
// leaves the comparison with it: kept, it is planned for a DROP INDEX the
// server refuses with ERROR 1553 while the key stands, on every apply. The key
// and its index are read back, and the runs that follow find nothing to do.
func TestExcludeEveryConstraintKeepsTheIndexAForeignKeyStandsOnE2E(t *testing.T) {
	const exclude = "*[type=constraint]"
	for _, engine := range foreignKeySelectorEngines {
		rows := []struct {
			name, live, want string
		}{
			{
				name: "a named key",
				live: "CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));\n" +
					"CREATE TABLE kids (id int NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id), " +
					"CONSTRAINT kids_parent_fk FOREIGN KEY (parent_id) REFERENCES parents (id));",
				want: "kids_parent_fk kids_parent_fk,PRIMARY",
			},
			{
				name: "a key written without a name",
				live: "CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));\n" +
					"CREATE TABLE kids (id int NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id), " +
					"FOREIGN KEY (parent_id) REFERENCES parents (id));",
				want: engine.unnamedKey + " parent_id,PRIMARY",
			},
		}
		for _, row := range rows {
			t.Run(engine.name+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				fixture := newForeignKeySelectorFixture(c, engine, row.live)

				applied := fixture.apply(c, exclude)
				read := fixture.read(c, mySQLKidsKeysAndIndexes)
				planned := fixture.dryRun(c, exclude)
				compared := fixture.compare(c, exclude)
				diffed := fixture.compatDiff(c, exclude)

				c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
				c.Assert(read, qt.Equals, row.want)
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

// TestExcludeNamesAForeignKeyAsFkE2E applies a desired schema without the
// foreign key under a selector that names the key as `[type=fk]`, the type
// the community binary names a foreign key by, through both binaries. Each
// binary keeps the key and the index the server built for it, read back from
// the catalog, and a run that follows finds nothing to do. The pinned community
// binary keeps the key under these selectors and plans the index's
// DROP INDEX, which the server refuses with ERROR 1553.
func TestExcludeNamesAForeignKeyAsFkE2E(t *testing.T) {
	for _, engine := range foreignKeySelectorEngines {
		rows := []struct {
			name, live, exclude, want string
		}{
			{
				name: "a named key",
				live: "CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));\n" +
					"CREATE TABLE kids (id int NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id), " +
					"CONSTRAINT kids_parent_fk FOREIGN KEY (parent_id) REFERENCES parents (id));",
				exclude: "kids.kids_parent_fk[type=fk]",
				want:    "kids_parent_fk kids_parent_fk,PRIMARY",
			},
			{
				name: "a key written without a name",
				live: "CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));\n" +
					"CREATE TABLE kids (id int NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id), " +
					"FOREIGN KEY (parent_id) REFERENCES parents (id));",
				exclude: "kids." + engine.unnamedKey + "[type=fk]",
				want:    engine.unnamedKey + " parent_id,PRIMARY",
			},
			{
				name: "every key",
				live: "CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));\n" +
					"CREATE TABLE kids (id int NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id), " +
					"CONSTRAINT kids_parent_fk FOREIGN KEY (parent_id) REFERENCES parents (id));",
				exclude: "*[type=fk]",
				want:    "kids_parent_fk kids_parent_fk,PRIMARY",
			},
		}
		for _, row := range rows {
			t.Run(engine.name+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				native := newForeignKeySelectorFixture(c, engine, row.live)
				compat := newForeignKeySelectorFixture(c, engine, row.live)

				nativeApplied := native.apply(c, row.exclude)
				nativeRead := native.read(c, mySQLKidsKeysAndIndexes)
				compatApplied := compat.compatApply(c, row.exclude)
				compatRead := compat.read(c, mySQLKidsKeysAndIndexes)
				planned := native.dryRun(c, row.exclude)
				diffed := compat.compatDiff(c, row.exclude)

				c.Assert(nativeApplied.ExitCode, qt.Equals, 0, historyComment(nativeApplied))
				c.Assert(nativeRead, qt.Equals, row.want)
				c.Assert(compatApplied.ExitCode, qt.Equals, 0, historyComment(compatApplied))
				c.Assert(compatRead, qt.Equals, row.want)
				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
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
