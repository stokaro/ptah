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
	return newForeignKeySelectorFixtureFor(c, engine, live, foreignKeySelectorDesired)
}

// newForeignKeySelectorFixtureFor is newForeignKeySelectorFixture with the
// desired schema given.
func newForeignKeySelectorFixtureFor(c *qt.C, engine foreignKeySelectorEngine, live, desired string) foreignKeySelectorFixture {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine.admin)
	name, database := scratch.builtFrom(c, live)
	_, dev := scratch.database(c, "fk_dev")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
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
// the catalog, and a run that follows finds nothing to do. The community
// binary v1.3.0 keeps the key under these selectors and plans the index's
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

// mySQLKidsKeyIndexes reads the foreign keys of kids, then each of its indexes
// with its columns in order.
const mySQLKidsKeyIndexes = "SELECT CONCAT_WS(' ', " +
	"(SELECT GROUP_CONCAT(CONSTRAINT_NAME ORDER BY CONSTRAINT_NAME) FROM information_schema.REFERENTIAL_CONSTRAINTS " +
	"WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'kids'), " +
	"(SELECT GROUP_CONCAT(CONCAT(INDEX_NAME, ':', parts) ORDER BY INDEX_NAME SEPARATOR ' ') FROM " +
	"(SELECT INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS parts FROM information_schema.STATISTICS " +
	"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kids' GROUP BY INDEX_NAME) AS kids_indexes))"

// mySQLKidsForeignKey reads the name of the one foreign key of kids.
const mySQLKidsForeignKey = "SELECT CONSTRAINT_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS " +
	"WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 'kids'"

// declaredIndexUnderAKey is a foreign key of kids that stands on an index the
// author declared, and a desired schema that leaves out both. index is the
// index the plan adds for the key, as the server would name the one it builds,
// with its columns.
type declaredIndexUnderAKey struct {
	name, live, desired, index string
}

// declaredIndexesUnderAKey are a key over one column with a name, the same key
// without one, which the server names and whose index it names after the
// column, and a key over two columns written in the order opposite to the
// table's, on an index that has a third.
var declaredIndexesUnderAKey = []declaredIndexUnderAKey{
	{
		name: "a named key",
		live: "CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));\n" +
			"CREATE TABLE kids (id int NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id), KEY kids_parent_idx (parent_id), " +
			"CONSTRAINT kids_parent_fk FOREIGN KEY (parent_id) REFERENCES parents (id));",
		desired: foreignKeySelectorDesired,
		index:   "kids_parent_fk:parent_id",
	},
	{
		name: "a key written without a name",
		live: "CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));\n" +
			"CREATE TABLE kids (id int NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id), KEY kids_parent_idx (parent_id), " +
			"FOREIGN KEY (parent_id) REFERENCES parents (id));",
		desired: foreignKeySelectorDesired,
		index:   "parent_id:parent_id",
	},
	{
		name: "a key over two columns on a wider index",
		live: "CREATE TABLE parents (a int NOT NULL, b int NOT NULL, PRIMARY KEY (b, a));\n" +
			"CREATE TABLE kids (id int NOT NULL, pa int NOT NULL, pb int NOT NULL, note int NOT NULL, PRIMARY KEY (id), " +
			"KEY kids_parent_idx (pb, pa, note), CONSTRAINT kids_parent_fk FOREIGN KEY (pb, pa) REFERENCES parents (b, a));",
		desired: "CREATE TABLE parents (a int NOT NULL, b int NOT NULL, PRIMARY KEY (b, a));\n" +
			"CREATE TABLE kids (id int NOT NULL, pa int NOT NULL, pb int NOT NULL, note int NOT NULL, PRIMARY KEY (id));\n",
		index: "kids_parent_fk:pb,pa",
	},
}

// TestExcludeKeepsAForeignKeyOnAnIndexWhenTheDeclaredOneLeavesE2E excludes a
// foreign key that stands on an index the author declared, and the desired
// schema leaves out both. The declared index is the declaration's, so the
// plan drops it, and the key keeps an index of its own: the plan adds one over
// the key's columns, in the key's order, under the name the server gives the
// index it builds for the key, in the statement that drops the declared one.
// A comment above the statement says why. The selector leaves that index out
// with the key, so the next run plans nothing. Without the added index the
// server refuses the drop with ERROR 1553, which the community binary v1.3.0
// reaches on both engines, after the statements before it ran.
func TestExcludeKeepsAForeignKeyOnAnIndexWhenTheDeclaredOneLeavesE2E(t *testing.T) {
	excludeKeepsAForeignKeyOnAnIndex(t, foreignKeySelectorFixture.apply)
}

// TestCompatExcludeKeepsAForeignKeyOnAnIndexWhenTheDeclaredOneLeavesE2E is
// the same measurement, applied through ptah-compat.
func TestCompatExcludeKeepsAForeignKeyOnAnIndexWhenTheDeclaredOneLeavesE2E(t *testing.T) {
	excludeKeepsAForeignKeyOnAnIndex(t, foreignKeySelectorFixture.compatApply)
}

// excludeKeepsAForeignKeyOnAnIndex plans every row of declaredIndexesUnderAKey
// on every engine, applies the plan with apply and reads the result back.
func excludeKeepsAForeignKeyOnAnIndex(t *testing.T, apply func(foreignKeySelectorFixture, *qt.C, string) clirun.Result) {
	t.Helper()
	for _, engine := range foreignKeySelectorEngines {
		for _, row := range declaredIndexesUnderAKey {
			t.Run(engine.name+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				fixture := newForeignKeySelectorFixtureFor(c, engine, row.live, row.desired)
				key := fixture.read(c, mySQLKidsForeignKey)
				exclude := "kids." + key + "[type=foreign_key]"
				index, columns, _ := strings.Cut(row.index, ":")
				planned := fixture.dryRun(c, exclude)

				applied := apply(fixture, c, exclude)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Equals, "Planned schema changes:\n"+
					"-- Add index "+index+" for foreign key "+key+", which an exclude selector keeps out of scope: "+
					"the server refuses to drop kids_parent_idx while the key has no other index\n"+
					"ALTER TABLE `kids` DROP INDEX `kids_parent_idx`, ADD INDEX `"+index+"` (`"+
					strings.ReplaceAll(columns, ",", "`, `")+"`);\n")
				c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
				c.Assert(fixture.read(c, mySQLKidsKeyIndexes), qt.Equals, key+" "+row.index+" PRIMARY:id")
				converged := fixture.dryRun(c, exclude)
				c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
				c.Assert(converged.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
				diffed := fixture.compatDiff(c, exclude)
				c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
				c.Assert(diffed.Stdout, qt.Equals, includeSynced)
			})
		}
	}
}
