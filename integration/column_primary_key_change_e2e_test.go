//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbtarget"
)

// columnKeyEngines are the engines a key on an existing column can change on, each
// with the statement that reads the columns of the table's key back.
var columnKeyEngines = []struct {
	name    string
	dialect string
	target  func(c *qt.C) string
	keyOf   string
	// dropsKey is the words of a statement that drops the key, and wantDrops how many
	// a move of the key writes. SQLite rebuilds the table instead.
	dropsKey  string
	wantDrops int
}{
	{
		name:    "postgres",
		dialect: platform.Postgres,
		target:  func(c *qt.C) string { return pinnedDevURL(c, postgresScratchDevURL(c, "ptah_column_key"), "public") },
		keyOf: `SELECT coalesce(string_agg(a.attname, ',' ORDER BY a.attname), '') FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
WHERE i.indrelid = 'public.t'::regclass AND i.indisprimary`,
		dropsKey:  "DROP CONSTRAINT",
		wantDrops: 1,
	},
	{
		name:    "mysql",
		dialect: platform.MySQL,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "column_key")
			return url
		},
		keyOf: "SELECT COALESCE(GROUP_CONCAT(column_name ORDER BY column_name), '') FROM information_schema.statistics " +
			"WHERE table_schema = DATABASE() AND table_name = 't' AND index_name = 'PRIMARY'",
		dropsKey:  "DROP PRIMARY KEY",
		wantDrops: 1,
	},
	{
		name:    "mariadb",
		dialect: platform.MariaDB,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin).database(c, "column_key")
			return url
		},
		keyOf: "SELECT COALESCE(GROUP_CONCAT(column_name ORDER BY column_name), '') FROM information_schema.statistics " +
			"WHERE table_schema = DATABASE() AND table_name = 't' AND index_name = 'PRIMARY'",
		dropsKey:  "DROP PRIMARY KEY",
		wantDrops: 1,
	},
	{
		name:     "sqlite",
		dialect:  platform.SQLite,
		target:   sqliteScratchURL,
		keyOf:    "SELECT coalesce(group_concat(name, ','), '') FROM (SELECT name FROM pragma_table_info('t') WHERE pk > 0 ORDER BY name)",
		dropsKey: "DROP CONSTRAINT",
	},
}

// columnKeySteps are changes to the key a column declares for itself. A file of each
// step has to leave the key it says, and its rollback the key before it.
var columnKeySteps = []struct {
	name    string
	schema  string
	wantKey string
}{
	{name: "no key", schema: "CREATE TABLE t (id int, a int);\n", wantKey: ""},
	{name: "a column becomes the key", schema: "CREATE TABLE t (id int PRIMARY KEY, a int);\n", wantKey: "id"},
	{name: "another column becomes the key", schema: "CREATE TABLE t (id int, a int PRIMARY KEY);\n", wantKey: "a"},
	{name: "the key goes", schema: "CREATE TABLE t (id int, a int);\n", wantKey: ""},
}

// TestCompatMigrateDiffColumnKeyE2E_TheKeyMovesBothWays applies each step's file and
// reads the key back, then applies its rollback and reads the key before it. A file
// that only touches the columns leaves the key where it was, and on an engine with a
// column NOT NULL in the key the server stops the file.
//
// The last schema is asked for twice and has to write nothing the second time. A column
// that kept NOT NULL because it left the key too late would write a file there.
func TestCompatMigrateDiffColumnKeyE2E_TheKeyMovesBothWays(t *testing.T) {
	schemas := make([]string, 0, len(columnKeySteps)+1)
	for _, step := range columnKeySteps {
		schemas = append(schemas, step.schema)
	}
	schemas = append(schemas, columnKeySteps[len(columnKeySteps)-1].schema)
	for _, engine := range columnKeyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", engine.target(c), schemas))
			target := engine.target(c)

			c.Assert(pairs, qt.HasLen, len(columnKeySteps))
			c.Assert(strings.Count(pairs[2].Up, engine.dropsKey), qt.Equals, engine.wantDrops, qt.Commentf("%s", pairs[2].Up))
			applyEngineFile(c, target, engine.dialect, pairs[0].Up)
			for i := 1; i < len(columnKeySteps); i++ {
				applyEngineFile(c, target, engine.dialect, pairs[i].Up)
				c.Assert(heldColumns(c, target, engine.keyOf), qt.Equals, columnKeySteps[i].wantKey, qt.Commentf("%s:\n%s", columnKeySteps[i].name, pairs[i].Up))
				applyEngineFile(c, target, engine.dialect, pairs[i].Down)
				c.Assert(heldColumns(c, target, engine.keyOf), qt.Equals, columnKeySteps[i-1].wantKey, qt.Commentf("rollback of %s:\n%s", columnKeySteps[i].name, pairs[i].Down))
				applyEngineFile(c, target, engine.dialect, pairs[i].Up)
			}
		})
	}
}

// TestCompatMigrateDiffColumnKeyE2E_AnAutoIncrementColumnBecomesTheKey holds the file
// to one statement for the column and its key: MySQL and MariaDB refuse an
// AUTO_INCREMENT column that is no key.
func TestCompatMigrateDiffColumnKeyE2E_AnAutoIncrementColumnBecomesTheKey(t *testing.T) {
	for _, engine := range columnKeyEngines[1:3] {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", engine.target(c), []string{
				"CREATE TABLE t (id int, a int);\n",
				"CREATE TABLE t (id int AUTO_INCREMENT PRIMARY KEY, a int);\n",
			}))
			target := engine.target(c)

			c.Assert(pairs, qt.HasLen, 2)
			applyEngineFile(c, target, engine.dialect, pairs[0].Up)
			applyEngineFile(c, target, engine.dialect, pairs[1].Up)
			c.Assert(heldColumns(c, target, engine.keyOf), qt.Equals, "id", qt.Commentf("%s", pairs[1].Up))
			applyEngineFile(c, target, engine.dialect, pairs[1].Down)
			c.Assert(heldColumns(c, target, engine.keyOf), qt.Equals, "", qt.Commentf("%s", pairs[1].Down))
		})
	}
}

// TestCompatMigrateDiffColumnKeyE2E_AColumnBecomesTheKeyOnSQLServer holds the file
// to a key on a column SQL Server would refuse it on while it is nullable. The rollback
// names the key on a database the dev database is not, so the name has to be the file's
// and not one the server made up.
func TestCompatMigrateDiffColumnKeyE2E_AColumnBecomesTheKeyOnSQLServer(t *testing.T) {
	c := qt.New(t)
	newTarget := func() string {
		return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
			"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
	}
	keyOf := "SELECT COALESCE(STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY c.name), '') FROM sys.indexes i " +
		"JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id " +
		"JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id " +
		"WHERE i.is_primary_key = 1 AND i.object_id = OBJECT_ID('t')"

	pairs := readMigrationPairs(c, diffEngineSteps(c, "", newTarget(), []string{
		"CREATE TABLE t (id int, a int);\n",
		"CREATE TABLE t (id int PRIMARY KEY, a int);\n",
	}))
	target := newTarget()

	c.Assert(pairs, qt.HasLen, 2)
	applyEngineFile(c, target, platform.SQLServer, pairs[0].Up)
	applyEngineFile(c, target, platform.SQLServer, pairs[1].Up)
	c.Assert(heldColumns(c, target, keyOf), qt.Equals, "id")
	applyEngineFile(c, target, platform.SQLServer, pairs[1].Down)
	c.Assert(heldColumns(c, target, keyOf), qt.Equals, "")
}

// TestCompatMigrateDiffE2E_ALaterFileDropsAKeyOnAnotherDatabase writes a table with a
// primary key nobody named, then a file that drops the key, against a dev database, and
// runs both files on another. Left to the server, the key takes a name with a hash that
// differs between the two, the later file names the dev database's, and the target
// answers that it is not a constraint. The next plan reads the keys back and finds
// nothing to do. One row's table leaves no room for a suffix within the server's limit
// of 128 characters.
func TestCompatMigrateDiffE2E_ALaterFileDropsAKeyOnAnotherDatabase(t *testing.T) {
	tests := []struct {
		name  string
		table string
	}{
		{name: "a short name", table: "t"},
		{name: "a name with room for the suffix and no more", table: strings.Repeat("t", 123)},
		{name: "a name with no room for the suffix", table: strings.Repeat("t", 125)},
		{name: "the longest name", table: strings.Repeat("t", 128)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			newTarget := func() string {
				return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
					"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
			}
			first := "CREATE TABLE [" + test.table + "] (id int PRIMARY KEY, u int UNIQUE);\n"
			second := "CREATE TABLE [" + test.table + "] (id int NOT NULL, u int UNIQUE);\n"
			keys := "SELECT COALESCE(STRING_AGG(CONCAT(kc.type COLLATE DATABASE_DEFAULT, ':', c.name COLLATE DATABASE_DEFAULT), ',') WITHIN GROUP (ORDER BY kc.type, c.name), '') FROM sys.key_constraints kc " +
				"JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id " +
				"JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id " +
				"WHERE kc.parent_object_id = OBJECT_ID('[" + test.table + "]')"
			dev := newTarget()

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", dev, []string{first, second}))
			target := newTarget()
			schema := filepath.Join(c.TempDir(), "second.sql")
			c.Assert(os.WriteFile(schema, []byte(second), 0o600), qt.IsNil)

			c.Assert(pairs, qt.HasLen, 2)
			applyEngineFile(c, target, platform.SQLServer, pairs[0].Up)
			held := heldColumns(c, target, keys)
			c.Assert(held, qt.Equals, "PK:id,UQ:u")
			applyEngineFile(c, target, platform.SQLServer, pairs[1].Up)
			c.Assert(heldColumns(c, target, keys), qt.Equals, "UQ:u")
			next, err := runCompatVerb("schema", "diff", "--from", target, "--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", next))
			c.Assert(next, qt.Contains, "synced")
			applyEngineFile(c, target, platform.SQLServer, pairs[1].Down)
			c.Assert(heldColumns(c, target, keys), qt.Equals, held)
		})
	}
}
