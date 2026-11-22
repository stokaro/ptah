//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/atlascompat"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/migration/migrationfile"
)

// memoryDevURL is a dev database that lives in its connection's one session:
// any other connection to the same URL reaches a new, empty database.
const memoryDevURL = "sqlite://file?mode=memory"

// A rollback derived with --plan replays the history on the dev database and
// compares it with the target. With the dev database in memory, the shipped
// binaries roll back from version 3 to version 2 and drop c alone; read through
// a second connection, the dev database is empty, and the run drops a and b as
// well while the revision table still says version 2.
func TestMigrationsDownPlanInMemoryDevDatabaseE2E(t *testing.T) {
	// Each binary spells the target and the directory its own way: urlFlag
	// takes the target URL, and dirFlag takes the directory path behind
	// dirScheme.
	tests := []struct {
		name      string
		binary    clirun.Target
		urlFlag   string
		dirFlag   string
		dirScheme string
		apply     []string
		down      []string
	}{
		{
			name:    "native migrations down",
			binary:  clirun.Ptah,
			urlFlag: "--db-url",
			dirFlag: "--migrations-dir",
			apply:   []string{"migrations", "up", "--dir-format", "atlas"},
			down: []string{
				"migrations", "down", "--plan", "--dir-format", "atlas", "--shadow-db", memoryDevURL,
				"--target", "2", "--confirm",
			},
		},
		{
			name:      "compatibility migrate down",
			binary:    clirun.Compat,
			urlFlag:   "--url",
			dirFlag:   "--dir",
			dirScheme: "file://",
			apply:     []string{"migrate", "apply"},
			down:      []string{"migrate", "down", "--plan", "--dev-url", memoryDevURL, "--to-version", "2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			migrations := writeMemoryDevHistory(c, dir)
			targetURL := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db"))
			target := []string{test.urlFlag, targetURL, test.dirFlag, test.dirScheme + filepath.ToSlash(migrations)}
			opts := clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}

			applied := clirun.Run(c, test.binary, opts, append(test.apply, target...)...)
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(memoryDevTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})

			rolledBack := clirun.Run(c, test.binary, opts, append(test.down, target...)...)

			c.Assert(rolledBack.ExitCode, qt.Equals, 0,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", rolledBack.Stdout, rolledBack.Stderr))
			c.Assert(rolledBack.Stdout, qt.Contains, "Derived rollback plan: 1 statements\n  DROP TABLE \"c\"\n")
			c.Assert(memoryDevTables(c, targetURL), qt.DeepEquals, []string{"a", "b"})
		})
	}
}

// writeMemoryDevHistory writes an Atlas-format directory with its atlas.sum,
// whose versions 1, 2 and 3 create a, b and c, and returns its path. No file
// carries a down body, so the only rollback is the derived one.
func writeMemoryDevHistory(c *qt.C, dir string) string {
	c.Helper()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	for name, body := range map[string]string{
		"1_a.sql": "CREATE TABLE a (id INTEGER PRIMARY KEY);\n",
		"2_b.sql": "CREATE TABLE b (id INTEGER PRIMARY KEY);\n",
		"3_c.sql": "CREATE TABLE c (id INTEGER PRIMARY KEY);\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	sum, err := atlascompat.ComputeSum(os.DirFS(migrations), migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)
	return migrations
}

// memoryDevTables reads back which of the history's tables the target holds.
func memoryDevTables(c *qt.C, targetURL string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(),
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('a', 'b', 'c') ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	tables := make([]string, 0, 3)
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		tables = append(tables, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}
