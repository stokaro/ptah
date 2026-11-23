package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
)

// `migrate down --dev-url` replays the applied history on the dev database,
// each file as it ran, forwarded to native `migrations down` and on its own
// --format path alike. SQLite refuses VACUUM inside a transaction, so
// migration 1 applies under --tx-mode none, and its replay runs without a
// transaction. Neither path reads a transaction mode: PTAH_TX_MODE and the
// env's migration.tx_mode change nothing, and the verb takes no --tx-mode.

// compatVacuumHistory is a SQLite target at version 2 of a history whose
// migration 1 holds a VACUUM, applied with `migrate apply --tx-mode none`,
// in a project whose atlas.hcl names it, a dev database and the directory as
// env "local", with migration lines after the directory. It returns the
// project's directory and the target's path.
func compatVacuumHistory(c *qt.C, migration string) (root, targetPath string) {
	c.Helper()
	root = c.TempDir()
	c.Chdir(root)
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	for name, body := range map[string]string{
		"1_items.sql":      "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\nVACUUM;\n",
		"1_items.down.sql": "DROP TABLE items;\n",
		"2_note.sql":       "ALTER TABLE items ADD COLUMN note TEXT;\n",
		"2_note.down.sql":  "ALTER TABLE items DROP COLUMN note;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	targetPath = filepath.Join(root, "target.db")
	project := "env \"local\" {\n" +
		"  url = \"" + atlasurl.SQLiteURLFromPath(targetPath) + "\"\n" +
		"  dev = \"" + atlasurl.SQLiteURLFromPath(filepath.Join(root, "dev.db")) + "\"\n" +
		"  migration {\n    dir = \"file://migrations\"\n" + migration + "  }\n" +
		"}\n"
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
	_, stderr, code := runCompat(c, "migrate", "hash", "--dir", "file://migrations")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	_, stderr, code = runCompat(c, "migrate", "apply", "--env", "local", "--tx-mode", "none")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	return root, targetPath
}

// compatVacuumNoteColumns counts the note columns the target's items table
// holds: 1 at version 2, 0 once migration 2 is rolled back.
func compatVacuumNoteColumns(c *qt.C, targetPath string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(targetPath))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM pragma_table_info('items') WHERE name = 'note'").Scan(&count), qt.IsNil)
	return count
}

func TestAtlasMigrateDownReplaysEachFileAsItRan_HappyPath(t *testing.T) {
	for _, path := range []struct {
		name   string
		flags  []string
		stdout string
	}{
		{name: "forwarded", stdout: "Rollback plan verified on shadow database"},
		{name: "--format", flags: []string{"--format", "{{ .Current }}->{{ .Target }}"}, stdout: "2->1"},
	} {
		for _, mode := range []struct {
			name      string
			env       string
			migration string
		}{
			{name: "no mode"},
			{name: "PTAH_TX_MODE", env: "file"},
			{name: "the env's migration.tx_mode", migration: "    tx_mode = \"file\"\n"},
		} {
			t.Run(path.name+"/"+mode.name, func(t *testing.T) {
				c := qt.New(t)
				c.Setenv("PTAH_TX_MODE", mode.env)
				_, targetPath := compatVacuumHistory(c, mode.migration)

				stdout, stderr, code := runCompat(c,
					append([]string{"migrate", "down", "--env", "local", "--to-version", "1"}, path.flags...)...)

				c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
				c.Assert(stdout, qt.Contains, path.stdout)
				c.Assert(compatVacuumNoteColumns(c, targetPath), qt.Equals, 0)
			})
		}
	}
}

func TestAtlasMigrateDownTakesNoTxMode_FailurePath(t *testing.T) {
	for _, path := range []struct {
		name  string
		flags []string
	}{
		{name: "forwarded"},
		{name: "--format", flags: []string{"--format", "{{ .Current }}->{{ .Target }}"}},
	} {
		t.Run(path.name, func(t *testing.T) {
			c := qt.New(t)
			_, targetPath := compatVacuumHistory(c, "")

			stdout, stderr, code := runCompat(c,
				append([]string{"migrate", "down", "--env", "local", "--to-version", "1", "--tx-mode", "none"}, path.flags...)...)

			c.Assert(code, qt.Equals, 1)
			c.Assert(stderr, qt.Contains, "unknown flag: --tx-mode")
			c.Assert(stdout, qt.Equals, "")
			c.Assert(compatVacuumNoteColumns(c, targetPath), qt.Equals, 1)
		})
	}
}
