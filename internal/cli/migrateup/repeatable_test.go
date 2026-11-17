package migrateup_test

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// The command tests run a directory whose versioned migration creates an
// audit table and whose repeatable migration appends a row to it, so the
// number of rows is the number of times the repeatable body ran.

const cliRepeatableBody = "INSERT INTO repeatable_runs (name) VALUES ('audit');\n"

func writeRepeatableDirectory(c *qt.C, body string) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000001_runs.up.sql":   "CREATE TABLE repeatable_runs (name TEXT NOT NULL);\n",
		"0000000001_runs.down.sql": "DROP TABLE repeatable_runs;\n",
		"R__audit.sql":             body,
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

func cliRepeatableRuns(c *qt.C, dbPath string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(c.Context(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'repeatable_runs'`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)
	c.Assert(conn.QueryRowContext(c.Context(), `SELECT COUNT(*) FROM repeatable_runs`).Scan(&count), qt.IsNil)
	return count
}

func cliH1(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "h1:" + base64.StdEncoding.EncodeToString(sum[:])
}

// writeExpectedRepeatables writes an --expect-sequence document approving the
// given versions and repeatable work, each entry identity and checksum.
func writeExpectedRepeatables(c *qt.C, versions []int64, repeatables [][2]string) string {
	c.Helper()
	type migration struct {
		Version int64 `json:"version"`
	}
	type repeatable struct {
		Identity string `json:"identity"`
		Checksum string `json:"checksum"`
	}
	document := struct {
		Migrations  []migration  `json:"migrations"`
		Repeatables []repeatable `json:"repeatables,omitempty"`
	}{Migrations: make([]migration, 0, len(versions))}
	for _, version := range versions {
		document.Migrations = append(document.Migrations, migration{Version: version})
	}
	for _, item := range repeatables {
		document.Repeatables = append(document.Repeatables, repeatable{Identity: item[0], Checksum: item[1]})
	}
	content, err := json.Marshal(document)
	c.Assert(err, qt.IsNil)
	path := filepath.Join(c.TempDir(), "expected.json")
	c.Assert(os.WriteFile(path, content, 0o600), qt.IsNil)
	return path
}

func TestMigrateUpRepeatables_HappyPath(t *testing.T) {
	t.Run("an unchanged rerun runs nothing and reports up to date", func(t *testing.T) {
		c := qt.New(t)
		dir := writeRepeatableDirectory(c, cliRepeatableBody)
		dbPath := filepath.Join(c.TempDir(), "app.db")

		out, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Ran 1 repeatable migrations:\n  audit new "+cliH1(cliRepeatableBody)+"\n")

		out, err = runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Not(qt.Contains), "repeatable migrations:")
		c.Assert(cliRepeatableRuns(c, dbPath), qt.Equals, 1)
	})

	t.Run("a dry run names the repeatable work and runs none of it", func(t *testing.T) {
		c := qt.New(t)
		dir := writeRepeatableDirectory(c, cliRepeatableBody)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		c.Assert(os.Remove(filepath.Join(dir, "R__audit.sql")), qt.IsNil)
		_, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)
		c.Assert(err, qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "R__audit.sql"), []byte(cliRepeatableBody), 0o600), qt.IsNil)

		out, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--dry-run")
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Repeatable migrations: 1, not applied as they stand: 1\n  audit pending ")
		c.Assert(out, qt.Contains, "Would have run 1 repeatable migrations:\n  audit new "+cliH1(cliRepeatableBody)+"\n")
		c.Assert(cliRepeatableRuns(c, dbPath), qt.Equals, 0)
	})

	t.Run("an approved sequence that names the repeatable work runs it", func(t *testing.T) {
		c := qt.New(t)
		dir := writeRepeatableDirectory(c, cliRepeatableBody)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		expected := writeExpectedRepeatables(c, []int64{1}, [][2]string{{"audit", cliH1(cliRepeatableBody)}})

		out, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--expect-sequence", expected)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(cliRepeatableRuns(c, dbPath), qt.Equals, 1)
	})
}

func TestMigrateUpRepeatables_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		repeatables [][2]string
		wantErr     string
	}{
		{
			name:    "an approval that names no repeatable work",
			wantErr: `(?s).*the repeatable migrations selected under the migration lock are \[audit@h1:\S+\], and the approved repeatables are \[\].*`,
		},
		{
			name:        "an approval of other content",
			repeatables: [][2]string{{"audit", cliH1("SELECT 1;\n")}},
			wantErr:     `(?s).*the repeatable migrations selected under the migration lock are \[audit@h1:\S+\], and the approved repeatables are \[audit@h1:\S+\].*`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeRepeatableDirectory(c, cliRepeatableBody)
			dbPath := filepath.Join(c.TempDir(), "app.db")
			expected := writeExpectedRepeatables(c, []int64{1}, tc.repeatables)

			_, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--expect-sequence", expected)
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(countRevisionTables(c, dbPath), qt.Equals, 1)
			c.Assert(seqRevisions(c, dbPath), qt.HasLen, 0)
		})
	}

	t.Run("a bounded run below the latest version is refused while repeatable work is pending", func(t *testing.T) {
		c := qt.New(t)
		dir := writeRepeatableDirectory(c, cliRepeatableBody)
		seqWriteMigrations(c, dir, 2)
		dbPath := filepath.Join(c.TempDir(), "app.db")

		_, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--to-version", "1")
		c.Assert(err, qt.ErrorMatches, `(?s).*cannot run up to target version 1: .*repeatable migrations audit are pending.*`)
		c.Assert(seqRevisions(c, dbPath), qt.HasLen, 0)
	})

	t.Run("a drift check is refused over a directory with repeatable migrations", func(t *testing.T) {
		c := qt.New(t)
		dir := writeRepeatableDirectory(c, cliRepeatableBody)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		shadow := "sqlite://" + filepath.Join(c.TempDir(), "shadow.db")

		_, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--check-drift", "--shadow-db", shadow)
		c.Assert(err, qt.ErrorMatches, `(?s).*--check-drift replays the versioned history on the shadow database, .*`)
		c.Assert(countRevisionTables(c, dbPath), qt.Equals, 0)
	})
}
