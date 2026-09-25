package migrateup_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// seqWriteMigrations writes one ptah-format migration per version, each creating
// its own table, so which of them ran is a question the schema answers.
func seqWriteMigrations(c *qt.C, dir string, versions ...int) {
	c.Helper()
	for _, version := range versions {
		name := seqTable(version)
		up := filepath.Join(dir, seqFile(version, name, "up"))
		down := filepath.Join(dir, seqFile(version, name, "down"))
		c.Assert(os.WriteFile(up, []byte("CREATE TABLE "+name+" (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(down, []byte("DROP TABLE "+name+";\n"), 0o600), qt.IsNil)
	}
}

func seqTable(version int) string { return "t" + strings.Repeat("x", version) }

func seqFile(version int, name, direction string) string {
	return strings.Repeat("0", 10-len(seqItoa(version))) + seqItoa(version) + "_" + name + "." + direction + ".sql"
}

func seqItoa(value int) string {
	digits := ""
	for value > 0 || digits == "" {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}

// seqWriteExpected writes the --expect-sequence document for the given versions.
func seqWriteExpected(c *qt.C, versions ...int64) string {
	c.Helper()
	type migration struct {
		Version    int64  `json:"version"`
		VersionKey string `json:"version_key"`
	}
	document := struct {
		Migrations []migration `json:"migrations"`
	}{Migrations: make([]migration, 0, len(versions))}
	for _, version := range versions {
		document.Migrations = append(document.Migrations, migration{Version: version})
	}
	content, err := json.Marshal(document)
	c.Assert(err, qt.IsNil)
	path := filepath.Join(c.TempDir(), "expected.json")
	c.Assert(os.WriteFile(path, content, 0o600), qt.IsNil)
	return path
}

func seqTableExists(c *qt.C, dbPath, table string) bool {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

func seqRevisions(c *qt.C, dbPath string) []int64 {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(), `SELECT version FROM schema_migrations ORDER BY version`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var versions []int64
	for rows.Next() {
		var version int64
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

// seqRestoreBackwards does to the database what restoring an older backup does: the
// objects and the revision rows of the later migrations are both gone.
func seqRestoreBackwards(c *qt.C, dbPath string, versions ...int) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, version := range versions {
		_, err := conn.ExecContext(context.Background(), "DROP TABLE "+seqTable(version))
		c.Assert(err, qt.IsNil)
		_, err = conn.ExecContext(context.Background(), `DELETE FROM schema_migrations WHERE version = ?`, version)
		c.Assert(err, qt.IsNil)
	}
}

func TestMigrateUpExpectSequence(t *testing.T) {
	// The approval is taken against history at version 3, with version 4 the
	// only pending migration. Each row then changes the history before the run.
	setup := func(c *qt.C) (dir, dbPath string) {
		dir = c.TempDir()
		dbPath = filepath.Join(c.TempDir(), "approved.db")
		seqWriteMigrations(c, dir, 1, 2, 3)
		_, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)
		c.Assert(err, qt.IsNil)
		seqWriteMigrations(c, dir, 4)
		return dir, dbPath
	}

	t.Run("the approved sequence runs", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := setup(c)
		_, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
			"--expect-sequence", seqWriteExpected(c, 4))
		c.Assert(err, qt.IsNil)
		c.Assert(seqRevisions(c, dbPath), qt.DeepEquals, []int64{1, 2, 3, 4})
	})

	t.Run("a history restored backwards runs nothing it did not approve", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := setup(c)
		seqRestoreBackwards(c, dbPath, 2, 3)

		// The control: without the flag, the same database runs all three.
		control := filepath.Join(c.TempDir(), "control.db")
		seqCopyFile(c, dbPath, control)
		_, err := runUpThroughRoot("--db-url", "sqlite://"+control, "--migrations-dir", dir)
		c.Assert(err, qt.IsNil)
		c.Assert(seqRevisions(c, control), qt.DeepEquals, []int64{1, 2, 3, 4})

		out, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
			"--expect-sequence", seqWriteExpected(c, 4))
		c.Assert(err, qt.ErrorMatches, `.*selected under the migration lock are \[2 3 4\], and the approved sequence is \[4\].*`)
		c.Assert(seqRevisions(c, dbPath), qt.DeepEquals, []int64{1})
		for _, version := range []int{2, 3, 4} {
			c.Assert(seqTableExists(c, dbPath, seqTable(version)), qt.IsFalse,
				qt.Commentf("migration %d ran:\n%s", version, out))
		}
	})

	t.Run("a history that moved past the approval is refused rather than reported up to date", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := setup(c)
		_, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)
		c.Assert(err, qt.IsNil)

		_, err = runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
			"--expect-sequence", seqWriteExpected(c, 4))
		c.Assert(err, qt.ErrorMatches, `.*selected under the migration lock are \[\], and the approved sequence is \[4\].*`)
	})

	t.Run("the JSON document records the refusal as a failed run with nothing applied", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := setup(c)
		seqRestoreBackwards(c, dbPath, 3)
		out, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
			"--expect-sequence", seqWriteExpected(c, 4), "--json")
		c.Assert(err, qt.IsNotNil)
		start := strings.Index(out, "{")
		c.Assert(start >= 0, qt.IsTrue, qt.Commentf("no JSON document in:\n%s", out))
		var document struct {
			Outcome string  `json:"outcome"`
			Applied []int64 `json:"applied"`
		}
		c.Assert(json.NewDecoder(strings.NewReader(out[start:])).Decode(&document), qt.IsNil)
		c.Assert(document.Outcome, qt.Equals, "failed")
		c.Assert(document.Applied, qt.HasLen, 0)
	})
}

// TestMigrateUpExpectSequenceHistoryMovedBelowTheApproval covers the one move
// the selection cannot show: deployment B records a migration below the one
// deployment A approved, so A still selects exactly [5]. The comparison with the
// approved sequence passes, and the run is refused anyway, because A's
// directory has no file for what B recorded. That refusal is why the approval
// file carries no digest of the whole history: every move that leaves the
// selection unchanged is one this check or the modified and dirty checks
// already refuse.
func TestMigrateUpExpectSequenceHistoryMovedBelowTheApproval(t *testing.T) {
	c := qt.New(t)
	dirA, dirB := c.TempDir(), c.TempDir()
	dbPath := filepath.Join(c.TempDir(), "shared.db")
	seqWriteMigrations(c, dirA, 1, 2)
	_, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dirA)
	c.Assert(err, qt.IsNil)
	seqWriteMigrations(c, dirA, 5)
	seqWriteMigrations(c, dirB, 1, 2, 3)
	_, err = runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dirB)
	c.Assert(err, qt.IsNil)

	_, err = runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dirA,
		"--expect-sequence", seqWriteExpected(c, 5))
	c.Assert(err, qt.ErrorMatches, `.*migration 3 is recorded as applied and this directory has no file for it.*`)
	c.Assert(seqRevisions(c, dbPath), qt.DeepEquals, []int64{1, 2, 3})
	c.Assert(seqTableExists(c, dbPath, seqTable(5)), qt.IsFalse)
}

func TestMigrateUpExpectSequenceRefusesAFileItCannotRead(t *testing.T) {
	for _, row := range []struct {
		name    string
		content string
		want    string
	}{
		{"an unknown field", `{"migrations":[],"sequence":[4]}`, `.*unknown field.*`},
		{"no migrations list", `{}`, `.*names no migrations list.*`},
		{"a version that is not positive", `{"migrations":[{"version":0}]}`, `.*not positive.*`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "expected.json")
			c.Assert(os.WriteFile(path, []byte(row.content), 0o600), qt.IsNil)
			_, err := runUpThroughRoot("--db-url", "sqlite://"+filepath.Join(c.TempDir(), "x.db"),
				"--migrations-dir", c.TempDir(), "--expect-sequence", path)
			c.Assert(err, qt.ErrorMatches, row.want)
		})
	}
}

func seqCopyFile(c *qt.C, from, to string) {
	c.Helper()
	content, err := os.ReadFile(from)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(to, content, 0o600), qt.IsNil) // #nosec G703 -- both paths are the test's own temporary files.
}
