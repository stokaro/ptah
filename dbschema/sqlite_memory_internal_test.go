package dbschema

// White-box testing required: the DSN the driver receives is built by an
// unexported converter, and the property under test — that no file appears —
// is about the filesystem rather than about any value the package returns.

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// memoryDSNRow is one authored URL and whether opening it may touch the disk.
type memoryDSNRow struct {
	name       string
	url        string
	file       string
	wantOnDisk bool
}

func TestSQLiteMemoryURLOpensNoFile(t *testing.T) {
	rows := []memoryDSNRow{{
		// The form the repository's own tests, help text and docs use. It
		// created an 8 KB database named `dev` in the working directory, and
		// carried its contents into the next run (stokaro/ptah#1819).
		name:       "a named memory database touches nothing",
		url:        "sqlite://dev?mode=memory",
		file:       "dev",
		wantOnDisk: false,
	}, {
		name:       "the anonymous form still touches nothing",
		url:        "sqlite:///:memory:",
		file:       ":memory:",
		wantOnDisk: false,
	}, {
		name:       "a file database is still a file",
		url:        "sqlite://real.db",
		file:       "real.db",
		wantOnDisk: true,
	}, {
		// The control that actually discriminates. Adding file: to every path
		// would leave both rows above passing, because file:real.db still
		// writes real.db -- but it puts SQLite in URI mode, where the name is
		// percent-DECODED. Measured: `pct%41.db` opens that name literally,
		// while `file:pct%41.db` opens `pctA.db`. So this row fails the moment
		// the prefix is applied where it should not be.
		name:       "a percent in a file name stays literal",
		url:        "sqlite://pct%2541.db",
		file:       "pct%41.db",
		wantOnDisk: true,
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			t.Chdir(dir)

			db, err := sql.Open("sqlite", convertSQLiteURL(row.url))
			c.Assert(err, qt.IsNil)
			t.Cleanup(func() { _ = db.Close() })
			_, err = db.Exec("CREATE TABLE probe (id INTEGER)")
			c.Assert(err, qt.IsNil)

			_, statErr := os.Stat(filepath.Join(dir, row.file))
			c.Assert(statErr == nil, qt.Equals, row.wantOnDisk)
		})
	}
}

// TestSQLiteMemoryDatabaseStartsEmpty pins the half that matters more than the
// stray file.
//
// A dev database exists to be empty. One that persists carries the previous
// run's tables into the next plan, which produces a wrong answer that looks
// like a right one.
func TestSQLiteMemoryDatabaseStartsEmpty(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dsn := convertSQLiteURL("sqlite://dev?mode=memory")

	first, err := sql.Open("sqlite", dsn)
	c.Assert(err, qt.IsNil)
	_, err = first.Exec("CREATE TABLE leftover (id INTEGER)")
	c.Assert(err, qt.IsNil)
	c.Assert(first.Close(), qt.IsNil)

	second, err := sql.Open("sqlite", dsn)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = second.Close() })
	var count int
	err = second.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE name = 'leftover'").Scan(&count)

	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}
