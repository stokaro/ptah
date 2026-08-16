package atlas_test

// Shared helpers for the tests that assert what a migration did and what the
// runner recorded about it: the user tables it created, and the revision rows
// it wrote.

import (
	"database/sql"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"
)

func countRows(c *qt.C, dbPath, table string) int {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	var n int
	c.Assert(db.QueryRow(`SELECT count(*) FROM `+table).Scan(&n), qt.IsNil)
	return n
}

func revisionType(c *qt.C, dbPath, version string) int {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	var revisionType int
	c.Assert(db.QueryRow(
		"SELECT type FROM atlas_schema_revisions WHERE version = ?",
		version,
	).Scan(&revisionType), qt.IsNil)
	return revisionType
}

func revisionVersions(c *qt.C, dbPath string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	rows, err := db.Query(`SELECT version FROM atlas_schema_revisions ORDER BY version`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var out []string
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		out = append(out, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return out
}

// rewriteRevisionVersion moves one recorded revision to another version and
// clears the source-identity marker, leaving the generic one. A caller uses it
// to put a database into a state the runner did not write itself.
func rewriteRevisionVersion(c *qt.C, dbPath, from, to string) {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	result, err := db.Exec(`UPDATE atlas_schema_revisions SET version = ?, operator_version = 'Ptah' WHERE version = ?`, to, from)
	c.Assert(err, qt.IsNil)
	affected, err := result.RowsAffected()
	c.Assert(err, qt.IsNil)
	// A fixture that rewrote nothing would leave the database on the new
	// encoding and make every assertion below vacuous.
	c.Assert(affected, qt.Equals, int64(1))
}

// userTables lists the non-Atlas tables in a sqlite database, sorted.
func userTables(c *qt.C, dbPath string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table'
		AND name NOT LIKE 'atlas%' AND name NOT LIKE 'sqlite%' ORDER BY name`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var out []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		out = append(out, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return out
}
