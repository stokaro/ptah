package migratetests_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// TestCompatMigrateApply_OneVersionSpelledTwoWaysRefuses pins a retained
// divergence. The pinned community binary applies 1_a.sql beside
// 001_b.sql as two revisions, 001 and then 1, because it keys revisions on the
// spelling and orders files by name. Ptah runs migrations in numeric order, so
// the pair ties, and it refuses the directory before any migration runs rather
// than choose an order the directory does not state.
func TestCompatMigrateApply_OneVersionSpelledTwoWaysRefuses(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := writeConvertedApplyDir(c, filepath.Join(root, "migrations"), map[string]string{
		"1_a.sql":   "CREATE TABLE spelled_a (id INTEGER PRIMARY KEY);\n",
		"001_b.sql": "CREATE TABLE spelled_b (id INTEGER PRIMARY KEY);\n",
	})
	hashOut, hashErrOut, hashErr := atlastest.RunCompat("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(hashErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashOut, hashErrOut))
	dbPath := filepath.Join(root, "apply.db")

	stdout, stderr, err := compatApply(dir, dbPath)

	c.Assert(err, qt.ErrorMatches, `.*Atlas migration files 001_b\.sql and 1_a\.sql spell version 1 two ways, "001" and "1": `+
		`the revision table records the spelling and migrations run in numeric order, so one version needs one spelling`)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: "+err.Error()+"\n")
	c.Assert(atlastest.CompatTableNames(c, dbPath), qt.HasLen, 0)
}

// TestCompatMigrateApply_DuplicateVersionRefuses pins a retained divergence.
// Measured on the community binary v1.3.0 and v1.4.0, two files spelling the
// same version, 20260101000000_a.sql beside 20260101000000_b.sql, make
// `migrate apply` run the first file, record its revision and panic with
// exit 2; the next apply exits 0 with nothing to execute, so the second file
// never runs. ptah-compat refuses the directory before any migration runs and
// names both files.
func TestCompatMigrateApply_DuplicateVersionRefuses(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := writeConvertedApplyDir(c, filepath.Join(root, "migrations"), map[string]string{
		"20260101000000_a.sql": "CREATE TABLE duplicate_a (id INTEGER PRIMARY KEY);\n",
		"20260101000000_b.sql": "CREATE TABLE duplicate_b (id INTEGER PRIMARY KEY);\n",
	})
	hashOut, hashErrOut, hashErr := atlastest.RunCompat("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(hashErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashOut, hashErrOut))
	dbPath := filepath.Join(root, "apply.db")

	stdout, stderr, err := compatApply(dir, dbPath)

	c.Assert(err, qt.ErrorMatches, `error registering migrations: duplicate Atlas up migration for version 20260101000000: `+
		`20260101000000_a\.sql and 20260101000000_b\.sql`)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: "+err.Error()+"\n")
	c.Assert(atlastest.CompatTableNames(c, dbPath), qt.HasLen, 0)
}

// TestCompatMigrateApply_RevisionSpelledApartFromItsFileRefuses pins the other
// retained divergence on spelling. A history recorded 1 and 2 meets a
// directory that spells 001, 002 and a new 003. The pinned community binary
// compares versions as text, reads 003 as below the current version 2,
// and exits 0 with nothing executed, so 003 never runs. ptah-compat refuses
// before any migration runs and prints the statements that respell the rows.
func TestCompatMigrateApply_RevisionSpelledApartFromItsFileRefuses(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dbPath := filepath.Join(root, "apply.db")
	recorded := writeConvertedApplyDir(c, filepath.Join(root, "recorded"), map[string]string{
		"1_a.sql": "CREATE TABLE spelled_a (id INTEGER PRIMARY KEY);\n",
		"2_b.sql": "CREATE TABLE spelled_b (id INTEGER PRIMARY KEY);\n",
	})
	spelled := writeConvertedApplyDir(c, filepath.Join(root, "spelled"), map[string]string{
		"001_a.sql": "CREATE TABLE spelled_a (id INTEGER PRIMARY KEY);\n",
		"002_b.sql": "CREATE TABLE spelled_b (id INTEGER PRIMARY KEY);\n",
		"003_c.sql": "CREATE TABLE spelled_c (id INTEGER PRIMARY KEY);\n",
	})
	for _, dir := range []string{recorded, spelled} {
		hashOut, hashErrOut, hashErr := atlastest.RunCompat("migrate", "hash", "--dir", "file://"+dir)
		c.Assert(hashErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashOut, hashErrOut))
	}
	applyOut, applyErrOut, applyErr := compatApply(recorded, dbPath)
	c.Assert(applyErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applyOut, applyErrOut))

	stdout, stderr, err := compatApply(spelled, dbPath)

	c.Assert(err, qt.ErrorMatches, `(?s).*revision table "atlas_schema_revisions" records 2 versions under another `+
		`spelling than their migration files: 1 for 001 and 1 more; .*`+
		`UPDATE "atlas_schema_revisions" SET version = '001' WHERE version = '1';\n`+
		`UPDATE "atlas_schema_revisions" SET version = '002' WHERE version = '2';`)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: "+err.Error()+"\n")
	c.Assert(atlastest.CompatTableNames(c, dbPath), qt.Not(qt.Contains), "spelled_c")
	rows := compatRevisionRows(c, dbPath)
	versions := make([]string, 0, len(rows))
	for _, row := range rows {
		versions = append(versions, row.Version)
	}
	c.Assert(versions, qt.DeepEquals, []string{"1", "2"})
}
