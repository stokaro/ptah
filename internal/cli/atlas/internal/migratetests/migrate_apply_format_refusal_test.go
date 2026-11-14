package migratetests_test

import (
	"encoding/json"
	"maps"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// These tests pin what `migrate apply --format` writes for a run refused before
// its first statement. Measured on the pinned community binary v1.3.0 over
// SQLite, the out-of-order refusal writes no report: stdout stays empty and the
// error goes to stderr exactly as it does without --format, at exit 1. A file
// the binary sends and the database refuses is reported inside the document,
// which TestCompatCommand_MigrateApplyWritesFormatOnApplyError pins, and a file
// Ptah refuses before sending it takes the same shape.

func TestCompatMigrateApplyFormat_OutOfOrderRefusalWritesNoReport(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "order.db")
	atlastest.WriteAtlasApplyProjectMigration(c, dir, "1_a.sql", "CREATE TABLE a (id int);\n")
	atlastest.WriteHashedAtlasDir(c, dir, "3_c.sql", "CREATE TABLE c (id int);\n")
	_, _, err := atlastest.RunCompatStreams(c, "migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil)
	atlastest.WriteHashedAtlasDir(c, dir, "2_b.sql", "CREATE TABLE b (id int);\n")

	stdout, stderr, err := atlastest.RunCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir,
		"--exec-order", "linear",
		"--format", "{{ json . }}",
	)

	const refusal = "migration file 2_b.sql was added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error"
	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(refusal))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: "+refusal+"\n")
	c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"a", "c"})
}

func TestCompatMigrateApplyFormat_UpFrontStatementRefusalIsReported(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "vacuum.db")
	atlastest.WriteAtlasApplyProjectMigration(c, dir, "1_a.sql", "CREATE TABLE t1 (id int);\n")
	atlastest.WriteHashedAtlasDir(c, dir, "2_b.sql", "CREATE TABLE t2 (id int);\nVACUUM;\n")

	stdout, stderr, err := atlastest.RunCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir,
		"--format", "{{ json . }}",
	)

	// Ptah refuses the file before any statement is sent, where the community
	// binary sends it and reports the database's answer in the document. The
	// document still arrives: it carries Ptah's refusal and lists no
	// statement, because none was sent.
	const refusal = `2_b\.sql cannot run inside a transaction: line 2 VACUUM is refused inside a transaction block.*\nSQL: VACUUM`
	c.Assert(err, qt.ErrorMatches, `(?s)error applying migrations: `+refusal)
	c.Assert(stderr, qt.Equals, "")
	var report map[string]json.RawMessage
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("stdout=%s", stdout))
	c.Assert(slices.Sorted(maps.Keys(report)), qt.DeepEquals, []string{
		"Dir", "Driver", "End", "Error", "Pending", "Start", "Target", "URL",
	})
	var reported string
	c.Assert(json.Unmarshal(report["Error"], &reported), qt.IsNil)
	c.Assert(reported, qt.Matches, `(?s)`+refusal)
	c.Assert(userTables(c, dbPath), qt.HasLen, 0)
}
