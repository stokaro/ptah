//go:build integration

package integration_test

import (
	"encoding/json"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

// positionFiles run under their positions in file-name order, because 00 is
// not a decimal above zero: 00_x.sql runs under 1, 1_x.sql under 2, 2_x.sql
// under 3 and 3_x.sql under 4.
var positionFiles = []string{"00_x.sql", "1_x.sql", "2_x.sql", "3_x.sql"}

// mixedPositionFiles run in file-name order, 1_x.sql, 10_x.sql, 2_x.sql and
// v1_x.sql, under the positions 1 to 4, because v1 is not a number.
var mixedPositionFiles = []string{"1_x.sql", "2_x.sql", "10_x.sql", "v1_x.sql"}

// writePositionFiles writes files with their atlas.sum into dir.
func writePositionFiles(c *qt.C, dir string, files []string) {
	c.Helper()
	writeVersionOrderFiles(c, dir, compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables()), files)
}

// documentMembers decodes one JSON object and returns its members.
func documentMembers(c *qt.C, document string) map[string]json.RawMessage {
	c.Helper()
	var members map[string]json.RawMessage
	c.Assert(json.Unmarshal([]byte(document), &members), qt.IsNil, qt.Commentf("%s", document))
	return members
}

// memberNames returns the names of members, sorted.
func memberNames(members map[string]json.RawMessage) []string {
	return slices.Sorted(maps.Keys(members))
}

// TestNativeAtlasPositionsUpToVersionE2E bounds `migrations up` by the version
// a file spells. In a directory keyed by position, 2 names 2_x.sql and stops
// after it in file-name order, never at the second file.
func TestNativeAtlasPositionsUpToVersionE2E(t *testing.T) {
	tests := []struct {
		name      string
		files     []string
		toVersion string
		wantRows  []string
	}{
		{name: "a number after a wider one", files: mixedPositionFiles, toVersion: "2", wantRows: []string{"1", "10", "2"}},
		{name: "a version string", files: mixedPositionFiles, toVersion: "v1", wantRows: []string{"1", "10", "2", "v1"}},
		{name: "a number after 00", files: positionFiles, toVersion: "2", wantRows: []string{"00", "1", "2"}},
		{name: "00", files: positionFiles, toVersion: "00", wantRows: []string{"00"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writePositionFiles(c, dir, tt.files)

			got := nativePtah(c, dir, "migrations", "up", "--to-version", tt.toVersion)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
			c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, tt.wantRows)
		})
	}
}

// TestNativeAtlasPositionsUpToVersion_FailurePath refuses a bound that names
// no file: 0, the empty history, before the command connects, and 4, the
// position 3_x.sql runs under, before any row is written.
func TestNativeAtlasPositionsUpToVersion_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		toVersion string
		wantErr   string
	}{
		{name: "zero", toVersion: "0", wantErr: `invalid --to-version "0": a migration version is greater than zero`},
		{name: "a position", toVersion: "4", wantErr: `invalid --to-version "4": no migration file carries version "4"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writePositionFiles(c, dir, positionFiles)

			got := nativePtah(c, dir, "migrations", "up", "--to-version", tt.toVersion)

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(got.Stderr, qt.Contains, tt.wantErr)
			c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.Not(qt.Contains), "atlas_schema_revisions")
		})
	}
}

// TestNativeAtlasPositionsBaselineDryRunE2E prints the versions the files
// spell in a baseline dry run, as the run that writes the rows names them:
// through 2 the rows are 00, 1 and 2, which run under the positions 1 to 3.
func TestNativeAtlasPositionsBaselineDryRunE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writePositionFiles(c, dir, positionFiles)

	got := nativePtah(c, dir, "migrations", "baseline", "--version", "2", "--dry-run")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "Baseline version: 2\nRows:\n- version=00 description=")
	c.Assert(got.Stdout, qt.Contains, "\n- version=1 description=")
	c.Assert(got.Stdout, qt.Contains, "\n- version=2 description=")
	c.Assert(got.Stdout, qt.Not(qt.Contains), "version=3")
	c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.Not(qt.Contains), "atlas_schema_revisions")
}

// TestNativeAtlasPositionsRunDocumentE2E names every migration by its version
// key in the run document and the status document of a directory keyed by
// position. The numeric members are absent: 00_x.sql runs under 1, and a
// document that said 1 would name 1_x.sql to the next reader.
func TestNativeAtlasPositionsRunDocumentE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writePositionFiles(c, dir, positionFiles[:3])

	up := nativePtah(c, dir, "migrations", "up", "--json")
	status := nativePtah(c, dir, "migrations", "status", "--json")

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))
	run := documentMembers(c, up.Stdout)
	c.Assert(memberNames(run), qt.Not(qt.Contains), "planned")
	c.Assert(memberNames(run), qt.Not(qt.Contains), "applied")
	c.Assert(string(run["planned_keys"]), qt.JSONEquals, []string{"00", "1", "2"})
	c.Assert(string(run["applied_keys"]), qt.JSONEquals, []string{"00", "1", "2"})
	after := documentMembers(c, string(run["status"]))
	c.Assert(memberNames(after), qt.Not(qt.Contains), "current_version")
	c.Assert(memberNames(after), qt.Not(qt.Contains), "applied_migrations")
	c.Assert(memberNames(after), qt.Not(qt.Contains), "pending_migrations")
	c.Assert(string(after["current_version_key"]), qt.JSONEquals, "2")
	c.Assert(string(after["applied_migration_keys"]), qt.JSONEquals, []string{"00", "1", "2"})

	c.Assert(status.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", status.Stderr))
	read := documentMembers(c, status.Stdout)
	c.Assert(memberNames(read), qt.Not(qt.Contains), "current_version")
	c.Assert(string(read["current_version_key"]), qt.JSONEquals, "2")
	var records []map[string]json.RawMessage
	c.Assert(json.Unmarshal(read["migrations"], &records), qt.IsNil)
	c.Assert(records, qt.HasLen, 3)
	c.Assert(memberNames(records[0]), qt.Not(qt.Contains), "version")
	c.Assert(string(records[0]["version_key"]), qt.JSONEquals, "00")
}

// TestNumberedRunDocumentKeepsItsVersionsE2E is the control: a numbered
// directory names its migrations by number as well as by key.
func TestNumberedRunDocumentKeepsItsVersionsE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writePositionFiles(c, dir, []string{"1_x.sql", "2_x.sql"})

	up := nativePtah(c, dir, "migrations", "up", "--json")

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))
	run := documentMembers(c, up.Stdout)
	c.Assert(string(run["planned"]), qt.JSONEquals, []int64{1, 2})
	c.Assert(string(run["planned_keys"]), qt.JSONEquals, []string{"1", "2"})
	after := documentMembers(c, string(run["status"]))
	c.Assert(string(after["current_version"]), qt.JSONEquals, 2)
}

// TestNativeAtlasPositionsExpectSequenceE2E approves a run by the version keys
// the run document prints. The number beside a key takes no part, since it is
// a position that moves when a file is added before it: 9 is not the position
// v1_x.sql runs under, and the approval still names it.
func TestNativeAtlasPositionsExpectSequenceE2E(t *testing.T) {
	tests := []struct {
		name     string
		approval string
	}{
		{name: "keys alone", approval: `{"migrations":[{"version_key":"2"},{"version_key":"v1"}]}`},
		{name: "a key beside another number", approval: `{"migrations":[{"version_key":"2"},{"version":9,"version_key":"v1"}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writePositionFiles(c, dir, []string{"1_x.sql", "2_x.sql", "v1_x.sql"})
			first := nativePtah(c, dir, "migrations", "up", "--to-version", "1")
			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", first.Stderr))
			c.Assert(os.WriteFile(filepath.Join(dir, "expected.json"), []byte(tt.approval), 0o600), qt.IsNil)

			got := nativePtah(c, dir, "migrations", "up", "--expect-sequence", "expected.json")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
			c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "2", "v1"})
		})
	}
}

// TestNativeAtlasPositionsExpectSequence_FailurePath refuses an approval whose
// keys are not the selection, and names both lists by key.
func TestNativeAtlasPositionsExpectSequence_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writePositionFiles(c, dir, []string{"1_x.sql", "2_x.sql", "v1_x.sql"})
	first := nativePtah(c, dir, "migrations", "up", "--to-version", "1")
	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", first.Stderr))
	approval := `{"migrations":[{"version":3,"version_key":"v1"}]}`
	c.Assert(os.WriteFile(filepath.Join(dir, "expected.json"), []byte(approval), 0o600), qt.IsNil)

	got := nativePtah(c, dir, "migrations", "up", "--expect-sequence", "expected.json")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(got.Stderr, qt.Contains, "the migrations selected under the migration lock are [2 v1], and the approved sequence is [v1]")
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1"})
}

// TestNativeAtlasPositionsRemoveE2E removes 2_x.sql named by the version it
// spells while 00_x.sql and 1_x.sql are recorded. 2_x.sql runs under 3, and the
// guard compares the applied files' versions, so the applied 1_x.sql, which
// runs under 2, does not make 2_x.sql read as applied.
func TestNativeAtlasPositionsRemoveE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writePositionFiles(c, dir, positionFiles)
	set := nativePtah(c, dir, "migrations", "set", "--version", "1")
	c.Assert(set.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", set.Stderr))

	got := nativePtah(c, dir, "migrations", "rm", "--version", "2")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "Removed migrations/2_x.sql\n")
	_, statErr := os.Stat(filepath.Join(dir, "migrations", "2_x.sql"))
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestNativeAtlasPositionsRemove_FailurePath refuses to remove 1_x.sql, which
// is recorded and runs under 2.
func TestNativeAtlasPositionsRemove_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writePositionFiles(c, dir, positionFiles)
	set := nativePtah(c, dir, "migrations", "set", "--version", "1")
	c.Assert(set.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", set.Stderr))

	got := nativePtah(c, dir, "migrations", "rm", "--version", "1")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(got.Stderr, qt.Contains, "migration version 1 is already applied")
	_, statErr := os.Stat(filepath.Join(dir, "migrations", "1_x.sql"))
	c.Assert(statErr, qt.IsNil)
}

// TestNativeAtlasPositionsRollbackToTag_FailurePath refuses a rollback to a
// tag: a tag records a number, and here a number is a position that moves
// when a file is added before it.
func TestNativeAtlasPositionsRollbackToTag_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writePositionFiles(c, dir, positionFiles)
	up := nativePtah(c, dir, "migrations", "up")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))

	got := nativePtah(c, dir, "migrations", "down", "--to-tag", "release", "--dry-run")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(got.Stderr, qt.Contains, "a migration tag records a version number, and this Atlas directory runs its files under their positions")
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"00", "1", "2", "3"})
}
