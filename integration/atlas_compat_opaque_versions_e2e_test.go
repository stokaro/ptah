//go:build integration

package integration_test

import (
	"database/sql"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// revisionIdentityTemplate prints the parts of the status report that say
// whether 1_a.sql is pending.
const revisionIdentityTemplate = `{{ json .OutOfOrder }}|{{ json .Pending }}|{{ .Error }}`

// TestCompatAtlasRevisionIdentityE2E adds 1_a.sql beside an applied
// 1R__rep.sql. Both run under the number 1, and the applied row records the
// string 1R, so 1_a.sql is pending: status reports it out of order and apply
// refuses it, where reading the number alone reports the directory applied
// and never runs the file.
func TestCompatAtlasRevisionIdentityE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	run := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
	prepareVersionOrder(c, dir, run, []string{"10_b.sql", "1R__rep.sql"}, []string{"1_a.sql"})

	status := run(append(append([]string{"migrate", "status"}, versionOrderTarget...), "--format", revisionIdentityTemplate)...)
	apply := run(append([]string{"migrate", "apply"}, versionOrderTarget...)...)

	c.Assert(status.Stdout, qt.Equals, `[{"Name":"1_a.sql","Version":"1","Description":"a"}]|[]|`+
		"migration file 1_a.sql was added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error")
	c.Assert(apply.ExitCode, qt.Equals, 1)
	c.Assert(apply.Stderr, qt.Contains, "migration file 1_a.sql was added out of order.")
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"10", "1R"})
}

// TestCompatAtlasRevisionIdentityMatchesTheCommunityBinaryE2E holds the status
// report, the refusal, and the run under --exec-order non-linear to the pinned
// community binary v1.3.0 on the same history.
func TestCompatAtlasRevisionIdentityMatchesTheCommunityBinaryE2E(t *testing.T) {
	c := qt.New(t)
	reference := atlasReferenceBinary(t)
	communityDir, compatDir := c.TempDir(), c.TempDir()
	community := referenceVersionOrderRunner(c, communityDir, reference, environmentWithoutPtahVariables())
	compat := compatVersionOrderRunner(c, compatDir, environmentWithoutPtahVariables())
	prepareVersionOrder(c, communityDir, community, []string{"10_b.sql", "1R__rep.sql"}, []string{"1_a.sql"})
	prepareVersionOrder(c, compatDir, compat, []string{"10_b.sql", "1R__rep.sql"}, []string{"1_a.sql"})

	status := append(append([]string{"migrate", "status"}, versionOrderTarget...), "--format", revisionIdentityTemplate)
	c.Assert(compat(status...).Stdout, qt.Equals, community(status...).Stdout)
	apply := append([]string{"migrate", "apply"}, versionOrderTarget...)
	communityApply, compatApply := community(apply...), compat(apply...)
	c.Assert(compatApply.ExitCode, qt.Equals, communityApply.ExitCode)
	c.Assert(firstLine(compatApply.Stderr), qt.Equals, firstLine(communityApply.Stderr))
	nonLinear := append(slices.Clone(apply), "--exec-order", "non-linear")
	c.Assert(compat(nonLinear...).ExitCode, qt.Equals, community(nonLinear...).ExitCode)
	c.Assert(revisionTableVersions(c, filepath.Join(compatDir, "app.db")), qt.DeepEquals,
		revisionTableVersions(c, filepath.Join(communityDir, "app.db")))
}

// TestNativeAtlasRevisionIdentityE2E runs the same history through the native
// binary over both Atlas formats. 1R__rep.sql spells the number 1_a.sql spells,
// so the three files run under their positions in file-name order: 10_b.sql,
// 1R__rep.sql, 1_a.sql. The pending 1_a.sql comes after both applied files in
// that order, so the native linear order applies it, under a row of its own
// rather than read as applied through the number 1.
func TestNativeAtlasRevisionIdentityE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	hash := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
	up := []string{
		"migrations", "up", "--db-url", "sqlite://app.db", "--migrations-dir", "migrations",
		"--dir-format", "atlas", "--revision-format", "atlas",
	}
	native := func() clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, up...)
	}
	writeVersionOrderFiles(c, dir, hash, []string{"10_b.sql", "1R__rep.sql"})
	applied := native()
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
	writeVersionOrderFiles(c, dir, hash, []string{"1_a.sql"})

	got := native()

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "10", "1R"})
}

// opaqueVersionCases are files whose Atlas version is not a decimal number
// above zero, each beside 1_a.sql, with the versions atlas_schema_revisions
// records once both ran, measured on the pinned community binary v1.3.0.
// Reading the leading number records 1.x.sql as 1, and leaving the others out
// applies 1_a.sql alone and reports the directory applied.
var opaqueVersionCases = []struct {
	name     string
	versions []string
}{
	{name: "1.x.sql", versions: []string{"1", "1.x"}},
	{name: "1-x_y.sql", versions: []string{"1", "1-x"}},
	{name: "v1_x.sql", versions: []string{"1", "v1"}},
	{name: "x_y.sql", versions: []string{"1", "x"}},
	{name: "1a_x.sql", versions: []string{"1", "1a"}},
	{name: "00_x.sql", versions: []string{"00", "1"}},
}

// TestCompatAtlasOpaqueVersionsE2E applies each file under the version Atlas
// reads from its name.
func TestCompatAtlasOpaqueVersionsE2E(t *testing.T) {
	for _, tt := range opaqueVersionCases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			run := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
			writeVersionOrderFiles(c, dir, run, []string{"1_a.sql", tt.name})

			got := run(append([]string{"migrate", "apply"}, versionOrderTarget...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
			c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, tt.versions)
		})
	}
}

// TestCompatAtlasOpaqueVersionsMatchTheCommunityBinaryE2E holds each run, and
// the status report after a 2_b.sql is added, to the pinned community binary
// v1.3.0. 2_b.sql sorts above 1.x.sql, 1-x_y.sql, 1a_x.sql and 00_x.sql, so it
// is pending, and below v1_x.sql and x_y.sql, so it is out of order.
func TestCompatAtlasOpaqueVersionsMatchTheCommunityBinaryE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, tt := range opaqueVersionCases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			communityDir, compatDir := c.TempDir(), c.TempDir()
			community := referenceVersionOrderRunner(c, communityDir, reference, environmentWithoutPtahVariables())
			compat := compatVersionOrderRunner(c, compatDir, environmentWithoutPtahVariables())
			prepareVersionOrder(c, communityDir, community, []string{"1_a.sql", tt.name}, []string{"2_b.sql"})
			prepareVersionOrder(c, compatDir, compat, []string{"1_a.sql", tt.name}, []string{"2_b.sql"})

			status := append(append([]string{"migrate", "status"}, versionOrderTarget...),
				"--format", `{{ .Current }}|{{ json .Pending }}|{{ json .OutOfOrder }}|{{ .Error }}`)

			c.Assert(compat(status...).Stdout, qt.Equals, community(status...).Stdout)
			c.Assert(revisionTableVersions(c, filepath.Join(compatDir, "app.db")), qt.DeepEquals,
				revisionTableVersions(c, filepath.Join(communityDir, "app.db")))
		})
	}
}

// TestCompatAtlasEmptyVersion_FailurePath refuses a directory holding _x.sql
// before anything runs. Atlas reads its version as "", which no --to-version,
// --baseline or migrate set argument can name; leaving the file out would
// apply 1_a.sql alone and report the directory applied.
func TestCompatAtlasEmptyVersion_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	run := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
	writeVersionOrderFiles(c, dir, run, []string{"1_a.sql", "_x.sql"})

	got := run(append([]string{"migrate", "apply"}, versionOrderTarget...)...)

	c.Assert(got.ExitCode, qt.Equals, 1)
	c.Assert(got.Stderr, qt.Contains, `Atlas migration files Ptah cannot run: _x.sql (version "").`)
	c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.HasLen, 0)
}

// nativeAtlasUp runs the native binary over the Atlas directory in dir under
// the revision table format given.
func nativeAtlasUp(c *qt.C, dir, revisionFormat string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"migrations", "up", "--db-url", "sqlite://app.db", "--migrations-dir", "migrations",
		"--dir-format", "atlas", "--revision-format", revisionFormat,
	)
}

// TestNativeAtlasOpaqueVersionE2E applies v1_x.sql through the native binary
// over the Atlas revision table, which records the version string.
func TestNativeAtlasOpaqueVersionE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeVersionOrderFiles(c, dir, compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables()), []string{"1_a.sql", "v1_x.sql"})

	got := nativeAtlasUp(c, dir, "atlas")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "v1"})
}

// TestNativeAtlasOpaqueVersion_FailurePath refuses v1_x.sql over the Ptah
// revision table before anything runs. That table records a number, and the
// number v1_x.sql runs under moves when a numbered file is added, so the row
// would name another file the next run.
func TestNativeAtlasOpaqueVersion_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeVersionOrderFiles(c, dir, compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables()), []string{"1_a.sql", "v1_x.sql"})

	got := nativeAtlasUp(c, dir, "ptah")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(got.Stderr, qt.Contains, `Atlas migration v1_x.sql has version "v1", which a revision table that records numbers cannot keep`)
	c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.Not(qt.Contains), "t_1_a")
}

// sqliteTables lists the tables of the SQLite database at path, so a refusal
// that ran nothing reads as an empty list.
func sqliteTables(c *qt.C, path string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	tables := make([]string, 0)
	for rows.Next() {
		var table string
		c.Assert(rows.Scan(&table), qt.IsNil)
		tables = append(tables, table)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

// TestCompatAtlasOpaqueVersionOperandsE2E addresses files by versions that are
// not numbers, over 00_z.sql, 1_a.sql, 2_b.sql and v1_x.sql, which run under
// their positions in file-name order. migrate set 00 records 00_z.sql alone;
// read as the number 0 it removes every row. --baseline v1 and --to-version v1
// name v1_x.sql, which a number cannot name.
func TestCompatAtlasOpaqueVersionOperandsE2E(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{name: "set 00", args: []string{"migrate", "set", "00"}, want: []string{"00"}},
		{name: "baseline v1", args: []string{"migrate", "apply", "--baseline", "v1"}, want: []string{"v1"}},
		{name: "baseline 1", args: []string{"migrate", "apply", "--baseline", "1"}, want: []string{"1", "2", "v1"}},
		{name: "to-version v1", args: []string{"migrate", "apply", "--to-version", "v1"}, want: []string{"00", "1", "2", "v1"}},
		{name: "to-version 1", args: []string{"migrate", "apply", "--to-version", "1"}, want: []string{"00", "1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			run := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
			writeVersionOrderFiles(c, dir, run, []string{"00_z.sql", "1_a.sql", "2_b.sql", "v1_x.sql"})

			got := run(append(slices.Clone(tt.args), versionOrderTarget...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
			c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, tt.want)
		})
	}
}

// TestCompatAtlasOpaqueVersionOperandsMatchTheCommunityBinaryE2E holds migrate
// set and --baseline, which the pinned community binary v1.3.0 has, to it on
// the same directory. It has no --to-version.
func TestCompatAtlasOpaqueVersionOperandsMatchTheCommunityBinaryE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	tests := []struct {
		name string
		args []string
	}{
		{name: "set 00", args: []string{"migrate", "set", "00"}},
		{name: "set v1", args: []string{"migrate", "set", "v1"}},
		{name: "baseline v1", args: []string{"migrate", "apply", "--baseline", "v1"}},
		{name: "baseline 00", args: []string{"migrate", "apply", "--baseline", "00"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			communityDir, compatDir := c.TempDir(), c.TempDir()
			community := referenceVersionOrderRunner(c, communityDir, reference, environmentWithoutPtahVariables())
			compat := compatVersionOrderRunner(c, compatDir, environmentWithoutPtahVariables())
			files := []string{"00_z.sql", "1_a.sql", "2_b.sql", "v1_x.sql"}
			writeVersionOrderFiles(c, communityDir, community, files)
			writeVersionOrderFiles(c, compatDir, compat, files)
			args := append(slices.Clone(tt.args), versionOrderTarget...)

			communityRun, compatRun := community(args...), compat(args...)

			c.Assert(compatRun.ExitCode, qt.Equals, communityRun.ExitCode,
				qt.Commentf("compat stderr:\n%s\ncommunity stderr:\n%s", compatRun.Stderr, communityRun.Stderr))
			c.Assert(firstLine(compatRun.Stderr), qt.Equals, firstLine(communityRun.Stderr))
			c.Assert(revisionTableVersions(c, filepath.Join(compatDir, "app.db")), qt.DeepEquals,
				revisionTableVersions(c, filepath.Join(communityDir, "app.db")))
		})
	}
}

// TestCompatAtlasOpaqueVersionOperandNamingNoFile_FailurePath refuses migrate
// set 3 over the same directory, as the pinned community binary v1.3.0 does:
// the four files run under the positions 1 to 4, and no file carries the
// version 3.
func TestCompatAtlasOpaqueVersionOperandNamingNoFile_FailurePath(t *testing.T) {
	c := qt.New(t)
	reference := atlasReferenceBinary(t)
	communityDir, compatDir := c.TempDir(), c.TempDir()
	community := referenceVersionOrderRunner(c, communityDir, reference, environmentWithoutPtahVariables())
	compat := compatVersionOrderRunner(c, compatDir, environmentWithoutPtahVariables())
	files := []string{"00_z.sql", "1_a.sql", "2_b.sql", "v1_x.sql"}
	writeVersionOrderFiles(c, communityDir, community, files)
	writeVersionOrderFiles(c, compatDir, compat, files)
	args := append([]string{"migrate", "set", "3"}, versionOrderTarget...)

	communityRun, compatRun := community(args...), compat(args...)

	c.Assert(compatRun.ExitCode, qt.Equals, 1)
	c.Assert(communityRun.ExitCode, qt.Equals, 1)
	c.Assert(firstLine(compatRun.Stderr), qt.Equals, `Error: migration with version "3" not found`)
	c.Assert(firstLine(communityRun.Stderr), qt.Equals, firstLine(compatRun.Stderr))
}
