//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// nativeAtlasDirectory writes 1_a.sql, 2_b.sql and v1_x.sql with their
// atlas.sum into dir, so v1_x.sql is the one file whose version carries no
// number.
func nativeAtlasDirectory(c *qt.C, dir string) {
	c.Helper()
	writeVersionOrderFiles(c, dir, compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables()),
		[]string{"1_a.sql", "2_b.sql", "v1_x.sql"})
}

// nativePtah runs the native binary in dir over the Atlas directory and the
// Atlas revision table, with args after the command path.
func nativePtah(c *qt.C, dir string, args ...string) clirun.Result {
	c.Helper()
	common := []string{
		"--db-url", "sqlite://app.db", "--migrations-dir", "migrations",
		"--dir-format", "atlas", "--revision-format", "atlas",
	}
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		append(append(args[:2:2], common...), args[2:]...)...)
}

// TestNativeAtlasNumberlessVersionLabelsE2E prints v1, the version v1_x.sql
// spells, wherever the native commands name it. The three files run under
// their positions in file-name order, 1, 2 and 3, which name no version.
func TestNativeAtlasNumberlessVersionLabelsE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	nativeAtlasDirectory(c, dir)

	up := nativePtah(c, dir, "migrations", "up")
	status := nativePtah(c, dir, "migrations", "status")
	down := nativePtah(c, dir, "migrations", "down", "--target", "1", "--verbose", "--dry-run")

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))
	c.Assert(up.Stdout, qt.Contains, "Database is now at version: v1\n")
	c.Assert(status.Stdout, qt.Contains, "Current Version: v1\n")
	c.Assert(down.Stdout, qt.Contains, "Current version: v1\n")
	c.Assert(down.Stdout, qt.Contains, "Will roll back from version v1 to 1\n")
	c.Assert(down.Stdout, qt.Contains, "Specific migrations to rollback: [2 v1]\n")
	// The files carry no down section, and the refusal names v1 too.
	c.Assert(down.Stderr, qt.Contains, "migration v1 has no Atlas down migration")
}

// TestNativeAtlasNumberlessVersionLogLinesE2E names v1_x.sql by its version
// in the structured log lines too. The "version" key carries a string for such
// a file, as it carries the source tool's token for a converted directory, and
// a number for a numbered file.
func TestNativeAtlasNumberlessVersionLogLinesE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	nativeAtlasDirectory(c, dir)

	up := nativePtah(c, dir, "migrations", "up")

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))
	c.Assert(up.Stderr, qt.Contains, `msg="Applying migration"`)
	c.Assert(up.Stderr, qt.Contains, " version=v1 description=X\n")
	c.Assert(up.Stderr, qt.Contains, " version=2 description=B\n")
	c.Assert(up.Stderr, qt.Not(qt.Contains), " version=3 ")
}

// TestNativeAtlasNumberlessVersionTargetE2E rolls back to v1 named by its
// version: nothing above it is applied, so nothing is rolled back.
func TestNativeAtlasNumberlessVersionTargetE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	nativeAtlasDirectory(c, dir)
	up := nativePtah(c, dir, "migrations", "up")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))

	got := nativePtah(c, dir, "migrations", "down", "--target", "v1", "--dry-run")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "Database is already at or below target version v1!")
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "2", "v1"})
}

// TestNativeAtlasNumberlessVersionTarget_FailurePath refuses --target 3,
// the position v1_x.sql runs under, since a number names a file here only as
// its version string, and a target the directory does not spell.
func TestNativeAtlasNumberlessVersionTarget_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		wantErr string
	}{
		{
			name:    "the position",
			target:  "3",
			wantErr: `invalid target version "3": no migration file carries version "3"`,
		},
		{
			name:    "a version no file spells",
			target:  "v2",
			wantErr: `invalid target version "v2": no migration file carries version "v2"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			nativeAtlasDirectory(c, dir)
			up := nativePtah(c, dir, "migrations", "up")
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))

			got := nativePtah(c, dir, "migrations", "down", "--target", tt.target, "--dry-run")

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(got.Stderr, qt.Contains, tt.wantErr)
			c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "2", "v1"})
		})
	}
}

// TestNativeAtlasNumberlessVersionPlanWithoutShadow_FailurePath asks for a
// --plan rollback to v1 with no shadow database. The refusal names the target
// v1, where v1_x.sql runs under the position 2 and v2_y.sql under 3.
func TestNativeAtlasNumberlessVersionPlanWithoutShadow_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeVersionOrderFiles(c, dir, compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables()),
		[]string{"1_a.sql", "v1_x.sql", "v2_y.sql"})
	up := nativePtah(c, dir, "migrations", "up")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))

	got := nativePtah(c, dir, "migrations", "down", "--target", "v1", "--plan")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(got.Stderr, qt.Contains, "--plan requires --shadow-db: the schema at version v1 has to be built somewhere")
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "v1", "v2"})
}

// spelledNumberFiles are versions Atlas reads as text although they read as
// numbers: 00 and -5 are not decimals above zero. The five run in file-name
// order: -5_m.sql, 00_z.sql, 1_a.sql, 2_b.sql, v1_v.sql.
var spelledNumberFiles = []string{"1_a.sql", "2_b.sql", "00_z.sql", "-5_m.sql", "v1_v.sql"}

// TestNativeAtlasSpelledNumberVersionsE2E names 00_z.sql by --version=00 and
// -5_m.sql by --version=-5 on every native command that takes a version, and
// each acts on the files after it in execution order. Read as a number first,
// 00 is version 0, which a set reads as removing every revision, and -5 is
// refused as negative before the directory is read. Ordered by the numbers
// the names spell, a set to 00 keeps 1_a.sql and 2_b.sql, which run after it.
func TestNativeAtlasSpelledNumberVersionsE2E(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantExit int
		wantOut  string
		wantRows []string
	}{
		{
			name:     "set to 00 removes what runs after it",
			args:     []string{"migrations", "set", "--version=00"},
			wantRows: []string{"-5", "00"},
		},
		{
			name:     "set to -5 removes what runs after it",
			args:     []string{"migrations", "set", "--version=-5"},
			wantRows: []string{"-5"},
		},
		{
			name: "down to 00 plans the three files after it",
			args: []string{"migrations", "down", "--target=00", "--dry-run"},
			// The files carry no down section, so the plan ends in that refusal.
			wantExit: 2,
			wantOut:  "Migrations to roll back: 3\n",
			wantRows: []string{"-5", "00", "1", "2", "v1"},
		},
		{
			name:     "repair 00",
			args:     []string{"migrations", "repair", "--version=00", "--force"},
			wantOut:  "Updated migration 00 revision metadata; no migration SQL ran\n",
			wantRows: []string{"-5", "00", "1", "2", "v1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writeVersionOrderFiles(c, dir, compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables()),
				spelledNumberFiles)
			up := nativePtah(c, dir, "migrations", "up")
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))

			got := nativePtah(c, dir, tt.args...)

			c.Assert(got.ExitCode, qt.Equals, tt.wantExit, qt.Commentf("stderr:\n%s", got.Stderr))
			c.Assert(got.Stdout, qt.Contains, tt.wantOut)
			c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, tt.wantRows)
		})
	}
}

// TestNativeAtlasSpelledNumberVersionBaselineE2E baselines a fresh database
// through 00 and through -5. The Atlas baseline records only the boundary.
func TestNativeAtlasSpelledNumberVersionBaselineE2E(t *testing.T) {
	tests := []struct {
		version  string
		wantOut  string
		wantRows []string
	}{
		{version: "00", wantOut: "Baselined 2 migration(s) through version 00", wantRows: []string{"00"}},
		{version: "-5", wantOut: "Baselined 1 migration(s) through version -5", wantRows: []string{"-5"}},
	}
	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writeVersionOrderFiles(c, dir, compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables()),
				spelledNumberFiles)

			got := nativePtah(c, dir, "migrations", "baseline", "--version="+tt.version)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
			c.Assert(got.Stdout, qt.Contains, tt.wantOut)
			c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, tt.wantRows)
		})
	}
}

// TestNativeAtlasNumberlessVersionSetE2E sets the revision to v1 named by its
// version, and the summary and the dry run name it the same way.
func TestNativeAtlasNumberlessVersionSetE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	nativeAtlasDirectory(c, dir)

	dryRun := nativePtah(c, dir, "migrations", "set", "--version", "v1", "--dry-run")
	set := nativePtah(c, dir, "migrations", "set", "--version", "v1")

	c.Assert(dryRun.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", dryRun.Stderr))
	c.Assert(dryRun.Stdout, qt.Contains, "Dry run: would set the revision boundary to version v1.\n")
	c.Assert(set.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", set.Stderr))
	c.Assert(set.Stdout, qt.Equals, "Current version is v1 (3 set):\n\n  + 1 (a)\n  + 2 (b)\n  + v1 (x)\n\n")
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "2", "v1"})
}

// TestNativeAtlasNumberlessVersionSet_FailurePath refuses --version 3, the
// position v1_x.sql runs under, before any row is written.
func TestNativeAtlasNumberlessVersionSet_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	nativeAtlasDirectory(c, dir)

	got := nativePtah(c, dir, "migrations", "set", "--version", "3")

	c.Assert(got.ExitCode, qt.Equals, 2)
	c.Assert(got.Stderr, qt.Contains, `invalid migration version "3": no migration file carries version "3"`)
	c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.Not(qt.Contains), "atlas_schema_revisions")
}

// TestNativeAtlasNumberlessVersionBaselineE2E baselines through v1 named by
// its version, and the summary names it the same way. The Atlas table records
// the baseline as its one row.
func TestNativeAtlasNumberlessVersionBaselineE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	nativeAtlasDirectory(c, dir)

	got := nativePtah(c, dir, "migrations", "baseline", "--version", "v1", "--force")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Contains, `Baselined 3 migration(s) through version v1 in "atlas_schema_revisions"`)
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"v1"})
}

// TestNativeAtlasNumberlessVersionBaselineAndRepair_FailurePath refuses
// --version 3, the position v1_x.sql runs under, on baseline and repair.
func TestNativeAtlasNumberlessVersionBaselineAndRepair_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "baseline",
			args:    []string{"migrations", "baseline", "--version", "3", "--force"},
			wantErr: `invalid baseline version "3": no migration file carries version "3"`,
		},
		{
			name:    "repair",
			args:    []string{"migrations", "repair", "--version", "3", "--force"},
			wantErr: `invalid migration version "3": no migration file carries version "3"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			nativeAtlasDirectory(c, dir)

			got := nativePtah(c, dir, tt.args...)

			c.Assert(got.ExitCode, qt.Equals, 2)
			c.Assert(got.Stderr, qt.Contains, tt.wantErr)
		})
	}
}

// TestNativeAtlasNumberlessVersionRepairHintE2E stops v1_x.sql after its first
// statement, outside a transaction. The status hint names the repair by the
// version the file spells, with the flag that selects the Atlas revision
// table, and the repair it names runs as printed: without --revision-format
// atlas it reads the Ptah revision table, which refuses the directory. The
// directory carries atlas.sum, so the default directory format reads it as
// Atlas files and the hint names no --dir-format.
func TestNativeAtlasNumberlessVersionRepairHintE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1_a.sql"), []byte("CREATE TABLE t_a (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "v1_x.sql"),
		[]byte("-- atlas:txmode none\nCREATE TABLE t_x (id int);\nINSERT INTO t_fix (id) VALUES (1);\n"), 0o600), qt.IsNil)
	hash := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())("migrate", "hash", "--dir", "file://migrations")
	c.Assert(hash.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hash.Stderr))

	up := nativePtah(c, dir, "migrations", "up")
	status := nativePtah(c, dir, "migrations", "status")
	sqliteExec(c, filepath.Join(dir, "app.db"), "CREATE TABLE t_fix (id int)")
	printed := regexp.MustCompile(`'ptah (migrations repair [^']*)'`).FindStringSubmatch(status.Stdout)
	c.Assert(printed, qt.HasLen, 2, qt.Commentf("stdout:\n%s", status.Stdout))
	repair := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		append(strings.Fields(printed[1]), "--db-url", "sqlite://app.db", "--migrations-dir", "migrations")...)

	c.Assert(up.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(printed[1], qt.Equals,
		"migrations repair --version v1 --resume-from 2 --revision-format atlas")
	c.Assert(repair.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", repair.Stderr))
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "v1"})
}

// TestNativeAtlasNumberlessVersionApplyFailure_FailurePath names a migration
// that fails by its version: v1 for v1_x.sql, which runs under the position 2
// here, and 1 for 1_a.sql. The position appears in no file name, so a message
// naming it sends the reader to a file that does not exist.
func TestNativeAtlasNumberlessVersionApplyFailure_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		a, x string
		want string
	}{
		{
			name: "numberless file",
			a:    "CREATE TABLE t_a (id int);\n",
			x:    "CREATE TABLE t_x (id int);\nCREATE TABLE t_x (id int);\n",
			want: "failed to apply migration v1: failed to execute migration SQL: ",
		},
		{
			name: "numbered file",
			a:    "CREATE TABLE t_a (id int);\nCREATE TABLE t_a (id int);\n",
			x:    "CREATE TABLE t_x (id int);\n",
			want: "failed to apply migration 1: failed to execute migration SQL: ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			migrations := filepath.Join(dir, "migrations")
			c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(migrations, "1_a.sql"), []byte(tt.a), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(migrations, "v1_x.sql"), []byte(tt.x), 0o600), qt.IsNil)
			hash := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())(
				"migrate", "hash", "--dir", "file://migrations",
			)
			c.Assert(hash.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hash.Stderr))

			up := nativePtah(c, dir, "migrations", "up")

			c.Assert(up.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(up.Stderr, qt.Contains, tt.want)
			c.Assert(up.Stderr, qt.Not(qt.Contains), "migration 2")
		})
	}
}

// TestNativeAtlasNumberlessVersionOutOfOrder_FailurePath adds 3_c.sql after
// v1_x.sql is applied. The file sorts below v1_x.sql, so a linear run refuses
// it, and the refusal names the current version v1. The position of v1_x.sql
// moves from 2 to 3 when 3_c.sql is added, and neither number names a file.
func TestNativeAtlasNumberlessVersionOutOfOrder_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	hash := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1_a.sql"), []byte("CREATE TABLE t_a (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "v1_x.sql"), []byte("CREATE TABLE t_x (id int);\n"), 0o600), qt.IsNil)
	first := hash("migrate", "hash", "--dir", "file://migrations")
	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", first.Stderr))
	applied := nativePtah(c, dir, "migrations", "up")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
	c.Assert(os.WriteFile(filepath.Join(migrations, "3_c.sql"), []byte("CREATE TABLE t_c (id int);\n"), 0o600), qt.IsNil)
	second := hash("migrate", "hash", "--dir", "file://migrations")
	c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", second.Stderr))

	up := nativePtah(c, dir, "migrations", "up")

	c.Assert(up.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(up.Stderr, qt.Contains, "out-of-order pending migrations below current version v1: [3] ")
}

// TestNativeAutoFormatOverAnAtlasSumRefusesANumberlessVersion_FailurePath runs
// the native defaults, --dir-format auto and the Ptah revision table, over a
// directory atlas.sum covers. atlas.sum makes it an Atlas directory, so
// README_x.sql is version README there, which the Ptah table cannot keep.
// The run refuses before any migration runs, rather than apply 1_a.sql and
// leave README_x.sql out while the sum covers it. A revision set refuses too:
// set --version 2 would record README_x.sql under its position, 2, which names
// another file once a file is added before it.
func TestNativeAutoFormatOverAnAtlasSumRefusesANumberlessVersion_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "up", args: []string{"up"}},
		{name: "status", args: []string{"status"}},
		{name: "set to a position", args: []string{"set", "--version", "2"}},
		{name: "set to a numbered file", args: []string{"set", "--version", "1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writeVersionOrderFiles(c, dir, compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables()),
				[]string{"1_a.sql", "README_x.sql"})

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
				append(append([]string{"migrations"}, tt.args...), "--db-url", "sqlite://app.db", "--migrations-dir", "migrations")...)

			c.Assert(got.ExitCode, qt.Equals, 2)
			c.Assert(got.Stderr, qt.Contains, `Atlas migration README_x.sql has version "README", which a revision table that records numbers cannot keep; use the Atlas revision table (--revision-format atlas)`)
			c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.Not(qt.Contains), "t_1_a")
		})
	}
}

// TestAtlasRemovedNumberlessFileStaysReadableE2E applies 1_a.sql and
// v1_x.sql, removes v1_x.sql and rehashes. The row v1 names no file, and
// status and a set to 1 read it without a numeric cast of v1, which a strict
// dialect refuses: status names v1 as the current version, and the set removes
// the row, as the pinned community binary does on the same history.
func TestAtlasRemovedNumberlessFileStaysReadableE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	compat := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
	nativeAtlasDirectory(c, dir)
	c.Assert(os.Remove(filepath.Join(dir, "migrations", "2_b.sql")), qt.IsNil)
	writeVersionOrderFiles(c, dir, compat, nil)
	up := nativePtah(c, dir, "migrations", "up")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", up.Stderr))
	c.Assert(os.Remove(filepath.Join(dir, "migrations", "v1_x.sql")), qt.IsNil)
	writeVersionOrderFiles(c, dir, compat, nil)

	compatStatus := compat(append([]string{"migrate", "status"}, versionOrderTarget...)...)
	nativeStatus := nativePtah(c, dir, "migrations", "status")
	set := compat(append([]string{"migrate", "set", "1"}, versionOrderTarget...)...)

	c.Assert(compatStatus.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", compatStatus.Stderr))
	c.Assert(compatStatus.Stdout, qt.Contains, "  -- Current Version: v1\n")
	c.Assert(nativeStatus.Stdout, qt.Contains, "Current Version: v1\n")
	c.Assert(nativeStatus.Stdout, qt.Contains, "Missing Migration: version=v1 description=x ")
	c.Assert(set.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", set.Stderr))
	c.Assert(set.Stdout, qt.Equals, "Current version is 1 (1 removed):\n\n  - v1 (x)\n\n")
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1"})
}
