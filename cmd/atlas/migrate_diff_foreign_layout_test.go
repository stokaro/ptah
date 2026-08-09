package atlas_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests pin the last cell of stokaro/ptah#1013: `migrate diff` writing a
// migration directory in a layout other than the native Atlas one.
//
// Measured against the pinned community binary v1.3.0 on 2026-08-08, against a
// PostgreSQL dev database, running `migrate diff` into an EMPTY directory once
// per layout. That binary exits 0 on all five and writes reverse SQL as well as
// forward SQL, in three different shapes:
//
//	golang-migrate  20260808232952_demo.up.sql + 20260808232952_demo.down.sql
//	flyway          V20260808233006__demo.sql  + U20260808233006__demo.sql
//	goose           one file: "-- +goose Up" … "-- +goose Down" …
//	dbmate          one file: "-- migrate:up" … "-- migrate:down" …
//	liquibase       one file: "--changeset atlas:<v>-1" … "--rollback: …"
//
// Ptah refused every one of them until this change, because its `migrate diff`
// planned forward statements only. It now injects the reverse rule
// `migration/generator` has always applied to its own `.down.sql` half, so the
// plan carries both directions and each layout composes its own files.
//
// The SQL text is Ptah's own renderer's on every layout, including the native
// one, and always was. Matching the LAYOUT is what these close.

// compatGolangMigrateFixture writes a golang-migrate migration directory
// holding one hashed migration, and returns it with a desired-state .sql file
// that adds a second table.
//
// The layout is what makes the flip visible rather than an exit 0 that could
// have happened anyway: `atlas.sum` is written over the golang-migrate covered
// set, which is the `.up.sql` half alone, so the SAME directory read as native
// Atlas is a checksum error. Every row below therefore turns on the layout
// selection and nothing else, and the control row proves it.
func compatGolangMigrateFixture(c *qt.C) (dir, target string) {
	c.Helper()
	dir = filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "1_init.up.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "1_init.down.sql"),
		[]byte("DROP TABLE widgets;\n"),
		0o600,
	), qt.IsNil)
	_, _, err := runCompat("migrate", "hash", "--dir", "file://"+dir+"?format=golang-migrate")
	c.Assert(err, qt.IsNil)

	target = filepath.Join(c.TempDir(), "target.sql")
	c.Assert(os.WriteFile(
		target,
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	return dir, target
}

// TestCompatMigrateDiff_GolangMigrateQueryWritesThatLayout is the completion
// criterion stokaro/ptah#1013 states, with its control.
//
// On a golang-migrate directory, `migrate diff demo --dir 'file://gm?format=golang-migrate'`
// must exit 0 and add a migration in that layout with a refreshed atlas.sum over
// that layout's covered file set. It exited 1 and wrote nothing before this
// change. The control must stay red: the same directory with no layout
// selection keeps exiting 1 with the checksum error, writing nothing, on both
// tools.
func TestCompatMigrateDiff_GolangMigrateQueryWritesThatLayout(t *testing.T) {
	c := qt.New(t)
	dir, target := compatGolangMigrateFixture(c)

	_, _, err := runCompat("migrate", "diff", "more",
		"--dir", "file://"+dir+"?format=golang-migrate",
		"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
		"--to", "file://"+target)

	c.Assert(err, qt.IsNil)
	names := atlasDirEntryNames(c, dir)
	c.Assert(compatNamesWithSuffix(names, ".up.sql"), qt.HasLen, 2)
	c.Assert(compatNamesWithSuffix(names, ".down.sql"), qt.HasLen, 2)
	// The rollback half of the new migration is real SQL, not the empty file
	// `migrate new` leaves. A pair whose down half is empty would satisfy the
	// layout and still hand the operator a migration nothing can roll back.
	written := compatNewestNameWithSuffix(c, names, ".down.sql")
	rollback, readErr := os.ReadFile(filepath.Join(dir, written))
	c.Assert(readErr, qt.IsNil)
	c.Assert(strings.ToUpper(string(rollback)), qt.Contains, "DROP TABLE",
		qt.Commentf("rollback file %s", written))
	// atlas.sum covers the forward halves only, which is what the community
	// binary writes for this layout and what its own `migrate validate` then
	// accepts. Covering the pair would produce a sum both tools refuse.
	sum, readErr := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(strings.Count(string(sum), ".up.sql"), qt.Equals, 2)
	c.Assert(string(sum), qt.Not(qt.Contains), ".down.sql")
}

// TestCompatMigrateDiff_GolangMigrateDirIsRefusedWithoutTheLayout is the
// control arm of the test above, and it is what makes that one a flip rather
// than an exit 0 that could have happened anyway.
//
// The same fixture, the same command, no layout selection: read as native
// Atlas the covered set includes the `.down.sql` half that atlas.sum never
// recorded, so both tools report a checksum error and neither writes.
func TestCompatMigrateDiff_GolangMigrateDirIsRefusedWithoutTheLayout(t *testing.T) {
	c := qt.New(t)
	dir, target := compatGolangMigrateFixture(c)
	before := atlasDirEntryNames(c, dir)

	_, _, err := runCompat("migrate", "diff", "more",
		"--dir", "file://"+dir,
		"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
		"--to", "file://"+target)

	c.Assert(err, qt.ErrorMatches, `checksum mismatch`)
	c.Assert(atlasDirEntryNames(c, dir), qt.DeepEquals, before)
}

// TestCompatMigrateDiff_DirFormatFlagWritesTheSelectedLayout pins the same
// completion criterion through the other spelling.
//
// The issue's criterion names both: `?format=golang-migrate` and
// `--dir-format golang-migrate` must reach the same place. They resolve through
// one function ([resolveWritingVerbDirFormat]) precisely so they cannot drift,
// and this is the row that would notice if they did.
func TestCompatMigrateDiff_DirFormatFlagWritesTheSelectedLayout(t *testing.T) {
	c := qt.New(t)
	dir, target := compatGolangMigrateFixture(c)

	_, _, err := runCompat("migrate", "diff", "more",
		"--dir", "file://"+dir,
		"--dir-format", "golang-migrate",
		"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
		"--to", "file://"+target)

	c.Assert(err, qt.IsNil)
	c.Assert(compatNamesWithSuffix(atlasDirEntryNames(c, dir), ".up.sql"), qt.HasLen, 2)
}

// TestCompatMigrateDiff_ForeignLayoutComposesEachLayoutsFiles pins what each
// layout WRITES, across the whole format axis rather than on the one layout the
// issue names.
//
// All five rows are here because the layouts do not close together: the paired
// ones (golang-migrate, flyway) put the rollback in a second file, the
// directive ones (goose, dbmate) put it under a header in the same file, and
// liquibase attaches it to a changeset. A change that taught this verb
// golang-migrate alone would leave the other four writing a directory with no
// rollback half at all while a single-layout test stayed green.
//
// Each row asserts the file NAMES the layout requires and a marker of the
// rollback half, because the exit code alone cannot tell a composed layout from
// a native Atlas file that happened to be written under a foreign name.
func TestCompatMigrateDiff_ForeignLayoutComposesEachLayoutsFiles(t *testing.T) {
	tests := []struct {
		name   string
		format string
		check  func(c *qt.C, dir string, names []string)
	}{
		{
			name:   "golang-migrate writes a pair",
			format: "golang-migrate",
			check: func(c *qt.C, dir string, names []string) {
				c.Assert(compatNamesWithSuffix(names, ".up.sql"), qt.HasLen, 1)
				assertCompatRollbackSection(c, dir, compatNewestNameWithSuffix(c, names, ".down.sql"), "")
			},
		},
		{
			name:   "flyway writes a V and a U file",
			format: "flyway",
			check: func(c *qt.C, dir string, names []string) {
				c.Assert(compatNamesWithPrefix(names, "V"), qt.HasLen, 1)
				assertCompatRollbackSection(c, dir, compatNewestNameWithPrefix(c, names, "U"), "")
			},
		},
		{
			name:   "goose writes both halves under its directives",
			format: "goose",
			check: func(c *qt.C, dir string, names []string) {
				assertCompatRollbackSection(c, dir, compatNewestNameWithSuffix(c, names, "_demo.sql"), "-- +goose Down")
			},
		},
		{
			name:   "dbmate writes both halves under its directives",
			format: "dbmate",
			check: func(c *qt.C, dir string, names []string) {
				assertCompatRollbackSection(c, dir, compatNewestNameWithSuffix(c, names, "_demo.sql"), "-- migrate:down")
			},
		},
		{
			name:   "liquibase attaches the rollback to a changeset",
			format: "liquibase",
			check: func(c *qt.C, dir string, names []string) {
				assertCompatRollbackSection(c, dir, compatNewestNameWithSuffix(c, names, "_demo.sql"), "--rollback: ")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			target := filepath.Join(c.TempDir(), "target.sql")
			c.Assert(os.WriteFile(
				target,
				[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"),
				0o600,
			), qt.IsNil)

			_, _, err := runCompat("migrate", "diff", "demo",
				"--dir", "file://"+dir+"?format="+tt.format,
				"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
				"--to", "file://"+target)

			c.Assert(err, qt.IsNil)
			tt.check(c, dir, atlasDirEntryNames(c, dir))
		})
	}
}

// assertCompatRollbackSection asserts that the named file opens the layout's
// rollback container AND that real reverse SQL follows it.
//
// Both halves are the assertion. The container alone is emitted whether or not
// anything was planned — an empty `.down.sql`, a bare `-- +goose Down` — so a
// row that stopped at the container would stay green on a run that planned no
// reverse at all, which is precisely the state this change moved away from.
// Measured: with the reverse rule stubbed out to plan nothing, the container
// assertions still passed on goose and dbmate, and only the DROP TABLE half
// below failed.
//
// opener is "" for the two layouts whose rollback is a file of its own, where
// the container IS the file.
func assertCompatRollbackSection(c *qt.C, dir, name, opener string) {
	c.Helper()
	contents, err := os.ReadFile(filepath.Join(dir, name))
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Contains, opener,
		qt.Commentf("%s does not open the layout's rollback container", name))
	_, rollback, _ := strings.Cut(string(contents), opener)
	c.Assert(strings.ToUpper(rollback), qt.Contains, "DROP TABLE",
		qt.Commentf("%s carries no reverse SQL after %q", name, opener))
}

func compatNamesWithSuffix(names []string, suffix string) []string {
	return compatFilterNames(names, func(name string) bool {
		return strings.HasSuffix(name, suffix)
	})
}

func compatNamesWithPrefix(names []string, prefix string) []string {
	return compatFilterNames(names, func(name string) bool {
		return strings.HasPrefix(name, prefix) && strings.HasSuffix(name, ".sql")
	})
}

func compatFilterNames(names []string, keep func(string) bool) []string {
	return slices.DeleteFunc(slices.Clone(names), func(name string) bool {
		return !keep(name)
	})
}

func compatNewestNameWithSuffix(c *qt.C, names []string, suffix string) string {
	c.Helper()
	matched := compatNamesWithSuffix(names, suffix)
	c.Assert(len(matched) > 0, qt.IsTrue, qt.Commentf("no file ending %q in %v", suffix, names))
	return matched[len(matched)-1]
}

func compatNewestNameWithPrefix(c *qt.C, names []string, prefix string) string {
	c.Helper()
	matched := compatNamesWithPrefix(names, prefix)
	c.Assert(len(matched) > 0, qt.IsTrue, qt.Commentf("no file starting %q in %v", prefix, names))
	return matched[len(matched)-1]
}
