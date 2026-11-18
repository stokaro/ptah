//go:build integration

package integration_test

import (
	"path/filepath"
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
// spells, wherever the native commands name it. The file runs under an order
// key above every number in the directory, 3 here, which no file carries, and
// which moves to 4 when a 3_c.sql is added.
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
// the order key v1_x.sql runs under, and names the file and its version. A
// target the directory does not spell is refused too.
func TestNativeAtlasNumberlessVersionTarget_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		wantErr string
	}{
		{
			name:    "the order key",
			target:  "3",
			wantErr: `invalid target version "3": no migration file carries version 3: Ptah numbers v1_x.sql after the highest numbered file, so name that file by its version, v1`,
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

// TestNativeAutoFormatOverAnAtlasSumRefusesANumberlessVersion_FailurePath runs
// the native defaults, --dir-format auto and the Ptah revision table, over a
// directory atlas.sum covers. atlas.sum makes it an Atlas directory, so
// README_x.sql is version README there, which the Ptah table cannot keep.
// The run refuses before any migration runs, rather than apply 1_a.sql and
// leave README_x.sql out while the sum covers it.
func TestNativeAutoFormatOverAnAtlasSumRefusesANumberlessVersion_FailurePath(t *testing.T) {
	for _, command := range []string{"up", "status"} {
		t.Run(command, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writeVersionOrderFiles(c, dir, compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables()),
				[]string{"1_a.sql", "README_x.sql"})

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
				"migrations", command, "--db-url", "sqlite://app.db", "--migrations-dir", "migrations")

			c.Assert(got.ExitCode, qt.Equals, 2)
			c.Assert(got.Stderr, qt.Contains, `Atlas migration README_x.sql has version "README", which a revision table that records numbers cannot keep; use the Atlas revision table (--revision-format atlas)`)
			c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.Not(qt.Contains), "t_1_a")
		})
	}
}
