//go:build integration

package integration_test

import (
	"cmp"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// `ptah-compat migrate rm`, `rebase` and `edit` refuse a version the database
// has applied, given a database through PTAH_DB_URL. The guard reads the
// history `migrate apply` writes on this surface, Atlas's
// atlas_schema_revisions. Reading Ptah's schema_migrations instead, it finds
// nothing applied, creates that table, and removes, moves or rewrites the
// file of an applied migration. The community binary has none of the three
// verbs; Ptah keeps them.
//
// Each test applies version 1 of the directory 1_a, 2_b, 3_c through
// `ptah-compat migrate apply`, runs a verb, and reads the directory and the
// database back.

// maintenanceGuardEnv is the environment of a run that gives the guard
// database and an editor that leaves the file as it is.
func maintenanceGuardEnv(database string) []string {
	return append(environmentWithoutPtahVariables(), "PTAH_DB_URL=sqlite://"+database, "EDITOR=true", "VISUAL=true")
}

// newMaintenanceGuardDir writes and hashes the directory, applies version 1 to
// app.db through `ptah-compat migrate apply`, and returns the project
// directory.
func newMaintenanceGuardDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.Mkdir(filepath.Join(dir, "migrations"), 0o755), qt.IsNil)
	for name, body := range map[string]string{
		"1_a.sql": "CREATE TABLE a (id integer);\n",
		"2_b.sql": "CREATE TABLE b (id integer);\n",
		"3_c.sql": "CREATE TABLE c (id integer);\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, "migrations", name), []byte(body), 0o600), qt.IsNil)
	}
	for _, args := range [][]string{
		{"migrate", "hash", "--dir", "file://migrations"},
		{"migrate", "apply", "--url", "sqlite://app.db", "--dir", "file://migrations", "1"},
	} {
		got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, args...)
		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%v\nstdout:\n%s\nstderr:\n%s", args, got.Stdout, got.Stderr))
	}
	return dir
}

// migrationOrder returns the description of each migration file in dir, in
// the order the versions run: 1_a.sql is a. A rebase gives a file a new
// version after every other, which this reads as a move to the end.
func migrationOrder(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	type file struct {
		version     int64
		description string
	}
	var files []file
	for _, entry := range entries {
		name, isSQL := strings.CutSuffix(entry.Name(), ".sql")
		if !isSQL {
			continue
		}
		version, description, found := strings.Cut(name, "_")
		c.Assert(found, qt.IsTrue, qt.Commentf("%s", entry.Name()))
		number, err := strconv.ParseInt(version, 10, 64)
		c.Assert(err, qt.IsNil)
		files = append(files, file{version: number, description: description})
	}
	slices.SortFunc(files, func(a, b file) int { return cmp.Compare(a.version, b.version) })
	order := make([]string, 0, len(files))
	for _, f := range files {
		order = append(order, f.description)
	}
	return order
}

func TestCompatMaintenanceVerbsRefuseAnAppliedVersionE2E_FailurePath(t *testing.T) {
	for _, verb := range []string{"rm", "rebase", "edit"} {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			dir := newMaintenanceGuardDir(c)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: maintenanceGuardEnv("app.db")},
				"migrate", verb, "1", "--dir", "file://migrations")

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "migration version 1 is already applied; refusing to modify applied history")
			c.Assert(migrationOrder(c, filepath.Join(dir, "migrations")), qt.DeepEquals, []string{"a", "b", "c"})
			c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"a", "atlas_schema_revisions"})
		})
	}
}

// TestCompatMaintenanceVerbsRefuseATypedRevisionFormatE2E_FailurePath: the
// guard's layout is the adapter's to choose, so a native --revision-format is
// refused rather than forwarded after it. Forwarded, `--revision-format ptah`
// would point the guard at schema_migrations and remove the applied file.
func TestCompatMaintenanceVerbsRefuseATypedRevisionFormatE2E_FailurePath(t *testing.T) {
	for _, verb := range []string{"rm", "rebase", "edit"} {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			dir := newMaintenanceGuardDir(c)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: maintenanceGuardEnv("app.db")},
				"migrate", verb, "1", "--dir", "file://migrations", "--revision-format", "ptah")

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "atlas migrate "+verb+" does not accept native Ptah flag --revision-format")
			c.Assert(migrationOrder(c, filepath.Join(dir, "migrations")), qt.DeepEquals, []string{"a", "b", "c"})
			c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"a", "atlas_schema_revisions"})
		})
	}
}

// TestCompatMaintenanceVerbsProceedOnAPendingVersionE2E_HappyPath runs each
// verb on version 2, which the database has not applied, and over a database
// with no history at all. The verb does its work, and the guard's read
// creates no revision table in either database.
func TestCompatMaintenanceVerbsProceedOnAPendingVersionE2E_HappyPath(t *testing.T) {
	tests := []struct {
		verb       string
		database   string
		wantOrder  []string
		wantTables []string
	}{
		{verb: "rm", database: "app.db", wantOrder: []string{"a", "c"}, wantTables: []string{"a", "atlas_schema_revisions"}},
		{verb: "rebase", database: "app.db", wantOrder: []string{"a", "c", "b"}, wantTables: []string{"a", "atlas_schema_revisions"}},
		{verb: "edit", database: "app.db", wantOrder: []string{"a", "b", "c"}, wantTables: []string{"a", "atlas_schema_revisions"}},
		{verb: "rm", database: "fresh.db", wantOrder: []string{"a", "c"}, wantTables: make([]string, 0)},
		{verb: "rebase", database: "fresh.db", wantOrder: []string{"a", "c", "b"}, wantTables: make([]string, 0)},
		{verb: "edit", database: "fresh.db", wantOrder: []string{"a", "b", "c"}, wantTables: make([]string, 0)},
	}

	for _, test := range tests {
		t.Run(test.verb+" over "+test.database, func(t *testing.T) {
			c := qt.New(t)
			dir := newMaintenanceGuardDir(c)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: maintenanceGuardEnv(test.database)},
				"migrate", test.verb, "2", "--dir", "file://migrations")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(migrationOrder(c, filepath.Join(dir, "migrations")), qt.DeepEquals, test.wantOrder)
			c.Assert(sqliteTables(c, filepath.Join(dir, test.database)), qt.DeepEquals, test.wantTables)
		})
	}
}

// TestMigrationsRmOverADatabaseWithNoHistoryE2E_HappyPath is the native verb
// the compat ones forward to: its guard reads a database with no history
// without creating schema_migrations, and says nothing about the read.
func TestMigrationsRmOverADatabaseWithNoHistoryE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := newMaintenanceGuardDir(c)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"migrations", "rm", "--version", "2", "--migrations-dir", "migrations", "--db-url", "sqlite://fresh.db")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Equals, "")
	c.Assert(migrationOrder(c, filepath.Join(dir, "migrations")), qt.DeepEquals, []string{"a", "c"})
	c.Assert(sqliteTables(c, filepath.Join(dir, "fresh.db")), qt.DeepEquals, make([]string, 0))
}
