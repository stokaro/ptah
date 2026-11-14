//go:build integration

package integration_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// `ptah migrations up` asks every file of a run, before the first of them
// runs, whether the transaction the file runs in would refuse a statement of
// it. The shape is a lone CREATE INDEX CONCURRENTLY applied after an earlier
// file of the same run: PostgreSQL refuses it inside a transaction block, so
// the run is refused before it applies the earlier file or writes a revision
// row. A file sent without a transaction around it, as the golang-migrate
// runner sends a file of one statement, is not asked. `ptah-compat migrate
// apply` runs the same migrator and refuses the run the same way.
//
// What each run left is read back from the server: which of the table and the
// index exist, and how many rows the revision table holds.

// concurrentIndexRefusal is what the migrator says of the second file.
const concurrentIndexRefusal = "cannot run inside a transaction: line 1 CREATE or DROP INDEX CONCURRENTLY is " +
	"refused inside a transaction block"

// concurrentIndexDirectory writes a directory whose first migration creates
// base and whose second builds an index on it concurrently, alone in its file.
// name spells each file's name from its version, description and direction.
func concurrentIndexDirectory(c *qt.C, name func(version int, description, direction string) string) string {
	c.Helper()
	files := map[string]string{
		name(1, "base", "up"):   "CREATE TABLE base (id int);\n",
		name(1, "base", "down"): "DROP TABLE base;\n",
		name(2, "idx", "up"):    "CREATE INDEX CONCURRENTLY base_id_idx ON base (id);\n",
		name(2, "idx", "down"):  "DROP INDEX base_id_idx;\n",
	}
	return runnerMigrations(c, files)
}

// ptahFileName is a file name in Ptah's layout.
func ptahFileName(version int, description, direction string) string {
	return fmt.Sprintf("%010d_%s.%s.sql", version, description, direction)
}

// atlasFileName is a file name in Atlas's layout, whose up file carries no
// direction.
func atlasFileName(version int, description, direction string) string {
	if direction == "up" {
		return fmt.Sprintf("%d_%s.sql", version, description)
	}
	return fmt.Sprintf("%d_%s.%s.sql", version, description, direction)
}

// concurrentIndexCensus is how many rows the revision table holds, or -1 when
// there is no such table, and which of base and its index exist, read from the
// database at url.
func concurrentIndexCensus(c *qt.C, url, revisionTable string) (int, []string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows := -1
	var tableExists bool
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT to_regclass($1) IS NOT NULL", revisionTable).Scan(&tableExists),
		qt.IsNil)
	if tableExists {
		c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM "+revisionTable).Scan(&rows), qt.IsNil)
	}
	existing := make([]string, 0, 2)
	for _, relation := range []string{"base", "base_id_idx"} {
		var exists bool
		c.Assert(conn.QueryRowContext(c.Context(), "SELECT to_regclass($1) IS NOT NULL", relation).Scan(&exists), qt.IsNil)
		existing = append(existing, fmt.Sprintf("%s=%t", relation, exists))
	}
	return rows, existing
}

// migrationsUp runs `ptah migrations up` over dir against target with flags,
// in an environment that carries no PTAH_ variable.
func migrationsUp(c *qt.C, dir, target string, flags ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables()},
		append([]string{"migrations", "up", "--migrations-dir", dir, "--db-url", target}, flags...)...)
}

// The refusal comes before the first file, in the per-file transaction, in
// the one transaction of tx-mode all, and in a dry run. Nothing is created and
// no revision row is written. A dry run creates no revision table either.
func TestMigrationsUpRefusesTheConcurrentIndexBeforeTheFirstFile_FailurePathE2E(t *testing.T) {
	tests := []struct {
		name     string
		flags    []string
		wantRows int
	}{
		{name: "tx-mode file", wantRows: 0},
		{name: "tx-mode all", flags: []string{"--tx-mode", "all"}, wantRows: 0},
		{name: "dry run", flags: []string{"--dry-run"}, wantRows: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := concurrentIndexDirectory(c, ptahFileName)
			target, _ := scratchReplayDatabase(c)

			got := migrationsUp(c, dir, target, test.flags...)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, relations := concurrentIndexCensus(c, target, "schema_migrations")
			c.Assert(rows, qt.Equals, test.wantRows)
			c.Assert(relations, qt.DeepEquals, []string{"base=false", "base_id_idx=false"})
			c.Assert(got.Stderr, qt.Contains,
				"error: error running migrations: 0000000002_idx.up.sql "+concurrentIndexRefusal)
		})
	}
}

// The controls: a run that sends the file without a transaction is not
// refused, and builds the index. The golang-migrate runner sends a file of one
// statement alone, and tx-mode none sends every file without one.
func TestMigrationsUpRefusesTheConcurrentIndexBeforeTheFirstFile_HappyPathE2E(t *testing.T) {
	tests := []struct {
		name  string
		flags []string
	}{
		{name: "the golang-migrate runner", flags: []string{"--runner", "golang-migrate"}},
		{name: "tx-mode none", flags: []string{"--tx-mode", "none"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := concurrentIndexDirectory(c, ptahFileName)
			target, _ := scratchReplayDatabase(c)

			got := migrationsUp(c, dir, target, test.flags...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, relations := concurrentIndexCensus(c, target, "schema_migrations")
			c.Assert(rows, qt.Equals, 2)
			c.Assert(relations, qt.DeepEquals, []string{"base=true", "base_id_idx=true"})
		})
	}
}

// `ptah-compat migrate apply` refuses the same run before the first file, and
// a dry run refuses it too. The community binary applies the first file and
// then sends the build, which PostgreSQL refuses, and its dry run reports both
// files ok; the retained divergence for it cites this test.
func TestAtlasMigrateApplyRefusesTheConcurrentIndexBeforeTheFirstFileE2E(t *testing.T) {
	tests := []struct {
		name     string
		flags    []string
		wantRows int
	}{
		{name: "apply", wantRows: 0},
		{name: "dry run", flags: []string{"--dry-run"}, wantRows: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := concurrentIndexDirectory(c, atlasFileName)
			target, _ := scratchReplayDatabase(c)
			options := clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables()}
			hashed := clirun.Run(c, clirun.Compat, options, "migrate", "hash", "--dir", "file://"+dir)
			c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("%s", hashed.Stderr))

			got := clirun.Run(c, clirun.Compat, options,
				append([]string{"migrate", "apply", "--url", target, "--dir", "file://" + dir}, test.flags...)...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, relations := concurrentIndexCensus(c, target, "atlas_schema_revisions.atlas_schema_revisions")
			c.Assert(rows, qt.Equals, test.wantRows)
			c.Assert(relations, qt.DeepEquals, []string{"base=false", "base_id_idx=false"})
			c.Assert(got.Stderr, qt.Contains, "Error: error applying migrations: 2_idx.sql "+concurrentIndexRefusal)
		})
	}
}
