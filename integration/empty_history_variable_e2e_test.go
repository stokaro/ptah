//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// The tests in this file drive the variables that say where the history of
// applied migrations is: PTAH_MIGRATIONS_TABLE, PTAH_MIGRATIONS_SCHEMA and
// PTAH_REVISION_FORMAT on `ptah migrations`, and PTAH_REVISIONS_SCHEMA on
// `ptah-compat migrate`. Read as unset, an empty one names the default history,
// and over a history recorded somewhere else a run finds nothing applied. Each
// is refused before the command connects. A typed flag never reads its
// variable, so a typed value beside an empty variable is the control that the
// refusal is about the variable alone.

// recordedHistoryTables are the revision and log tables a history can be
// recorded in, under the table name mine or the defaults of each format.
const recordedHistoryTables = "('mine', 'mine_log', 'schema_migrations', 'schema_migrations_log', 'atlas_schema_revisions')"

// historyTablesOf lists which of recordedHistoryTables the SQLite database at
// targetURL holds.
func historyTablesOf(c *qt.C, targetURL string) []string {
	c.Helper()
	return limitHistoryColumn(c, targetURL,
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN "+recordedHistoryTables+" ORDER BY name")
}

// mineHistory applies the directory [nativeStopHistory] writes with
// --migrations-table mine, so the history is in mine and mine_log, and writes
// desired.sql, which declares the three tables the directory creates.
func mineHistory(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir, targetURL = nativeStopHistory(c)
	desired := "CREATE TABLE a (id INTEGER PRIMARY KEY);\nCREATE TABLE b (id INTEGER PRIMARY KEY);\n" +
		"CREATE TABLE c (id INTEGER PRIMARY KEY);\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "desired.sql"), []byte(desired), 0o600), qt.IsNil)
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables()},
		"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations", "--migrations-table", "mine")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
	c.Assert(historyTablesOf(c, targetURL), qt.DeepEquals, []string{"mine", "mine_log"})
	return dir, targetURL
}

// migrationFileCount counts the files in the migrations directory of dir.
func migrationFileCount(c *qt.C, dir string) int {
	c.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "migrations"))
	c.Assert(err, qt.IsNil)
	return len(entries)
}

// TestMigrationsHistoryVariablesE2E_HappyPath applies a directory to an empty
// database and reads back where the history was recorded.
func TestMigrationsHistoryVariablesE2E_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  []string
		want []string
	}{
		{name: "unset, the history is in the default tables", want: []string{"schema_migrations", "schema_migrations_log"}},
		{name: "PTAH_MIGRATIONS_TABLE names the table", env: []string{"PTAH_MIGRATIONS_TABLE=mine"}, want: []string{"mine", "mine_log"}},
		{name: "PTAH_REVISION_FORMAT names the layout", env: []string{"PTAH_REVISION_FORMAT=atlas"}, want: []string{"atlas_schema_revisions"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := nativeStopHistory(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
			c.Assert(historyTablesOf(c, targetURL), qt.DeepEquals, tc.want)
		})
	}
}

// TestMigrationsHistoryVariablesOverARecordedHistoryE2E_HappyPath runs
// `migrations up` over the history in mine: named by its variable, or typed
// beside an empty one, the table is read, every migration is applied and
// nothing is recorded anywhere else.
func TestMigrationsHistoryVariablesOverARecordedHistoryE2E_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  []string
		args []string
	}{
		{name: "PTAH_MIGRATIONS_TABLE names the table", env: []string{"PTAH_MIGRATIONS_TABLE=mine"}},
		{
			name: "a typed --migrations-table beside an empty PTAH_MIGRATIONS_TABLE",
			env:  []string{"PTAH_MIGRATIONS_TABLE="},
			args: []string{"--migrations-table", "mine"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := mineHistory(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				append([]string{"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations"}, tc.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
			c.Assert(historyTablesOf(c, targetURL), qt.DeepEquals, []string{"mine", "mine_log"})
		})
	}
}

// TestMigrationsHistoryVariablesE2E_FailurePath runs the verbs that read the
// history over the one in mine with an empty or blank variable. Each refuses
// before it connects, so no default table is created, no migration runs again
// and no file is written. SQLite has no schemas, so an empty
// PTAH_MIGRATIONS_SCHEMA is refused here for uniformity; the live test shows
// the history it would miss.
func TestMigrationsHistoryVariablesE2E_FailurePath(t *testing.T) {
	t.Parallel()
	const tableRefusal = "or unset it and leave --migrations-table out to use the project file's table or the revision format default\n"
	tests := []struct {
		name       string
		env        []string
		args       []string
		wantStderr string
	}{
		{
			name:       "migrations up, empty PTAH_MIGRATIONS_TABLE",
			env:        []string{"PTAH_MIGRATIONS_TABLE="},
			args:       []string{"migrations", "up"},
			wantStderr: "error: PTAH_MIGRATIONS_TABLE is set but empty: give it a value, " + tableRefusal,
		},
		{
			name:       "migrations up, blank PTAH_MIGRATIONS_TABLE",
			env:        []string{"PTAH_MIGRATIONS_TABLE= "},
			args:       []string{"migrations", "up"},
			wantStderr: `error: PTAH_MIGRATIONS_TABLE is set to " ", which names nothing: give it a value, ` + tableRefusal,
		},
		{
			name:       "migrations status, empty PTAH_MIGRATIONS_TABLE",
			env:        []string{"PTAH_MIGRATIONS_TABLE="},
			args:       []string{"migrations", "status"},
			wantStderr: "error: PTAH_MIGRATIONS_TABLE is set but empty: give it a value, " + tableRefusal,
		},
		{
			name:       "migrations down, empty PTAH_MIGRATIONS_TABLE",
			env:        []string{"PTAH_MIGRATIONS_TABLE="},
			args:       []string{"migrations", "down", "--target", "0", "--confirm"},
			wantStderr: "error: PTAH_MIGRATIONS_TABLE is set but empty: give it a value, " + tableRefusal,
		},
		{
			name:       "migrations generate, empty PTAH_MIGRATIONS_TABLE",
			env:        []string{"PTAH_MIGRATIONS_TABLE="},
			args:       []string{"migrations", "generate", "--schema-file", "desired.sql"},
			wantStderr: "error: PTAH_MIGRATIONS_TABLE is set but empty: give it a value, " + tableRefusal,
		},
		{
			name: "migrations up, empty PTAH_MIGRATIONS_SCHEMA",
			env:  []string{"PTAH_MIGRATIONS_TABLE=mine", "PTAH_MIGRATIONS_SCHEMA="},
			args: []string{"migrations", "up"},
			wantStderr: "error: PTAH_MIGRATIONS_SCHEMA is set but empty: give it a value, or unset it and leave " +
				"--migrations-schema out to use the project file's schema or the default schema\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := mineHistory(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				append(tc.args, "--db-url", targetURL, "--migrations-dir", "migrations")...)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, tc.wantStderr)
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
			c.Assert(historyTablesOf(c, targetURL), qt.DeepEquals, []string{"mine", "mine_log"})
			c.Assert(migrationFileCount(c, dir), qt.Equals, 6)
		})
	}
}

// atlasLayoutHistory applies the directory [nativeStopHistory] writes with
// --revision-format atlas, so the history is in atlas_schema_revisions.
func atlasLayoutHistory(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir, targetURL = nativeStopHistory(c)
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables()},
		"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations", "--revision-format", "atlas")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(historyTablesOf(c, targetURL), qt.DeepEquals, []string{"atlas_schema_revisions"})
	return dir, targetURL
}

// TestMigrationsRevisionFormatVariableE2E_HappyPath runs `migrations up` over
// the history in atlas_schema_revisions with the layout named by its variable,
// or typed beside an empty one: every migration is applied, and no ptah
// layout table is created.
func TestMigrationsRevisionFormatVariableE2E_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  []string
		args []string
	}{
		{name: "PTAH_REVISION_FORMAT names the layout", env: []string{"PTAH_REVISION_FORMAT=atlas"}},
		{
			name: "a typed --revision-format beside an empty PTAH_REVISION_FORMAT",
			env:  []string{"PTAH_REVISION_FORMAT="},
			args: []string{"--revision-format", "atlas"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := atlasLayoutHistory(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				append([]string{"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations"}, tc.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(historyTablesOf(c, targetURL), qt.DeepEquals, []string{"atlas_schema_revisions"})
		})
	}
}

// TestMigrationsRevisionFormatVariableE2E_FailurePath refuses an empty
// PTAH_REVISION_FORMAT over the history in atlas_schema_revisions. Read as
// unset, it would name the ptah layout, find nothing applied and run
// migration 1 again.
func TestMigrationsRevisionFormatVariableE2E_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir, targetURL := atlasLayoutHistory(c)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables("PTAH_REVISION_FORMAT=")},
		"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations")

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Equals, "error: PTAH_REVISION_FORMAT is set but empty: give it a value, or unset it and leave "+
		"--revision-format out to use the project file's format or ptah\n")
	c.Assert(historyTablesOf(c, targetURL), qt.DeepEquals, []string{"atlas_schema_revisions"})
}

// TestMigrationsGenerateHistoryTableE2E_HappyPath generates a migration over
// the history in mine. Unset, the variable leaves the default tables out of the
// comparison, and the migration drops the history: what an empty value read as
// unset would write. Named, the history is left out.
func TestMigrationsGenerateHistoryTableE2E_HappyPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		env  []string
		want []string
	}{
		{name: "unset, the migration drops the history", want: []string{`DROP TABLE "mine";`, `DROP TABLE "mine_log";`}},
		{name: "PTAH_MIGRATIONS_TABLE leaves the history out", env: []string{"PTAH_MIGRATIONS_TABLE=mine"}, want: make([]string, 0)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := mineHistory(c)

			got, statements := generatedUpStatements(c, dir, targetURL, tc.env)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(statements, qt.DeepEquals, tc.want)
		})
	}
}

// TestCompatMigrateDownReadsNoNativeHistoryVariableE2E_HappyPath rolls back
// a history `ptah-compat migrate apply` recorded. The revision table is
// Atlas's, which `migrate apply` and `migrate status` read whatever the
// variables of `ptah migrations` say, so `migrate down` reads it too: a value
// naming another table, schema or layout and an empty value each revert the
// last applied migration.
func TestCompatMigrateDownReadsNoNativeHistoryVariableE2E_HappyPath(t *testing.T) {
	t.Parallel()
	variables := []string{
		"PTAH_MIGRATIONS_TABLE=nope", "PTAH_MIGRATIONS_TABLE=",
		"PTAH_MIGRATIONS_SCHEMA=nope", "PTAH_MIGRATIONS_SCHEMA=",
		"PTAH_REVISION_FORMAT=ptah", "PTAH_REVISION_FORMAT=",
	}
	for _, variable := range variables {
		t.Run(variable, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := appliedLimitHistory(c)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables(variable)},
				"migrate", "down", "--url", targetURL, "--dir", "file://migrations")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b"})
			c.Assert(limitHistoryRevisions(c, targetURL), qt.DeepEquals, []string{"1", "2"})
			c.Assert(historyTablesOf(c, targetURL), qt.DeepEquals, []string{"atlas_schema_revisions"})
		})
	}
}

// TestCompatRevisionsSchemaVariableE2E_FailurePath runs every `ptah-compat
// migrate` verb that reads the revision table with an empty or blank
// PTAH_REVISIONS_SCHEMA. Each refuses before it connects, on both paths of
// `migrate down`.
func TestCompatRevisionsSchemaVariableE2E_FailurePath(t *testing.T) {
	t.Parallel()
	const refusal = "give it a value, or unset it and leave --revisions-schema out to use the project file's schema or the Atlas default\n"
	tests := []struct {
		name       string
		value      string
		args       []string
		wantStderr string
	}{
		{name: "migrate apply", args: []string{"migrate", "apply"}, wantStderr: "Error: PTAH_REVISIONS_SCHEMA is set but empty: " + refusal},
		{
			name:       "migrate apply, blank",
			value:      " ",
			args:       []string{"migrate", "apply"},
			wantStderr: `Error: PTAH_REVISIONS_SCHEMA is set to " ", which names nothing: ` + refusal,
		},
		{name: "migrate status", args: []string{"migrate", "status"}, wantStderr: "Error: PTAH_REVISIONS_SCHEMA is set but empty: " + refusal},
		{name: "migrate set", args: []string{"migrate", "set", "2"}, wantStderr: "Error: PTAH_REVISIONS_SCHEMA is set but empty: " + refusal},
		{name: "migrate down", args: []string{"migrate", "down"}, wantStderr: "Error: PTAH_REVISIONS_SCHEMA is set but empty: " + refusal},
		{
			name:       "migrate down --format",
			args:       []string{"migrate", "down", "--format", compatDownFormat},
			wantStderr: "Error: PTAH_REVISIONS_SCHEMA is set but empty: " + refusal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir, targetURL := appliedLimitHistory(c)

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables("PTAH_REVISIONS_SCHEMA=" + tc.value)},
				append(tc.args, "--url", targetURL, "--dir", "file://migrations")...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, tc.wantStderr)
			c.Assert(limitHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
			c.Assert(limitHistoryRevisions(c, targetURL), qt.DeepEquals, []string{"1", "2", "3"})
		})
	}
}

// TestCompatMigrateTestRevisionsSchemaVariableE2E_FailurePath refuses an empty
// PTAH_REVISIONS_SCHEMA on `ptah-compat migrate test` before it runs a case.
func TestCompatMigrateTestRevisionsSchemaVariableE2E_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir, _ := pendingLimitHistory(c)

	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables("PTAH_REVISIONS_SCHEMA=")},
		"migrate", "test", "--dir", "file://migrations")

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Equals, "Error: PTAH_REVISIONS_SCHEMA is set but empty: give it a value, or unset it and leave "+
		"--revisions-schema out to use the project file's schema or the Atlas default\n")
}
