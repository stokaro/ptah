//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestCompatAtlasSetOnClickHouseCreatesNothing_FailurePath runs a compat
// `migrate set` that Ptah refuses on ClickHouse, where a revision table is
// rewritten through mutations, and finds no revision table afterwards. A table
// created before the refusal would be adopted by the next apply whatever
// engine it was created with, and PTAH_MIGRATIONS_ENGINE names the engine that
// apply expects.
func TestCompatAtlasSetOnClickHouseCreatesNothing_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "a version", args: []string{"1"}, wantErr: "setting an Atlas revision is not supported for ClickHouse"},
		{name: "no version", wantErr: "accepts 1 arg(s), received 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			url := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			dir := c.TempDir()
			env := append(environmentWithoutPtahVariables(), "PTAH_MIGRATIONS_ENGINE=ReplacingMergeTree")
			run := compatVersionOrderRunner(c, dir, env)
			writeVersionOrderFiles(c, dir, run, []string{"1_a.sql"})

			got := run(append([]string{"migrate", "set", "--url", url, "--dir", "file://migrations"}, tt.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, tt.wantErr)
			var tables int
			conn := connectDevDialect(c, url)
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'atlas_schema_revisions'",
			).Scan(&tables), qt.IsNil)
			c.Assert(tables, qt.Equals, 0)
		})
	}
}
