package atlas_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

// TestSchemaApplyUnmatchedExcludeOptInIsResolvedBeforeAnyWork is the
// early-return control for PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE, and the
// no-mutation guard for it.
//
// The selector NAMES AN OBJECT THAT EXISTS, so there is no unmatched selector on
// this run and the opt-in's value cannot change what the command does. That is
// the shape of every healthy run of a pipeline that exports the variable, and
// before stokaro/ptah#1334 it was the shape on which a typo was invisible: the
// value was read beside the refusal, and the refusal never fired.
//
// The row assertions are not the exit code alone. `applied` reads the target
// back through the real reader afterwards, so a refusal that landed after the
// plan was carried out would fail here even though the exit code was 1.
func TestSchemaApplyUnmatchedExcludeOptInIsResolvedBeforeAnyWork(t *testing.T) {
	tests := []struct {
		name        string
		env         func(testing.TB)
		wantErr     string
		wantApplied bool
	}{
		{
			name:        "an unparsable value refuses with every selector matched",
			env:         envbooltest.Set(atlasfilter.AllowUnmatchedExcludeEnvVar, "maybe"),
			wantErr:     `invalid boolean value "maybe" for PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE`,
			wantApplied: false,
		},
		{
			name:        "an exported empty value refuses too",
			env:         envbooltest.Set(atlasfilter.AllowUnmatchedExcludeEnvVar, ""),
			wantErr:     `invalid boolean value "" for PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE`,
			wantApplied: false,
		},
		{
			name:        "unset applies, which is the control the refusal has to leave alone",
			env:         envbooltest.Unset(atlasfilter.AllowUnmatchedExcludeEnvVar),
			wantApplied: true,
		},
		{
			name:        "a valid false applies, because no selector went unmatched",
			env:         envbooltest.Set(atlasfilter.AllowUnmatchedExcludeEnvVar, "false"),
			wantApplied: true,
		},
		{
			name:        "a valid true applies",
			env:         envbooltest.Set(atlasfilter.AllowUnmatchedExcludeEnvVar, "1"),
			wantApplied: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			allowSchemaApplyWithoutDevURL(t)
			test.env(t)
			dir := t.TempDir()
			targetPath := filepath.Join(dir, "target.db")
			seedSQLiteDBAt(t, targetPath, "CREATE TABLE users (id INTEGER PRIMARY KEY); CREATE TABLE legacy (id INTEGER PRIMARY KEY)")
			schemaPath := writeUnmatchedExcludeSchemaFile(c, dir)

			out, err := runCompatCommand(t,
				"schema", "apply",
				"--auto-approve",
				"--url", "sqlite://"+targetPath,
				"--to", "file://"+schemaPath,
				"--exclude", "legacy",
			)

			c.Assert(errMessageOrEmpty(err), qt.Equals, test.wantErr, qt.Commentf("%s", out))
			c.Assert(sqliteHasUsersEmailColumn(c, targetPath), qt.Equals, test.wantApplied,
				qt.Commentf("command output:\n%s", out))
		})
	}
}

// writeUnmatchedExcludeSchemaFile writes a desired state that adds one column to
// `users` and leaves `legacy` as it is, so an applied run is visible as exactly
// one catalog change.
func writeUnmatchedExcludeSchemaFile(c *qt.C, dir string) string {
	c.Helper()
	path := filepath.Join(dir, "schema.sql")
	body := "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n" +
		"CREATE TABLE legacy (id INTEGER PRIMARY KEY);\n"
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// sqliteHasUsersEmailColumn reads the target back through the real reader, so
// the assertion is about the database rather than about what was printed.
func sqliteHasUsersEmailColumn(c *qt.C, path string) bool {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var count int
	row := conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = 'email'")
	c.Assert(row.Scan(&count), qt.IsNil)
	return count == 1
}

// errMessageOrEmpty renders an error for comparison against a table row without
// a branch in the test body.
func errMessageOrEmpty(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
