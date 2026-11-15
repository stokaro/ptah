package shadow_test

import (
	"cmp"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

// dynamicRollbackHistory creates users at version 1 and posts at version 2,
// in the Atlas format, which carries no down bodies.
var dynamicRollbackHistory = fstest.MapFS{
	"1_users.sql": {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
	"2_posts.sql": {Data: []byte("CREATE TABLE posts (id INTEGER PRIMARY KEY);")},
}

// dynamicRollbackOptions plan a rollback to version 1 of a live database at
// version 2 that also holds external_audit, a table no migration creates. An
// empty devURL selects a dev database file beside the live one.
func dynamicRollbackOptions(c *qt.C, devURL string, exclude []string) shadow.DynamicRollbackOptions {
	c.Helper()
	dir := c.TempDir()
	live, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(dir, "live.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(live) })
	_, err = live.ExecContext(c.Context(),
		"CREATE TABLE users (id INTEGER PRIMARY KEY); CREATE TABLE posts (id INTEGER PRIMARY KEY); "+
			"CREATE TABLE external_audit (id INTEGER PRIMARY KEY);")
	c.Assert(err, qt.IsNil)
	return shadow.DynamicRollbackOptions{
		Runtime:          builtintest.Runtime(),
		TargetConnection: live,
		DevDatabaseURL:   cmp.Or(devURL, atlasurl.SQLiteURLFromPath(filepath.Join(dir, "dev.db"))),
		FS:               dynamicRollbackHistory,
		TargetVersion:    1,
		ProviderOptions:  []migrator.FSProviderOption{migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas)},
		Exclude:          exclude,
	}
}

func TestPlanDynamicRollbackExclude_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
		want    []string
	}{
		{
			name: "nothing excluded drops the table no migration creates",
			want: []string{`DROP TABLE "external_audit"`, `DROP TABLE "posts"`},
		},
		{
			name:    "an excluded table is left alone",
			exclude: []string{"external_audit"},
			want:    []string{`DROP TABLE "posts"`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := shadow.PlanDynamicRollback(c.Context(), dynamicRollbackOptions(c, "", test.exclude))

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.ContentEquals, test.want)
		})
	}
}

// TestPlanDynamicRollbackExclude_FailurePath refuses a malformed selector
// before the dev database is opened: the dev URL names a file in a directory
// that does not exist, so a run that reached it would fail to connect instead.
func TestPlanDynamicRollbackExclude_FailurePath(t *testing.T) {
	c := qt.New(t)
	devURL := atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "missing", "dev.db"))

	statements, err := shadow.PlanDynamicRollback(c.Context(), dynamicRollbackOptions(c, devURL, []string{"us[ers"}))

	c.Assert(err, qt.ErrorMatches,
		`dynamic rollback planning failed: invalid exclude: invalid Atlas exclude glob "us\[ers": syntax error in pattern`)
	c.Assert(err, qt.Not(qt.ErrorIs), shadow.ErrUnmatchedExclude)
	c.Assert(statements, qt.IsNil)
}

// TestPlanDynamicRollbackUnmatchedExclude_HappyPath plans with selectors that
// name no object. Refusing is the caller's choice: without it each such
// selector is a warning on Diagnostics, named by the source the caller
// recorded, and the plan drops what the selector was meant to keep. The
// planner's own selectors for the revision tables match nothing in this
// database, so the refusing rows show it reports only the selectors the caller
// wrote.
func TestPlanDynamicRollbackUnmatchedExclude_HappyPath(t *testing.T) {
	tests := []struct {
		name            string
		exclude         []string
		sources         map[string]string
		refuse          bool
		want            []string
		wantDiagnostics string
	}{
		{
			name:            "an unmatched selector is a warning",
			exclude:         []string{"external_audt"},
			want:            []string{`DROP TABLE "external_audit"`, `DROP TABLE "posts"`},
			wantDiagnostics: `Warning: the --exclude selection matched no objects: "external_audt".` + "\n",
		},
		{
			name:    "the warning names the source of the selector",
			exclude: []string{"external_audit", "external_audt"},
			sources: map[string]string{"external_audit": "ptah.yaml", "external_audt": "ptah.yaml"},
			want:    []string{`DROP TABLE "posts"`},
			wantDiagnostics: `Warning: the exclude selection from ptah.yaml matched no objects: "external_audt".` +
				"\n",
		},
		{
			name:    "a refusing plan with a matching selector",
			exclude: []string{"external_audit"},
			refuse:  true,
			want:    []string{`DROP TABLE "posts"`},
		},
		{
			name:   "a refusing plan with no selector",
			refuse: true,
			want:   []string{`DROP TABLE "external_audit"`, `DROP TABLE "posts"`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var diagnostics strings.Builder
			opts := dynamicRollbackOptions(c, "", test.exclude)
			opts.ExcludeSources = test.sources
			opts.RefuseUnmatchedExclude = test.refuse
			opts.Diagnostics = &diagnostics

			statements, err := shadow.PlanDynamicRollback(c.Context(), opts)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.ContentEquals, test.want)
			c.Assert(diagnostics.String(), qt.Equals, test.wantDiagnostics)
		})
	}
}

// TestPlanDynamicRollbackUnmatchedExclude_FailurePath refuses a selector that
// names no object in the live database or the replayed target state. It
// protects nothing, so the plan would drop the table it was written for: no
// statement is returned and nothing is written to Diagnostics. A selector
// the caller wrote is refused even when it spells a revision table the
// planner leaves out on its own.
func TestPlanDynamicRollbackUnmatchedExclude_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
		sources map[string]string
		wantErr string
	}{
		{
			name:    "a mistyped selector",
			exclude: []string{"external_audt"},
			wantErr: `dynamic rollback planning failed: compare the schemas: ` +
				`the --exclude selection matched no objects: "external_audt"`,
		},
		{
			name:    "a mistyped selector beside a matching one, named by its source",
			exclude: []string{"external_audit", "external_audt"},
			sources: map[string]string{"external_audit": "atlas.hcl", "external_audt": "atlas.hcl"},
			wantErr: `dynamic rollback planning failed: compare the schemas: ` +
				`the exclude selection from atlas.hcl matched no objects: "external_audt"`,
		},
		{
			name:    "a revision table the database does not hold",
			exclude: []string{"schema_migrations"},
			wantErr: `dynamic rollback planning failed: compare the schemas: ` +
				`the --exclude selection matched no objects: "schema_migrations"`,
		},
		{
			name:    "a revision table in a comma-separated value",
			exclude: []string{"external_audit, schema_migrations"},
			wantErr: `dynamic rollback planning failed: compare the schemas: ` +
				`the --exclude selection matched no objects: "schema_migrations"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var diagnostics strings.Builder
			opts := dynamicRollbackOptions(c, "", test.exclude)
			opts.ExcludeSources = test.sources
			opts.RefuseUnmatchedExclude = true
			opts.Diagnostics = &diagnostics

			statements, err := shadow.PlanDynamicRollback(c.Context(), opts)

			c.Assert(err, qt.ErrorIs, shadow.ErrUnmatchedExclude)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(statements, qt.IsNil)
			c.Assert(diagnostics.String(), qt.Equals, "")
		})
	}
}
