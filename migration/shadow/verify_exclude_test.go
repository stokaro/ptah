package shadow_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/core/yamlschema"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/shadow"
)

// The exclusion fixture replays a history that created `widgets` and
// `legacy_log`, then a candidate adding a column to `widgets`. The declaration
// holds `widgets` with that column and a `reporting_cache` nothing creates. So
// the shadow database holds a table the declaration lacks, and the
// declaration holds one the shadow database lacks: an exclusion has to reach
// both sides of the comparison for the verification to pass.

var excludeHistory = fstest.MapFS{
	"0000000001_init.up.sql":   {Data: []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE legacy_log (id INTEGER PRIMARY KEY);\n")},
	"0000000001_init.down.sql": {Data: []byte("DROP TABLE legacy_log;\nDROP TABLE widgets;\n")},
}

var excludeCandidate = shadow.Candidate{
	Version: 2,
	Name:    "add_name",
	UpSQL:   "ALTER TABLE widgets ADD COLUMN name TEXT;",
	DownSQL: "ALTER TABLE widgets DROP COLUMN name;",
}

func excludeDeclaration(c *qt.C) *schemamodel.Database {
	c.Helper()
	return must.Must(yamlschema.Parse(c.Context(), builtintest.Runtime().YAML(), []byte(`
tables:
  widgets:
    columns:
      id:
        type: INTEGER
        primary: true
      name:
        type: TEXT
  reporting_cache:
    columns:
      id:
        type: INTEGER
        primary: true
`)))
}

// excludeVerifyOptions are the options of one verification over the fixture,
// against a target and a shadow database of the test's own.
func excludeVerifyOptions(c *qt.C, runtime schemadiff.DatabaseRuntime, exclude []string) shadow.MigrationVerifyOptions {
	c.Helper()
	dir := c.TempDir()
	target, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.ToSlash(filepath.Join(dir, "target.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })
	return shadow.MigrationVerifyOptions{
		Runtime:           runtime,
		ShadowDatabaseURL: "sqlite://" + filepath.ToSlash(filepath.Join(dir, "shadow.db")),
		TargetConnection:  target,
		MigrationsFS:      excludeHistory,
		Dialect:           "sqlite",
		Candidates:        []shadow.Candidate{excludeCandidate},
		Generated:         excludeDeclaration(c),
		Exclude:           exclude,
	}
}

func TestVerifyMigration_Exclude_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
	}{
		{name: "both tables", exclude: []string{"legacy_log", "reporting_cache"}},
		{name: "both tables in one value", exclude: []string{"legacy_log,reporting_cache"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := shadow.VerifyMigration(t.Context(), excludeVerifyOptions(c, must.Must(builtin.New()), test.exclude))

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestVerifyMigration_Exclude_FailurePath excludes one side's table at a time.
// Each row fails on the table it leaves in, which is what proves the
// exclusion reaches the read-back and the declaration separately; the
// unexcluded row reports both.
func TestVerifyMigration_Exclude_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
		want    []shadow.Mismatch
	}{
		{
			name: "nothing excluded",
			want: []shadow.Mismatch{
				{Kind: "missing_table", Object: "reporting_cache", Table: "reporting_cache", Message: "missing table reporting_cache"},
				{Kind: "extra_table", Object: "legacy_log", Table: "legacy_log", Message: "extra table legacy_log"},
				extraLegacyKey,
			},
		},
		{
			name:    "only the declared table",
			exclude: []string{"reporting_cache"},
			want: []shadow.Mismatch{
				{Kind: "extra_table", Object: "legacy_log", Table: "legacy_log", Message: "extra table legacy_log"},
				extraLegacyKey,
			},
		},
		{
			name:    "only the replayed table",
			exclude: []string{"legacy_log"},
			want: []shadow.Mismatch{
				{Kind: "missing_table", Object: "reporting_cache", Table: "reporting_cache", Message: "missing table reporting_cache"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := shadow.VerifyMigration(t.Context(), excludeVerifyOptions(c, must.Must(builtin.New()), test.exclude))

			var verificationErr *shadow.VerificationError
			c.Assert(err, qt.ErrorAs, &verificationErr)
			c.Assert(verificationErr.Result.Stage, qt.Equals, "schema-match")
			c.Assert(verificationErr.Result.Mismatches, qt.DeepEquals, test.want)
		})
	}
}

// extraLegacyKey is the primary key the replayed `legacy_log` carries, which
// the comparison reports beside the table.
var extraLegacyKey = shadow.Mismatch{
	Kind: "extra_constraint", Object: "legacy_log.legacy_log_pkey", Table: "legacy_log",
	Constraint: "legacy_log_pkey", Message: "extra constraint legacy_log.legacy_log_pkey",
}

// runtimeWithoutSelections hides the scope selections of the runtime it
// embeds, as a runtime that does not implement selectext.Runtime would.
type runtimeWithoutSelections struct {
	schemadiff.DatabaseRuntime
}

// TestVerifyMigration_ExcludeNeedsScopeSelections_FailurePath refuses at the
// configuration stage, before the shadow database is connected.
func TestVerifyMigration_ExcludeNeedsScopeSelections_FailurePath(t *testing.T) {
	c := qt.New(t)
	opts := excludeVerifyOptions(c, runtimeWithoutSelections{must.Must(builtin.New())}, []string{"legacy_log"})

	err := shadow.VerifyMigration(t.Context(), opts)

	c.Assert(err, qt.ErrorIs, selectext.ErrUnselected)
	var verificationErr *shadow.VerificationError
	c.Assert(err, qt.ErrorAs, &verificationErr)
	c.Assert(verificationErr.Result.Stage, qt.Equals, "configuration")
	c.Assert(verificationErr.Result.Mismatches, qt.HasLen, 1)
	c.Assert(verificationErr.Result.Mismatches[0].Kind, qt.Equals, "exclude_error")
	_, statErr := os.Stat(filepath.FromSlash(strings.TrimPrefix(opts.ShadowDatabaseURL, "sqlite://")))
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}
