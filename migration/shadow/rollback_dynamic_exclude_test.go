package shadow_test

import (
	"cmp"
	"path/filepath"
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
	c.Assert(statements, qt.IsNil)
}
