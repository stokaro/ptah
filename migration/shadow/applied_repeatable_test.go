package shadow_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

// repeatableFixture is a history whose versioned migration creates a table
// and whose repeatable migration creates a view over it, so the view is what
// a replay of the versioned history alone does not create.
var repeatableFixture = fstest.MapFS{
	"0000000001_users.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);")},
	"0000000001_users.down.sql": {Data: []byte("DROP TABLE users;")},
	"R__user_emails.sql":        {Data: []byte("DROP VIEW IF EXISTS user_emails;\nCREATE VIEW user_emails AS SELECT email FROM users;\n")},
}

// repeatableTarget migrates a new SQLite target with repeatableFixture and
// returns it with the history it recorded. Each statement in outOfBand then
// runs on the target outside any migration.
func repeatableTarget(c *qt.C, outOfBand ...string) (*dbschema.DatabaseConnection, migrator.AppliedHistory) {
	c.Helper()
	target, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "target.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })
	mig, err := migrator.NewFSMigrator(target, repeatableFixture, migrator.WithRepeatableMigrations())
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	history, err := mig.AppliedHistory(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(history.Repeatables, qt.HasLen, 1)
	for _, statement := range outOfBand {
		_, err := target.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	return target, history
}

func withRepeatables(history migrator.AppliedHistory) func(*shadow.AppliedVerifyOptions) {
	return func(opts *shadow.AppliedVerifyOptions) { opts.Repeatables = history.Repeatables }
}

func TestVerifyAppliedRepeatables_HappyPath(t *testing.T) {
	c := qt.New(t)
	target, history := repeatableTarget(c)
	shadowDatabase := newShadow(c)

	result, err := shadow.VerifyApplied(t.Context(), appliedOptions(target, history, shadowDatabase, withRepeatables(history)))

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, shadow.AppliedResult{})
	c.Assert(sqliteTables(c, shadowDatabase), qt.HasLen, 0)
}

func TestVerifyAppliedRepeatables_FailurePath(t *testing.T) {
	t.Run("a view an applied repeatable migration created and the target lost", func(t *testing.T) {
		c := qt.New(t)
		target, history := repeatableTarget(c, "DROP VIEW user_emails")
		shadowDatabase := newShadow(c)

		result, err := shadow.VerifyApplied(t.Context(), appliedOptions(target, history, shadowDatabase, withRepeatables(history)))

		c.Assert(err, qt.IsNil)
		c.Assert(result, qt.DeepEquals, shadow.AppliedResult{Drift: []shadow.Mismatch{
			{Kind: "missing_view", Object: "user_emails", Message: "missing view user_emails"},
		}})
		c.Assert(sqliteTables(c, shadowDatabase), qt.HasLen, 0)
	})

	// The control: the comparison sees the view, so replaying the repeatable
	// migration is what makes the clean target clean.
	t.Run("a replay that leaves the repeatable migrations out", func(t *testing.T) {
		c := qt.New(t)
		target, history := repeatableTarget(c)
		shadowDatabase := newShadow(c)

		result, err := shadow.VerifyApplied(t.Context(), appliedOptions(target, history, shadowDatabase, nil))

		c.Assert(err, qt.IsNil)
		c.Assert(result, qt.DeepEquals, shadow.AppliedResult{Drift: []shadow.Mismatch{
			{Kind: "extra_view", Object: "user_emails", Message: "extra view user_emails"},
		}})
		c.Assert(sqliteTables(c, shadowDatabase), qt.HasLen, 0)
	})
}
