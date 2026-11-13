package shadow_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/revisiontable"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

// appliedFixture is a history of three migrations. A target applies some of
// them, and the third stays pending in the rows that leave it out.
var appliedFixture = fstest.MapFS{
	"0000000001_users.up.sql":    {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);")},
	"0000000001_users.down.sql":  {Data: []byte("DROP TABLE users;")},
	"0000000002_orders.up.sql":   {Data: []byte("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL);")},
	"0000000002_orders.down.sql": {Data: []byte("DROP TABLE orders;")},
	"0000000003_notes.up.sql":    {Data: []byte("CREATE TABLE notes (id INTEGER PRIMARY KEY);")},
	"0000000003_notes.down.sql":  {Data: []byte("DROP TABLE notes;")},
}

// appliedTarget migrates a new SQLite target to version and returns it, its
// URL and the history it recorded. Each statement in outOfBand then runs on
// the target outside any migration.
func appliedTarget(c *qt.C, version int64, outOfBand ...string) (*dbschema.DatabaseConnection, string, migrator.AppliedHistory) {
	c.Helper()
	url := "sqlite://" + filepath.Join(c.TempDir(), "target.db")
	target, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })
	mig, err := migrator.NewFSMigrator(target, appliedFixture)
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateTo(c.Context(), version), qt.IsNil)
	history, err := mig.AppliedHistory(c.Context())
	c.Assert(err, qt.IsNil)
	for _, statement := range outOfBand {
		_, err := target.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	return target, url, history
}

// newShadow is the URL of a new SQLite shadow database, created holding each
// statement in contents.
func newShadow(c *qt.C, contents ...string) string {
	c.Helper()
	url := "sqlite://" + filepath.Join(c.TempDir(), "shadow.db")
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range contents {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	return url
}

// sqliteTables lists every table the database at url holds, the bookkeeping
// tables included.
func sqliteTables(c *qt.C, url string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// appliedOptions are the options a check of target against history takes,
// changed by edit when it is set.
func appliedOptions(target *dbschema.DatabaseConnection, history migrator.AppliedHistory, shadowDatabase string,
	edit func(*shadow.AppliedVerifyOptions),
) shadow.AppliedVerifyOptions {
	opts := shadow.AppliedVerifyOptions{
		Runtime:           must.Must(builtin.New()),
		ShadowDatabaseURL: shadowDatabase,
		TargetConn:        target,
		Migrations:        history.Migrations,
		Dialect:           "sqlite",
		Capabilities:      target.Info().Capabilities,
	}
	if edit != nil {
		edit(&opts)
	}
	return opts
}

// withoutTable is a scope that leaves the named table out of the target side.
func withoutTable(name string) func(*shadow.AppliedVerifyOptions) {
	return func(opts *shadow.AppliedVerifyOptions) {
		opts.Scope = func(_ context.Context, target *catalog.Database, replayed *schemamodel.Database) (*catalog.Database, *schemamodel.Database, error) {
			return revisiontable.Without(target, []string{name}), replayed, nil
		}
	}
}

func TestVerifyAppliedHappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		version int64
		// outOfBand runs on the target after it migrated.
		outOfBand []string
		edit      func(*shadow.AppliedVerifyOptions)
	}{
		{name: "the whole history applied", version: 3},
		{
			// The pending migration is no part of what the target should hold.
			name: "a pending migration left out", version: 2,
		},
		{
			name: "a bookkeeping table named as metadata", version: 2,
			outOfBand: []string{"CREATE TABLE custom_revisions (version INTEGER PRIMARY KEY)"},
			edit:      func(opts *shadow.AppliedVerifyOptions) { opts.MetadataTables = []string{"custom_revisions"} },
		},
		{
			name: "a difference the scope leaves out", version: 2,
			outOfBand: []string{"CREATE TABLE scratch (id INTEGER PRIMARY KEY)"},
			edit:      withoutTable("scratch"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _, history := appliedTarget(c, test.version, test.outOfBand...)
			shadowDatabase := newShadow(c)

			result, err := shadow.VerifyApplied(t.Context(), appliedOptions(target, history, shadowDatabase, test.edit))

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, shadow.AppliedResult{})
			c.Assert(result.Clean(), qt.IsTrue)
			c.Assert(sqliteTables(c, shadowDatabase), qt.HasLen, 0)
		})
	}
}

func TestVerifyAppliedFailurePathReportsDrift(t *testing.T) {
	for _, test := range []struct {
		name      string
		outOfBand []string
		want      []shadow.Mismatch
	}{
		{
			name:      "a column no migration added",
			outOfBand: []string{"ALTER TABLE users ADD COLUMN nickname TEXT"},
			want: []shadow.Mismatch{{
				Kind: "extra_column", Object: "users.nickname", Table: "users", Column: "nickname",
				Message: "extra column users.nickname",
			}},
		},
		{
			name:      "a table an applied migration created and the target lost",
			outOfBand: []string{"DROP TABLE orders"},
			want:      []shadow.Mismatch{{Kind: "missing_table", Object: "orders", Table: "orders", Message: "missing table orders"}},
		},
		{
			// A bookkeeping table the caller did not name is an ordinary table.
			name:      "a bookkeeping table not named as metadata",
			outOfBand: []string{"CREATE TABLE custom_revisions (version INTEGER PRIMARY KEY)"},
			want: []shadow.Mismatch{
				{Kind: "extra_table", Object: "custom_revisions", Table: "custom_revisions", Message: "extra table custom_revisions"},
				{
					Kind: "extra_constraint", Object: "custom_revisions.custom_revisions_pkey", Table: "custom_revisions",
					Constraint: "custom_revisions_pkey", Message: "extra constraint custom_revisions.custom_revisions_pkey",
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _, history := appliedTarget(c, 2, test.outOfBand...)
			shadowDatabase := newShadow(c)

			result, err := shadow.VerifyApplied(t.Context(), appliedOptions(target, history, shadowDatabase, nil))

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, shadow.AppliedResult{Drift: test.want})
			c.Assert(result.Clean(), qt.IsFalse)
			c.Assert(sqliteTables(c, shadowDatabase), qt.HasLen, 0)
		})
	}
}

func TestVerifyAppliedFailurePathCannotCompare(t *testing.T) {
	broken, err := migrator.NewFSMigrationProvider(fstest.MapFS{
		"0000000001_users.up.sql":    {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);")},
		"0000000001_users.down.sql":  {Data: []byte("DROP TABLE users;")},
		"0000000002_broken.up.sql":   {Data: []byte("CREATE TABLE orders (id INTEGER PRIMARY KEY REFERENCES;")},
		"0000000002_broken.down.sql": {Data: []byte("DROP TABLE orders;")},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		// shadow is the shadow database's URL, given the target's.
		shadow func(c *qt.C, targetURL string) string
		edit   func(*shadow.AppliedVerifyOptions)
		stage  string
		kind   string
		// left is what the shadow database holds afterwards.
		left []string
	}{
		{
			name:   "the target given as the shadow database",
			shadow: func(_ *qt.C, targetURL string) string { return targetURL },
			stage:  "realm-check", kind: "target_shadow_same_realm",
			left: []string{"orders", "schema_migrations", "schema_migrations_log", "users"},
		},
		{
			name:   "a shadow database that holds a table",
			shadow: func(c *qt.C, _ string) string { return newShadow(c, "CREATE TABLE keep_me (id INTEGER PRIMARY KEY)") },
			stage:  "claim", kind: "not_clean",
			left: []string{"keep_me"},
		},
		{
			name:   "a history whose replay fails",
			shadow: func(c *qt.C, _ string) string { return newShadow(c) },
			edit:   func(opts *shadow.AppliedVerifyOptions) { opts.Migrations = broken.Migrations() },
			stage:  "replay", kind: "replay_error",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, targetURL, history := appliedTarget(c, 2)
			shadowDatabase := test.shadow(c, targetURL)

			result, err := shadow.VerifyApplied(t.Context(), appliedOptions(target, history, shadowDatabase, test.edit))

			c.Assert(result, qt.DeepEquals, shadow.AppliedResult{})
			var verification *shadow.VerificationError
			c.Assert(err, qt.ErrorAs, &verification)
			c.Assert(err, qt.ErrorMatches, `(?s)applied history shadow check failed: .*`)
			c.Assert(verification.Result.Stage, qt.Equals, test.stage)
			c.Assert(verification.Result.Mismatches[0].Kind, qt.Equals, test.kind)
			c.Assert(sqliteTables(c, shadowDatabase), qt.DeepEquals, test.left)
		})
	}
}

// A target bootstrapped from an Atlas checkpoint replays from that checkpoint,
// and the files before it, which never ran there, are not compared.
func TestVerifyAppliedHappyPathReplaysFromABootstrapCheckpoint(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"20250801000001_users.sql":      {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n")},
		"20250801000002_email.sql":      {Data: []byte("ALTER TABLE users ADD COLUMN email TEXT NULL;\n")},
		"20260801100335_checkpoint.sql": {Data: []byte("-- atlas:checkpoint\n\nCREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NULL);\n")},
		"20260801100400_posts.sql":      {Data: []byte("CREATE TABLE posts (id INTEGER PRIMARY KEY);\n")},
	}
	target, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "target.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })
	mig, err := migrator.NewFSMigrator(target, fsys, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	history, err := mig.AppliedHistory(c.Context())
	c.Assert(err, qt.IsNil)
	shadowDatabase := newShadow(c)

	result, err := shadow.VerifyApplied(t.Context(), appliedOptions(target, history, shadowDatabase,
		func(opts *shadow.AppliedVerifyOptions) { opts.MetadataTables = revisiontable.Configured("atlas", "") }))

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, shadow.AppliedResult{})
	c.Assert(history.Versions(), qt.DeepEquals, []int64{20260801100335, 20260801100400})
	c.Assert(sqliteTables(c, shadowDatabase), qt.HasLen, 0)
}
