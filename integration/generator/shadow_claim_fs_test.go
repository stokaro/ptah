//go:build integration

package generator_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
	"ptah.run/dbschema"
	"ptah.run/migration/generator"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

// Every verification that replays into a shadow or dev database resets it
// first. A database holding a table is refused before that reset, and the
// verification hands the database back empty on its way out, so the same URL
// is accepted by the next run (stokaro/ptah#3811). Without the refusal, the
// reset empties a database the operator meant to keep; without the hand-back,
// the refusal would turn away the database the previous run filled.

// shadowClaimHistory is a two-version history in the Ptah directory format.
var shadowClaimHistory = fstest.MapFS{
	"0000000001_users.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
	"0000000001_users.down.sql": {Data: []byte("DROP TABLE users;")},
	"0000000002_posts.up.sql":   {Data: []byte("CREATE TABLE posts (id INTEGER PRIMARY KEY);")},
	"0000000002_posts.down.sql": {Data: []byte("DROP TABLE posts;")},
}

// shadowClaimTarget opens a SQLite target in dir holding the history's tables.
func shadowClaimTarget(ctx context.Context, dir string) (*dbschema.DatabaseConnection, error) {
	target, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(dir, "target.db"))
	if err != nil {
		return nil, err
	}
	for _, statement := range []string{
		"CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY)",
		"CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY)",
	} {
		if _, err := target.ExecContext(ctx, statement); err != nil {
			dbschema.CloseAndWarn(target)
			return nil, err
		}
	}
	return target, nil
}

// verifyMigrationWithShadow is `migrations generate --shadow-db`: the history's
// first version plus the second as a candidate.
func verifyMigrationWithShadow(ctx context.Context, dir, shadowURL string) error {
	target, err := shadowClaimTarget(ctx, dir)
	if err != nil {
		return err
	}
	defer dbschema.CloseAndWarn(target)
	desired, err := yamlschema.Parse([]byte(`
tables:
  users:
    columns:
      id:
        type: INTEGER
        primary: true
  posts:
    columns:
      id:
        type: INTEGER
        primary: true
`))
	if err != nil {
		return err
	}
	return shadow.VerifyMigration(ctx, shadow.MigrationVerifyOptions{
		ShadowDatabaseURL: shadowURL,
		TargetConnection:  target,
		MigrationsFS: fstest.MapFS{
			"0000000001_users.up.sql":   shadowClaimHistory["0000000001_users.up.sql"],
			"0000000001_users.down.sql": shadowClaimHistory["0000000001_users.down.sql"],
		},
		Dialect: "sqlite",
		Candidates: []shadow.Candidate{{
			Version: 2,
			Name:    "posts",
			UpSQL:   "CREATE TABLE posts (id INTEGER PRIMARY KEY);",
			DownSQL: "DROP TABLE posts;",
		}},
		Generated: desired,
	})
}

// verifyBaselineWithShadow is `migrations baseline --shadow-db`.
func verifyBaselineWithShadow(ctx context.Context, dir, shadowURL string) error {
	target, err := shadowClaimTarget(ctx, dir)
	if err != nil {
		return err
	}
	defer dbschema.CloseAndWarn(target)
	return shadow.VerifyBaseline(ctx, shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: shadowURL,
		TargetConn:        target,
		MigrationsFS:      shadowClaimHistory,
		Version:           2,
		Dialect:           "sqlite",
	})
}

// verifyRollbackWithShadow is `migrations down --shadow-db`.
func verifyRollbackWithShadow(ctx context.Context, dir, shadowURL string) error {
	target, err := shadowClaimTarget(ctx, dir)
	if err != nil {
		return err
	}
	defer dbschema.CloseAndWarn(target)
	return shadow.VerifyRollback(ctx, shadow.RollbackVerifyOptions{
		TargetConnection:  target,
		ShadowDatabaseURL: shadowURL,
		FS:                shadowClaimHistory,
		CurrentVersion:    2,
		TargetVersion:     1,
	})
}

// planDynamicRollbackWithDev is `migrations down --plan --dev-url`.
func planDynamicRollbackWithDev(ctx context.Context, dir, devURL string) error {
	target, err := shadowClaimTarget(ctx, dir)
	if err != nil {
		return err
	}
	defer dbschema.CloseAndWarn(target)
	statements, err := shadow.PlanDynamicRollback(ctx, shadow.DynamicRollbackOptions{
		TargetConnection: target,
		DevDatabaseURL:   devURL,
		FS: fstest.MapFS{
			"1_users.sql": {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
			"2_posts.sql": {Data: []byte("CREATE TABLE posts (id INTEGER PRIMARY KEY);")},
		},
		TargetVersion: 1,
		ProviderOptions: []migrator.FSProviderOption{
			migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
		},
	})
	if err != nil {
		return err
	}
	if len(statements) == 0 {
		return fmt.Errorf("the plan is empty")
	}
	return nil
}

// generateCheckpointWithShadow is `migrations checkpoint --shadow-db`.
func generateCheckpointWithShadow(ctx context.Context, _, shadowURL string) error {
	_, _, err := generator.GenerateCheckpointFromShadow(ctx, generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: shadowURL,
		MigrationsDir:     "migrations",
		MigrationsFS:      shadowClaimHistory,
		Dialect:           "sqlite",
	})
	return err
}

// shadowClaimRuns are the verifications, each with the words its refusal
// starts with and the database it names.
var shadowClaimRuns = []struct {
	name    string
	run     func(ctx context.Context, dir, shadowURL string) error
	wantErr string
}{
	{
		name:    "migrations generate",
		run:     verifyMigrationWithShadow,
		wantErr: `shadow check failed: connected database is not clean: found table "keep_me"; Ptah resets this database before and after the replay, so point the URL at an empty database`,
	},
	{
		name:    "migrations baseline",
		run:     verifyBaselineWithShadow,
		wantErr: `baseline shadow check failed: connected database is not clean: found table "keep_me"; Ptah resets this database before and after the replay, so point the URL at an empty database`,
	},
	{
		name:    "migrations down",
		run:     verifyRollbackWithShadow,
		wantErr: `rollback verification failed: connected database is not clean: found table "keep_me"; Ptah resets this database before and after the replay, so point the URL at an empty database`,
	},
	{
		name:    "migrations down --plan",
		run:     planDynamicRollbackWithDev,
		wantErr: `dynamic rollback planning failed: connected database is not clean: found table "keep_me"; Ptah resets this database before and after the replay, so point the URL at an empty database`,
	},
	{
		name:    "migrations checkpoint",
		run:     generateCheckpointWithShadow,
		wantErr: `checkpoint generation failed: connected database is not clean: found table "keep_me"; Ptah resets this database before and after the replay, so point the URL at an empty database`,
	},
}

// sqliteTables lists the tables a SQLite database holds, in name order.
func sqliteTables(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(),
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\' ORDER BY name`)
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

// TestShadowVerificationsRefuseADatabaseHoldingATable_FailurePath runs each
// verification against a shadow database holding keep_me with a row. Each
// refuses before its reset, and the row is still there.
func TestShadowVerificationsRefuseADatabaseHoldingATable_FailurePath(t *testing.T) {
	for _, verification := range shadowClaimRuns {
		t.Run(verification.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			shadowURL := "sqlite://" + filepath.Join(dir, "shadow.db")
			shadowConn, err := dbschema.ConnectToDatabase(c.Context(), shadowURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(shadowConn) })
			_, err = shadowConn.ExecContext(c.Context(), "CREATE TABLE keep_me (id INTEGER PRIMARY KEY)")
			c.Assert(err, qt.IsNil)
			_, err = shadowConn.ExecContext(c.Context(), "INSERT INTO keep_me VALUES (1)")
			c.Assert(err, qt.IsNil)

			err = verification.run(c.Context(), dir, shadowURL)

			c.Assert(err, qt.ErrorMatches, verification.wantErr+`.*`)
			var rows int
			c.Assert(shadowConn.QueryRowContext(c.Context(), "SELECT count(*) FROM keep_me").Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 1)
		})
	}
}

// TestShadowVerificationsHandTheDatabaseBackEmpty_HappyPath runs each
// verification twice against one shadow database. Both runs pass, since the
// first leaves nothing the second refuses, and the database holds no table
// afterwards: not the replayed schema, and not the revision table.
func TestShadowVerificationsHandTheDatabaseBackEmpty_HappyPath(t *testing.T) {
	for _, verification := range shadowClaimRuns {
		t.Run(verification.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			shadowURL := "sqlite://" + filepath.Join(dir, "shadow.db")

			first := verification.run(c.Context(), dir, shadowURL)
			second := verification.run(c.Context(), dir, shadowURL)

			c.Assert(first, qt.IsNil)
			c.Assert(second, qt.IsNil)
			shadowConn, err := dbschema.ConnectToDatabase(c.Context(), shadowURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(shadowConn) })
			c.Assert(sqliteTables(c, shadowConn), qt.HasLen, 0)
		})
	}
}

// TestVerifyBaseline_FailedReplayHandsTheShadowBackEmpty_FailurePath fails the
// baseline replay at its second version, after the first one and the revision
// table are in the shadow database. The verification drops its own revision
// table only after a replay succeeds, so the hand-back is what removes it here.
func TestVerifyBaseline_FailedReplayHandsTheShadowBackEmpty_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	shadowURL := "sqlite://" + filepath.Join(dir, "shadow.db")
	target, err := shadowClaimTarget(c.Context(), dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })

	err = shadow.VerifyBaseline(c.Context(), shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: shadowURL,
		TargetConn:        target,
		MigrationsFS: fstest.MapFS{
			"0000000001_users.up.sql":   shadowClaimHistory["0000000001_users.up.sql"],
			"0000000001_users.down.sql": shadowClaimHistory["0000000001_users.down.sql"],
			"0000000002_posts.up.sql":   {Data: []byte("CREATE TABLE posts (id INTEGER PRIMARY KEY REFERENCES;")},
			"0000000002_posts.down.sql": shadowClaimHistory["0000000002_posts.down.sql"],
		},
		Version: 2,
		Dialect: "sqlite",
	})

	c.Assert(err, qt.ErrorMatches, `(?s)baseline shadow check failed: replay migrations: failed to apply migration 2: .*`)
	shadowConn, err := dbschema.ConnectToDatabase(c.Context(), shadowURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(shadowConn) })
	c.Assert(sqliteTables(c, shadowConn), qt.HasLen, 0)
}
