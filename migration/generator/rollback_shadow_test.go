package generator_test

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/migration/generator"
)

func writeRollbackShadowMigrations(c *qt.C, dir string, downSQL string) {
	c.Helper()
	write := func(name, body string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	write("0000000001_init.up.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	write("0000000001_init.down.sql", "DROP TABLE users;\n")
	write("0000000002_add_email.up.sql", "ALTER TABLE users ADD COLUMN email TEXT;\n")
	write("0000000002_add_email.down.sql", downSQL)
}

func openRollbackTarget(c *qt.C, rawURL string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), rawURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
	})
	return conn
}

func TestVerifyRollbackFromShadow_HappyPathReplaysUpThenDown(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeRollbackShadowMigrations(c, dir, "ALTER TABLE users DROP COLUMN email;\n")
	targetConn := openRollbackTarget(
		c,
		"sqlite://"+filepath.Join(t.TempDir(), "target.db"),
	)

	err := generator.VerifyRollbackFromShadow(context.Background(), generator.RollbackFromShadowOptions{
		TargetConnection:  targetConn,
		ShadowDatabaseURL: "sqlite://" + filepath.Join(t.TempDir(), "shadow.db"),
		FS:                os.DirFS(dir),
		CurrentVersion:    2,
		TargetVersion:     1,
	})

	c.Assert(err, qt.IsNil)
}

func TestVerifyRollbackFromShadow_FailurePathBrokenDownMigrationReported(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeRollbackShadowMigrations(c, dir, "ALTER TABLE users DROP COLUMN no_such_column;\n")
	targetConn := openRollbackTarget(
		c,
		"sqlite://"+filepath.Join(t.TempDir(), "target.db"),
	)

	err := generator.VerifyRollbackFromShadow(context.Background(), generator.RollbackFromShadowOptions{
		TargetConnection:  targetConn,
		ShadowDatabaseURL: "sqlite://" + filepath.Join(t.TempDir(), "shadow.db"),
		FS:                os.DirFS(dir),
		CurrentVersion:    2,
		TargetVersion:     1,
	})

	// The broken down file fails on the shadow replay, so the caller aborts
	// before its target database is touched.
	c.Assert(err, qt.ErrorMatches, `(?s)rollback verification failed: roll back to version 1 on shadow database: failed to revert migration 2: .*no_such_column.*`)
}

func TestVerifyRollbackFromShadow_FailurePathValidatesInputs(t *testing.T) {
	c := qt.New(t)
	targetConn := openRollbackTarget(c, "sqlite://"+filepath.Join(t.TempDir(), "target.db"))

	err := generator.VerifyRollbackFromShadow(context.Background(), generator.RollbackFromShadowOptions{
		TargetConnection: targetConn,
		FS:               os.DirFS(t.TempDir()),
	})
	c.Assert(err, qt.ErrorMatches, `rollback verification failed: a shadow database URL is required`)

	err = generator.VerifyRollbackFromShadow(context.Background(), generator.RollbackFromShadowOptions{
		ShadowDatabaseURL: "sqlite://shadow.db",
		FS:                os.DirFS(t.TempDir()),
	})
	c.Assert(err, qt.ErrorMatches, `rollback verification failed: a target database connection is required`)

	err = generator.VerifyRollbackFromShadow(context.Background(), generator.RollbackFromShadowOptions{
		TargetConnection:  targetConn,
		ShadowDatabaseURL: "sqlite://ignored.db",
	})
	c.Assert(err, qt.ErrorMatches, `rollback verification failed: a migration filesystem is required`)
}

func TestVerifyRollbackFromShadow_FailurePathRejectsTargetAliasBeforeReset(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "target.db")
	targetURL := "sqlite://" + databasePath
	shadowAliasURL := "sqlite://" + filepath.Join(dir, ".", "target.db") + "?mode=rwc"

	targetConn, err := dbschema.ConnectToDatabase(ctx, targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(targetConn)
	_, err = targetConn.Exec("CREATE TABLE protected_target (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	err = generator.VerifyRollbackFromShadow(ctx, generator.RollbackFromShadowOptions{
		TargetConnection:  targetConn,
		ShadowDatabaseURL: shadowAliasURL,
		FS:                os.DirFS(t.TempDir()),
	})

	c.Check(err, qt.ErrorMatches, `rollback verification failed: shadow database must be distinct from target database`)
	var protectedTableCount int
	err = targetConn.QueryRow(
		"SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'protected_target'",
	).Scan(&protectedTableCount)
	c.Assert(err, qt.IsNil)
	c.Assert(protectedTableCount, qt.Equals, 1)
}

func TestVerifyRollbackFromShadow_FailurePathRejectsLiveDialectMismatch(t *testing.T) {
	c := qt.New(t)
	targetConn := openRollbackTarget(c, requireRollbackPostgresURL(c))

	err := generator.VerifyRollbackFromShadow(c.Context(), generator.RollbackFromShadowOptions{
		TargetConnection:  targetConn,
		ShadowDatabaseURL: "sqlite://" + filepath.Join(t.TempDir(), "shadow.db"),
		FS:                os.DirFS(t.TempDir()),
	})

	c.Assert(
		err,
		qt.ErrorMatches,
		`rollback verification failed: shadow database dialect "sqlite" does not match target dialect "postgres"`,
	)
}

func TestVerifyRollbackFromShadow_FailurePathRejectsDriverOverrideAliasLive(t *testing.T) {
	c := qt.New(t)
	targetURL := rollbackPostgresDatabaseURL(
		c,
		requireRollbackPostgresURL(c),
		"postgres",
	)
	shadowURL := rollbackPostgresDriverOverrideURL(c, targetURL)
	targetConn := openRollbackTarget(c, targetURL)
	fastPathSame, err := atlasurl.SameDatabase(targetURL, shadowURL)
	c.Assert(err, qt.IsNil)
	c.Assert(fastPathSame, qt.IsFalse)

	err = generator.VerifyRollbackFromShadow(c.Context(), generator.RollbackFromShadowOptions{
		TargetConnection:  targetConn,
		ShadowDatabaseURL: shadowURL,
		FS:                os.DirFS(t.TempDir()),
	})

	c.Assert(
		err,
		qt.ErrorMatches,
		`rollback verification failed: shadow database must be distinct from target database`,
	)
}

func requireRollbackPostgresURL(c *qt.C) string {
	c.Helper()
	rawURL := os.Getenv("POSTGRES_TEST_DSN")
	if rawURL == "" {
		c.Skip("POSTGRES_TEST_DSN is not set")
	}
	return rawURL
}

func rollbackPostgresDatabaseURL(c *qt.C, rawURL, database string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	if parsed.Scheme == "" {
		c.Skip("POSTGRES_TEST_DSN is not a URL")
	}
	parsed.Path = "/" + database
	query := parsed.Query()
	query.Set("database", database)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func rollbackPostgresDriverOverrideURL(c *qt.C, rawURL string) string {
	c.Helper()
	config, err := pgx.ParseConfig(rawURL)
	c.Assert(err, qt.IsNil)
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	parsed.Host = "guard.invalid:1"
	parsed.Path = "/ignored"
	query := parsed.Query()
	query.Set("host", config.Host)
	query.Set("port", strconv.Itoa(int(config.Port)))
	query.Set("database", config.Database)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
