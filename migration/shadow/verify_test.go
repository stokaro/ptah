package shadow_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/shadow"
)

func TestVerifyBaseline_MissingTargetReturnsStructuredError(t *testing.T) {
	c := qt.New(t)

	err := shadow.VerifyBaseline(t.Context(), shadow.BaselineVerifyOptions{})

	c.Assert(err, qt.ErrorMatches, `baseline shadow check failed: target database connection is required`)
	var shadowErr *shadow.VerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result, qt.DeepEquals, shadow.VerificationResult{
		Stage: "realm-check",
		Mismatches: []shadow.Mismatch{{
			Kind:    "target_connection_required",
			Message: "target database connection is required",
		}},
	})
}

func TestVerifyBaselineRejectsMalformedSQLiteToggleBeforeMissingTarget(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	err := shadow.VerifyBaseline(t.Context(), shadow.BaselineVerifyOptions{
		Dialect: "sqlite",
	})

	c.Assert(err, qt.ErrorMatches,
		`baseline shadow check failed: validate SQLite virtual-table drop toggle: invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	var shadowErr *shadow.VerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "configuration")
}

func TestVerifyBaselineRejectsMalformedSQLiteVirtualDropToggleBeforeReplay(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	target, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://"+targetPath)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(target) })

	err = shadow.VerifyBaseline(t.Context(), shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(t.TempDir(), "missing", "shadow.db"),
		TargetConn:        target,
	})

	c.Assert(err, qt.ErrorMatches,
		`baseline shadow check failed: validate SQLite virtual-table drop toggle: invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "connect to shadow database")
}

func TestVerifyBaseline_ConnectFailureReturnsStructuredError(t *testing.T) {
	c := qt.New(t)
	targetURL := "sqlite://" + filepath.Join(c.TempDir(), "target.db")
	target, err := dbschema.ConnectToDatabase(t.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)

	err = shadow.VerifyBaseline(t.Context(), shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: "unsupported://shadow",
		TargetConn:        target,
		Dialect:           "sqlite",
	})

	c.Assert(err, qt.ErrorMatches, `baseline shadow check failed: connect to shadow database: .*`)
	var shadowErr *shadow.VerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "connect")
	c.Assert(shadowErr.Result.Mismatches, qt.HasLen, 1)
	c.Assert(shadowErr.Result.Mismatches[0].Kind, qt.Equals, "connect_error")
	c.Assert(shadowErr.Err, qt.IsNotNil)
}

func TestVerifyBaseline_UsesProvidedSnapshotInsteadOfPath(t *testing.T) {
	c := qt.New(t)
	target, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(t.TempDir(), "target.db"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })
	_, err = target.ExecContext(t.Context(), "CREATE TABLE authorized_snapshot (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	reopenedDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(reopenedDir, "0000000001_changed.up.sql"),
		[]byte("CREATE TABLE changed_after_verification (id INTEGER PRIMARY KEY);"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(reopenedDir, "0000000001_changed.down.sql"),
		[]byte("DROP TABLE changed_after_verification;"),
		0o600,
	), qt.IsNil)
	authorized := fstest.MapFS{
		"0000000001_authorized.up.sql": {Data: []byte(
			"CREATE TABLE authorized_snapshot (id INTEGER PRIMARY KEY);",
		)},
		"0000000001_authorized.down.sql": {Data: []byte(
			"DROP TABLE authorized_snapshot;",
		)},
	}

	err = shadow.VerifyBaseline(t.Context(), shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(t.TempDir(), "shadow.db"),
		TargetConn:        target,
		MigrationsDir:     reopenedDir,
		MigrationsFS:      authorized,
		Version:           1,
		Dialect:           "sqlite",
	})

	c.Assert(err, qt.IsNil)
}

func TestVerifyBaseline_SchemaMismatchReturnsAllStructuredDifferences(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	targetURL := "sqlite://" + filepath.Join(dir, "target.db")
	shadowURL := "sqlite://" + filepath.Join(dir, "shadow.db")
	target, err := dbschema.ConnectToDatabase(t.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	_, err = target.ExecContext(t.Context(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	c.Assert(err, qt.IsNil)

	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY, legacy TEXT);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE users;\n"),
		0o600,
	), qt.IsNil)

	err = shadow.VerifyBaseline(t.Context(), shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: shadowURL,
		TargetConn:        target,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           "sqlite",
		Capabilities:      target.Info().Capabilities,
	})

	c.Assert(err, qt.ErrorMatches, `baseline shadow check failed: missing column users\.legacy`)
	var shadowErr *shadow.VerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Err, qt.IsNil)
	c.Assert(shadowErr.Result, qt.DeepEquals, shadow.VerificationResult{
		Stage: "schema-match",
		Mismatches: []shadow.Mismatch{
			{
				Kind:    "missing_column",
				Object:  "users.legacy",
				Table:   "users",
				Column:  "legacy",
				Message: "missing column users.legacy",
			},
			{
				Kind:    "extra_column",
				Object:  "users.name",
				Table:   "users",
				Column:  "name",
				Message: "extra column users.name",
			},
		},
	})
}

func TestVerifyBaseline_RejectsTargetDatabaseAsShadow(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.db")
	targetURL := "sqlite://" + targetPath
	target, err := dbschema.ConnectToDatabase(t.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	_, err = target.ExecContext(t.Context(), "CREATE TABLE users (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	migrationsDir := writeBaselineShadowRealmSafetyFixture(c, dir)

	err = shadow.VerifyBaseline(t.Context(), shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: targetURL,
		TargetConn:        target,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           target.Info().Dialect,
	})

	assertBaselineShadowRealmRejected(c, target, err)
}

func TestVerifyBaseline_RejectsEquivalentTargetDatabaseAlias(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.db")
	targetURL := "sqlite://" + targetPath
	target, err := dbschema.ConnectToDatabase(t.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	_, err = target.ExecContext(t.Context(), "CREATE TABLE users (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(target)

	aliasPath := filepath.Join(dir, "target-alias.db")
	c.Assert(os.Link(targetPath, aliasPath), qt.IsNil)
	target, err = dbschema.ConnectToDatabase(t.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	migrationsDir := writeBaselineShadowRealmSafetyFixture(c, dir)

	err = shadow.VerifyBaseline(t.Context(), shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: "sqlite://" + aliasPath,
		TargetConn:        target,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           target.Info().Dialect,
	})

	assertBaselineShadowRealmRejected(c, target, err)
}

func writeBaselineShadowRealmSafetyFixture(c *qt.C, dir string) string {
	c.Helper()
	migrationsDir := filepath.Join(dir, "baseline-migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE users;"),
		0o600,
	), qt.IsNil)
	return migrationsDir
}

func assertBaselineShadowRealmRejected(
	c *qt.C,
	target *dbschema.DatabaseConnection,
	err error,
) {
	c.Helper()
	c.Assert(err, qt.ErrorMatches, `baseline shadow check failed: shadow database must be distinct from target database`)
	var shadowErr *shadow.VerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "realm-check")
	c.Assert(shadowErr.Result.Mismatches, qt.DeepEquals, []shadow.Mismatch{{
		Kind:    "target_shadow_same_realm",
		Message: "shadow database must be distinct from target database",
	}})
	var tableCount int64
	c.Assert(target.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'").Scan(&tableCount), qt.IsNil)
	c.Assert(tableCount, qt.Equals, int64(1))
}
