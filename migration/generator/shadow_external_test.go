package generator_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/generator"
)

func TestGenerateMigration_ShadowSchemaMismatchReturnsStructuredError(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	targetURL := "sqlite://" + filepath.Join(dir, "target.db")
	shadowURL := "sqlite://" + filepath.Join(dir, "shadow.db")

	target, err := dbschema.ConnectToDatabase(t.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	_, err = target.ExecContext(t.Context(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(target)

	modelsDir := filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(modelsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "models.go"), []byte(`package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
	//ptah:schema:field name="name" type="TEXT"
	Name string
	//ptah:schema:field name="email" type="TEXT"
	Email string
}
`), 0o600), qt.IsNil)

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

	files, err := generator.GenerateMigration(t.Context(), generator.GenerateMigrationOptions{
		GoEntitiesDir:     modelsDir,
		DatabaseURL:       targetURL,
		MigrationName:     "add_email",
		OutputDir:         migrationsDir,
		ShadowDatabaseURL: shadowURL,
	})

	c.Assert(files, qt.IsNil)
	var shadowErr *generator.ShadowVerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Error(), qt.Equals, "shadow check failed: missing column users.name")
	c.Assert(shadowErr.Result.Stage, qt.Equals, "schema-match")
	c.Assert(shadowErr.Result.Mismatches, qt.DeepEquals, []generator.ShadowMismatch{
		{
			Kind:    "missing_column",
			Object:  "users.name",
			Table:   "users",
			Column:  "name",
			Message: "missing column users.name",
		},
		{
			Kind:    "extra_column",
			Object:  "users.legacy",
			Table:   "users",
			Column:  "legacy",
			Message: "extra column users.legacy",
		},
	})
	c.Assert(shadowErr.Err, qt.IsNil)
	c.Assert(errors.Unwrap(shadowErr), qt.IsNil)

	rawResult, err := json.Marshal(shadowErr.Result)
	c.Assert(err, qt.IsNil)
	c.Assert(string(rawResult), qt.Equals, `{"stage":"schema-match","mismatches":[{"kind":"missing_column","object":"users.name","table":"users","column":"name","message":"missing column users.name"},{"kind":"extra_column","object":"users.legacy","table":"users","column":"legacy","message":"extra column users.legacy"}]}`)

	migrationFiles, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(migrationFiles, qt.HasLen, 2)
}

func TestGenerateMigration_RejectsTargetDatabaseAsShadow(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.db")
	targetURL := "sqlite://" + targetPath
	target, err := dbschema.ConnectToDatabase(t.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	_, err = target.ExecContext(t.Context(), "CREATE TABLE users (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	modelsDir, migrationsDir := writeShadowRealmSafetyFixture(c, dir)

	files, err := generator.GenerateMigration(t.Context(), generator.GenerateMigrationOptions{
		GoEntitiesDir:     modelsDir,
		DBConn:            target,
		MigrationName:     "add_email",
		OutputDir:         migrationsDir,
		ShadowDatabaseURL: targetURL,
	})

	assertShadowRealmRejected(c, target, files, err)
}

func TestGenerateMigration_RejectsEquivalentTargetDatabaseAliasAsShadow(t *testing.T) {
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
	modelsDir, migrationsDir := writeShadowRealmSafetyFixture(c, dir)

	files, err := generator.GenerateMigration(t.Context(), generator.GenerateMigrationOptions{
		GoEntitiesDir:     modelsDir,
		DBConn:            target,
		MigrationName:     "add_email",
		OutputDir:         migrationsDir,
		ShadowDatabaseURL: "sqlite://" + aliasPath,
	})

	assertShadowRealmRejected(c, target, files, err)
}

func TestVerifyBaselineShadow_RejectsTargetDatabaseAsShadow(t *testing.T) {
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

	err = generator.VerifyBaselineShadow(t.Context(), generator.BaselineShadowVerifyOptions{
		ShadowDatabaseURL: targetURL,
		TargetConn:        target,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           target.Info().Dialect,
	})

	assertBaselineShadowRealmRejected(c, target, err)
}

func TestVerifyBaselineShadow_RejectsEquivalentTargetDatabaseAlias(t *testing.T) {
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

	err = generator.VerifyBaselineShadow(t.Context(), generator.BaselineShadowVerifyOptions{
		ShadowDatabaseURL: "sqlite://" + aliasPath,
		TargetConn:        target,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           target.Info().Dialect,
	})

	assertBaselineShadowRealmRejected(c, target, err)
}

func writeShadowRealmSafetyFixture(c *qt.C, dir string) (modelsDir, migrationsDir string) {
	c.Helper()
	modelsDir = filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(modelsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "models.go"), []byte(`package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
	//ptah:schema:field name="email" type="TEXT"
	Email string
}
`), 0o600), qt.IsNil)
	migrationsDir = filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	return modelsDir, migrationsDir
}

func assertShadowRealmRejected(
	c *qt.C,
	target *dbschema.DatabaseConnection,
	files *generator.MigrationFiles,
	err error,
) {
	c.Helper()
	c.Assert(files, qt.IsNil)
	var shadowErr *generator.ShadowVerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "realm-check")
	c.Assert(shadowErr.Result.Mismatches, qt.DeepEquals, []generator.ShadowMismatch{{
		Kind:    "target_shadow_same_realm",
		Message: "shadow database must be distinct from target database",
	}})
	var tableCount int64
	c.Assert(target.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'").Scan(&tableCount), qt.IsNil)
	c.Assert(tableCount, qt.Equals, int64(1))
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
	var tableCount int64
	c.Assert(target.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'").Scan(&tableCount), qt.IsNil)
	c.Assert(tableCount, qt.Equals, int64(1))
}
