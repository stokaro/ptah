package atlassource_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// seedSQLite creates a SQLite database file with the given DDL and returns
// its URL.
func seedSQLite(t *testing.T, ddl string) string {
	t.Helper()
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "source.db")
	url := "sqlite://" + dbPath
	conn, err := dbschema.ConnectToDatabase(context.Background(), url)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	_, err = conn.ExecContext(context.Background(), ddl)
	c.Assert(err, qt.IsNil)
	return url
}

func classifySingle(t *testing.T, flag, url string) atlassource.Set {
	t.Helper()
	c := qt.New(t)
	set, err := atlassource.ClassifySet(flag, []string{url}, atlassource.ProjectEnv{})
	c.Assert(err, qt.IsNil)
	return set
}

func TestResolve_DatabaseSourceIntrospectsLiveSchema(t *testing.T) {
	c := qt.New(t)
	url := seedSQLite(t, "CREATE TABLE live_users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)")
	set := classifySingle(t, "--from", url)

	state, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect:     "sqlite",
		DialectFlag: "--dev-url",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(state.Kind, qt.Equals, atlassource.KindDatabase)
	c.Assert(state.DB, qt.IsNotNil)
	c.Assert(state.DB.Tables, qt.HasLen, 1)
	c.Assert(state.DB.Tables[0].Name, qt.Equals, "live_users")
	c.Assert(state.Schema.Tables, qt.HasLen, 1)
	c.Assert(state.Schema.Tables[0].Name, qt.Equals, "live_users")
	c.Assert(state.DefaultSchema, qt.Equals, "main")
}

func TestResolve_DatabaseSourceDialectMismatchFailsBeforeConnecting(t *testing.T) {
	c := qt.New(t)
	// The URL points at nothing connectable; the scheme-level dialect check
	// must fire first.
	set := classifySingle(t, "--to", "postgres://localhost:1/never")

	_, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect:     "sqlite",
		DialectFlag: "--url",
	})

	c.Assert(err, qt.ErrorMatches, `--to database dialect "postgres" does not match --url dialect "sqlite"`)
}

func TestResolve_MigrationDirReplaysOnDevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationDir(t)
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	set := classifySingle(t, "--to", "file://"+dir)

	state, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect:     "sqlite",
		DialectFlag: "--url",
		DevURL:      devURL,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(state.Kind, qt.Equals, atlassource.KindMigrationDir)
	c.Assert(state.DB, qt.IsNotNil)
	c.Assert(state.DB.Tables, qt.HasLen, 1)
	c.Assert(state.DB.Tables[0].Name, qt.Equals, "replayed_users")
	c.Assert(state.Schema.Tables, qt.HasLen, 1)
	c.Assert(state.Schema.Tables[0].Name, qt.Equals, "replayed_users")
	c.Assert(state.DefaultSchema, qt.Equals, "main")
	assertSQLiteDevEmpty(c, devURL)
}

func TestResolve_MigrationDirFailureCleansPartialDevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_create_users.sql"),
		[]byte(`
CREATE TABLE users (id INTEGER PRIMARY KEY);
CREATE VIEW user_ids AS SELECT id FROM users;
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "2_invalid.sql"),
		[]byte("THIS IS NOT SQL;\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	set := classifySingle(t, "--to", "file://"+dir)

	_, err = set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect: "sqlite",
		DevURL:  devURL,
	})

	c.Assert(err, qt.ErrorMatches, `(?s)--to "file://.*": replay migration 2 on dev database: .*`)
	assertSQLiteDevEmpty(c, devURL)
}

func TestResolve_MigrationDirRequiresDevURL(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationDir(t)
	set := classifySingle(t, "--to", "file://"+dir)

	_, err := set.Resolve(t.Context(), atlassource.ResolveOptions{Dialect: "sqlite"})

	c.Assert(err, qt.ErrorMatches,
		`--to "file://.*" is a migration directory; --dev-url is required to replay it on a dev database`)
}

func TestResolve_MigrationDirRejectsDockerDevURL(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationDir(t)
	set := classifySingle(t, "--to", "file://"+dir)

	_, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect: "sqlite",
		DevURL:  "docker://sqlite/latest/dev",
	})

	c.Assert(err, qt.ErrorMatches,
		`docker --dev-url values are accepted by Atlas, but Ptah requires a directly connectable dev database URL for migration SQL replay`)
}

func TestResolve_MigrationDirRejectsDevDialectMismatch(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationDir(t)
	set := classifySingle(t, "--to", "file://"+dir)

	_, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect:     "sqlite",
		DialectFlag: "--from",
		DevURL:      "postgres://localhost:1/dev",
	})

	c.Assert(err, qt.ErrorMatches, `--dev-url dialect "postgres" does not match --from dialect "sqlite"`)
}

func TestResolve_MigrationDirRejectsChecksumDrift(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationDir(t)
	// Change migration content after atlas.sum was written to simulate drift.
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
		[]byte("CREATE TABLE tampered (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	set := classifySingle(t, "--to", "file://"+dir)

	_, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect: "sqlite",
		DevURL:  devURL,
	})

	c.Assert(err, qt.ErrorMatches, `(?s)migration directory checksum verification failed:.*`)
}

func TestResolve_MigrationDirValidatesStableSnapshotBeforeDevConnection(t *testing.T) {
	c := qt.New(t)
	dir := writeMigrationDir(t)
	devPath := filepath.Join(t.TempDir(), "dev.db")
	set := classifySingle(t, "--to", "file://"+dir)
	wantErr := errors.New("strict migration policy")

	_, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect: "sqlite",
		DevURL:  "sqlite://" + devPath,
		ValidateMigrationSource: func(snapshot fs.FS) error {
			contents, readErr := fs.ReadFile(snapshot, "1_init.sql")
			c.Assert(readErr, qt.IsNil)
			c.Assert(string(contents), qt.Equals,
				"CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n")
			return wantErr
		},
	})

	c.Assert(err, qt.ErrorIs, wantErr)
	_, statErr := os.Stat(devPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

func TestResolve_LocalFileValidatesSourceBeforeParsing(t *testing.T) {
	c := qt.New(t)
	wantErr := errors.New("strict local-source policy")
	set := classifySingle(t, "--to", "file://missing-schema.yaml")

	_, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect: "sqlite",
		DevURL:  "sqlite://dev.db",
		ValidateLocalSchemaSource: func(source string) error {
			c.Assert(filepath.Base(source), qt.Equals, "missing-schema.yaml")
			return wantErr
		},
	})

	c.Assert(err, qt.ErrorIs, wantErr)
}

func TestResolve_MigrationDirWithoutSumViaEnvReplays(t *testing.T) {
	// env://migration.dir marks the directory as a migration source even
	// without atlas.sum; a missing checksum file is tolerated, mirroring
	// `atlas migrate diff`.
	c := qt.New(t)
	baseDir := t.TempDir()
	migrationsDir := filepath.Join(baseDir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"),
		[]byte("CREATE TABLE env_dir_users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	env := atlassource.ProjectEnv{
		Loaded:  true,
		BaseDir: baseDir,
		Config:  projectconfig.Config{Migration: projectconfig.MigrationConfig{Dir: "file://migrations"}},
	}
	set, err := atlassource.ClassifySet("--to", []string{"env://migration.dir"}, env)
	c.Assert(err, qt.IsNil)

	state, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect: "sqlite",
		DevURL:  devURL,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(state.Schema.Tables, qt.HasLen, 1)
	c.Assert(state.Schema.Tables[0].Name, qt.Equals, "env_dir_users")
}

func TestResolve_MigrationDirFiltersRevisionTable(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"), []byte(
		"CREATE TABLE kept (id INTEGER PRIMARY KEY);\n"+
			"CREATE TABLE atlas_schema_revisions (version TEXT PRIMARY KEY);\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	set := classifySingle(t, "--to", "file://"+dir)

	state, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect: "sqlite",
		DevURL:  devURL,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(state.DB.Tables, qt.HasLen, 1)
	c.Assert(state.DB.Tables[0].Name, qt.Equals, "kept")
}

func TestResolve_LocalFilesMatchLegacyLoader(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath,
		[]byte("CREATE TABLE files_users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	set := classifySingle(t, "--to", "file://"+schemaPath)

	state, err := set.Resolve(t.Context(), atlassource.ResolveOptions{Dialect: "sqlite"})

	c.Assert(err, qt.IsNil)
	c.Assert(state.Kind, qt.Equals, atlassource.KindLocalFile)
	c.Assert(state.DB, qt.IsNil)
	c.Assert(state.Schema.Tables, qt.HasLen, 1)
	c.Assert(state.Schema.Tables[0].Name, qt.Equals, "files_users")
}

// TestResolve_EmptyLocalDirectoryRefuses covers the one directory that is still
// not a desired state: a directory without atlas.sum is a schema directory now
// (stokaro/ptah#940 item B), and an empty one holds no schema to resolve.
func TestResolve_EmptyLocalDirectoryRefuses(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	set := classifySingle(t, "--to", "file://"+dir)

	_, err := set.Resolve(t.Context(), atlassource.ResolveOptions{Dialect: "sqlite"})

	c.Assert(err, qt.ErrorMatches, `".*" contains neither SQL nor HCL files`)
}

// TestResolve_LocalDirectoryOfSQLFiles is the regression test for
// stokaro/ptah#940 item B on the resolver: a file:// directory of .sql schema
// files was classified as a local file and then refused with `schema file is a
// directory`, while the pinned community binary read every file in it.
func TestResolve_LocalDirectoryOfSQLFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_users.sql"),
		[]byte("CREATE TABLE dir_users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "2_posts.sql"),
		[]byte("CREATE TABLE dir_posts (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	set := classifySingle(t, "--to", "file://"+dir)

	state, err := set.Resolve(t.Context(), atlassource.ResolveOptions{Dialect: "sqlite"})

	c.Assert(err, qt.IsNil)
	c.Assert(state.Kind, qt.Equals, atlassource.KindLocalFile)
	c.Assert(state.Schema.Tables, qt.HasLen, 2)
}

func assertSQLiteDevEmpty(c *qt.C, devURL string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(c.Context(), `
		SELECT COUNT(*)
		FROM pragma_table_list
		WHERE schema = ?
		  AND type IN ('table', 'view', 'virtual')
		  AND name NOT LIKE 'sqlite_%'
		  AND name <> 'schema_migrations'
	`, "main").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}
