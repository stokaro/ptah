package atlasschema_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/internal/atlassource"
	"github.com/stokaro/ptah/internal/devlock"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/schemafile"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestInspectSource_DatabaseURL(t *testing.T) {
	c := qt.New(t)
	dbPath := seedInspectSQLiteDB(c)

	rendered, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
		URL:    "sqlite://" + dbPath,
		Format: "hcl",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `table "users"`)
	c.Assert(rendered, qt.Contains, `column "email"`)
}

// TestInspectSource_LocalSQLFileOnDev mirrors the pinned Atlas
// cli-inspect-file fixture: a local schema file is materialized on the dev
// database and the introspected result is rendered.
func TestInspectSource_LocalSQLFileOnDev(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "a.sql")
	devPath := filepath.Join(dir, "dev.db")
	c.Assert(os.WriteFile(schemaPath, []byte("CREATE TABLE users (\n  id INTEGER PRIMARY KEY,\n  email TEXT NOT NULL\n);\n"), 0o600), qt.IsNil)

	rendered, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
		URL:    "file://" + schemaPath,
		DevURL: "sqlite://" + devPath,
		Format: "hcl",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `table "users"`)
	c.Assert(rendered, qt.Contains, `column "email"`)
	assertInspectSQLiteDevEmpty(c, devPath)
}

func TestInspectSource_LocalSQLFileWaitsForDevRealmLock(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	devPath := filepath.Join(dir, "dev.db")
	c.Assert(
		os.WriteFile(
			schemaPath,
			[]byte("CREATE TABLE locked_inspection (id INTEGER PRIMARY KEY);\n"),
			0o600,
		),
		qt.IsNil,
	)

	lockConn := connectSQLite(c, devPath)
	defer dbschema.CloseAndWarn(lockConn)
	lock, err := devlock.Acquire(t.Context(), lockConn, 0)
	c.Assert(err, qt.IsNil)

	blockedCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	rendered, err := atlasschema.InspectSource(blockedCtx, atlasschema.InspectSourceOptions{
		URL:    "file://" + schemaPath,
		DevURL: "sqlite://" + devPath,
		Format: "hcl",
	})
	c.Assert(err, qt.ErrorMatches, `acquire schema inspection dev database lock: .*context deadline exceeded`)
	c.Assert(rendered, qt.Equals, "")
	c.Assert(lock.Release(), qt.IsNil)

	rendered, err = atlasschema.InspectSource(t.Context(), atlasschema.InspectSourceOptions{
		URL:    "file://" + schemaPath,
		DevURL: "sqlite://" + devPath,
		Format: "hcl",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `table "locked_inspection"`)
	assertInspectSQLiteDevEmpty(c, devPath)
}

// TestInspectSource_DevDatabaseIsReset proves the dev database is reset
// before the source is materialized: stale dev objects must not leak into
// the inspection output.
func TestInspectSource_DevDatabaseIsReset(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devPath := filepath.Join(dir, "dev.db")
	devConn := connectSQLite(c, devPath)
	_, err := devConn.ExecContext(context.Background(), "CREATE TABLE stale_dev_table (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(devConn)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte("CREATE TABLE fresh_table (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	rendered, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
		URL:    "file://" + schemaPath,
		DevURL: "sqlite://" + devPath,
		Format: "hcl",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `table "fresh_table"`)
	c.Assert(rendered, qt.Not(qt.Contains), "stale_dev_table")
}

func TestInspectSource_MigrationDirOnDev(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	devPath := filepath.Join(dir, "dev.db")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_init.sql"),
		[]byte("CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	rendered, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
		URL:    "file://" + migrationsDir,
		DevURL: "sqlite://" + devPath,
		Format: "hcl",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `table "replayed_users"`)
	c.Assert(rendered, qt.Not(qt.Contains), "atlas_schema_revisions")
	assertInspectSQLiteDevEmpty(c, devPath)
}

func assertInspectSQLiteDevEmpty(c *qt.C, path string) {
	c.Helper()
	conn := connectSQLite(c, path)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err := conn.QueryRowContext(c.Context(), `
		SELECT COUNT(*)
		FROM main.sqlite_schema
		WHERE name NOT LIKE 'sqlite_%'
	`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestInspectSource_EnvSchemaSource(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(dir, "schema.sql"),
		[]byte("CREATE TABLE env_sourced (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)

	rendered, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
		URL:    "env://src",
		DevURL: "sqlite://" + filepath.Join(dir, "dev.db"),
		Format: "hcl",
		ProjectEnv: atlassource.ProjectEnv{
			Loaded:  true,
			Config:  projectconfig.Config{SchemaSources: []string{"schema.sql"}},
			BaseDir: dir,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `table "env_sourced"`)
}

// TestInspectSource_SplitWriteExportReloads proves the exported split files
// round-trip: every written HCL file re-parses through the schema-file
// loader, and together they reproduce the inspected tables.
func TestInspectSource_SplitWriteExportReloads(t *testing.T) {
	c := qt.New(t)
	dbPath := seedInspectSQLiteDB(c)
	outDir := filepath.Join(t.TempDir(), "schema")

	rendered, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
		URL:    "sqlite://" + dbPath,
		Format: `{{ hcl . | split | write "` + outDir + `" }}`,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Equals, "")
	written := collectFiles(c, outDir, ".hcl")
	c.Assert(written, qt.Not(qt.HasLen), 0)
	reloaded, err := schemafile.LoadAll(written, schemafile.Options{Dialect: "sqlite"})
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(reloaded.Tables))
	for _, table := range reloaded.Tables {
		names = append(names, table.Name)
	}
	sort.Strings(names)
	c.Assert(names, qt.DeepEquals, []string{"posts", "users"})
}

// TestInspectSource_SQLSplitWriteExportReloads round-trips the SQL export:
// each written object file re-parses as schema SQL. The fixture avoids
// foreign keys because the SQL schema parser does not yet accept the inline
// REFERENCES column constraint the SQLite renderer emits for them.
func TestInspectSource_SQLSplitWriteExportReloads(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect-sql-export.db")
	conn := connectSQLite(c, dbPath)
	_, err := conn.ExecContext(context.Background(), `
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);
CREATE TABLE sessions (
  id INTEGER PRIMARY KEY,
  token TEXT NOT NULL
);
`)
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)
	outDir := filepath.Join(dir, "schema-sql")

	rendered, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
		URL:    "sqlite://" + dbPath,
		Format: `{{ sql . | split | write "` + outDir + `" }}`,
	})
	c.Assert(err, qt.IsNil)

	c.Assert(rendered, qt.Equals, "")
	mainSQL, err := os.ReadFile(filepath.Join(outDir, "main.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(mainSQL), qt.Contains, "-- atlas:import ./tables/users.sql")
	objectFiles := collectFiles(c, filepath.Join(outDir, "tables"), ".sql")
	c.Assert(objectFiles, qt.Not(qt.HasLen), 0)
	reloaded, err := schemafile.LoadAll(objectFiles, schemafile.Options{Dialect: "sqlite"})
	c.Assert(err, qt.IsNil)
	c.Assert(reloaded.Tables, qt.HasLen, 2)
}

// TestInspectSource_FileExportThenDevInspectionRoundTrip exports a live
// inspection to a single HCL file and re-inspects that file through the dev
// database: the reloaded output must reproduce the live output.
func TestInspectSource_FileExportThenDevInspectionRoundTrip(t *testing.T) {
	c := qt.New(t)
	dbPath := seedInspectSQLiteDB(c)
	dir := t.TempDir()

	live, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
		URL:    "sqlite://" + dbPath,
		Format: "hcl",
	})
	c.Assert(err, qt.IsNil)
	exported := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(exported, []byte(live), 0o600), qt.IsNil)

	reloaded, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
		URL:    "file://" + exported,
		DevURL: "sqlite://" + filepath.Join(dir, "dev.db"),
		Format: "hcl",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(reloaded, qt.Equals, live)
}

func TestInspectSource_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("local file requires dev url", func(c *qt.C) {
		schemaPath := filepath.Join(c.TempDir(), "schema.sql")
		c.Assert(os.WriteFile(schemaPath, []byte("CREATE TABLE t (id int);\n"), 0o600), qt.IsNil)

		rendered, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
			URL: "file://" + schemaPath,
		})

		c.Assert(err, qt.ErrorMatches, `--dev-url cannot be empty`)
		c.Assert(rendered, qt.Equals, "")
	})

	c.Run("docker dev url", func(c *qt.C) {
		schemaPath := filepath.Join(c.TempDir(), "schema.sql")
		c.Assert(os.WriteFile(schemaPath, []byte("CREATE TABLE t (id int);\n"), 0o600), qt.IsNil)

		_, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
			URL:    "file://" + schemaPath,
			DevURL: "docker://sqlite",
		})

		c.Assert(err, qt.ErrorMatches, `docker --dev-url values are accepted by Atlas, but Ptah requires a directly connectable dev database URL for schema inspection`)
	})

	c.Run("unsupported source scheme", func(c *qt.C) {
		_, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
			URL: "atlas://remote/app",
		})

		c.Assert(err, qt.ErrorMatches, `--url "atlas://remote/app": atlas:// registry URLs are not supported.*`)
	})

	c.Run("invalid exclude selector before any connection", func(c *qt.C) {
		// The unreachable URL proves selector validation runs pre-connect:
		// reaching the database would produce a connection error instead.
		_, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
			URL:     "postgres://127.0.0.1:1/unreachable",
			Exclude: []string{"a[type=table].b[type=column]"},
		})

		c.Assert(err, qt.ErrorMatches, `unsupported Atlas exclude selector .*final pattern segment only`)
	})

	c.Run("invalid format before source resolution", func(c *qt.C) {
		_, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
			URL:    "sqlite://ignored.db",
			Format: "{{ if }}",
		})

		c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
	})

	c.Run("write escaping the working directory", func(c *qt.C) {
		dbPath := seedInspectSQLiteDB(c)

		_, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
			URL:    "sqlite://" + dbPath,
			Format: `{{ sql . | split | write "../../outside-ptah" }}`,
		})

		c.Assert(err, qt.ErrorMatches, `resolve output directory: .*outside allowed root.*`)
	})

	c.Run("duplicate write targets", func(c *qt.C) {
		dbPath := seedInspectSQLiteDB(c)
		outDir := filepath.Join(c.TempDir(), "dup")

		_, err := atlasschema.InspectSource(context.Background(), atlasschema.InspectSourceOptions{
			URL:    "sqlite://" + dbPath,
			Format: `{{ $s := sql . | split }}{{ $s | write "` + outDir + `" }}{{ $s | write "` + outDir + `" }}`,
		})

		c.Assert(err, qt.ErrorMatches, `duplicate output path "main.sql" .*`)
	})
}

func seedInspectSQLiteDB(c *qt.C) string {
	c.Helper()
	dbPath := filepath.Join(c.TempDir(), "inspect-source.db")
	conn := connectSQLite(c, dbPath)
	defer dbschema.CloseAndWarn(conn)
	createInspectSchema(c, conn)
	return dbPath
}

// collectFiles returns every file under root with the given extension, in
// deterministic sorted order.
func collectFiles(c *qt.C, root, extension string) []string {
	c.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, extension) {
			files = append(files, path)
		}
		return nil
	})
	c.Assert(err, qt.IsNil)
	sort.Strings(files)
	return files
}
