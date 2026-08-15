package atlasschema_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// seedSourceSQLite creates a SQLite database with the given DDL and returns
// its path.
func seedSourceSQLite(t *testing.T, ddl string) string {
	t.Helper()
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "seed.db")
	conn := connectSQLite(c, dbPath)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll, ddl), qt.IsNil)
	return dbPath
}

func writeAtlasMigrationDir(t *testing.T, ddl string) string {
	t.Helper()
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"), []byte(ddl), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

func TestDiff_DatabaseFromSource(t *testing.T) {
	c := qt.New(t)
	sourcePath := seedSourceSQLite(t, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	dir := t.TempDir()
	to := filepath.Join(dir, "to.sql")
	c.Assert(os.WriteFile(to, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT
);
`), 0o600), qt.IsNil)

	report, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"sqlite://" + sourcePath},
		ToURLs:   []string{"file://" + to},
	})

	c.Assert(err, qt.IsNil)
	sql, err := report.MarshalSQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "email")
}

func TestDiff_DatabaseToSource(t *testing.T) {
	c := qt.New(t)
	sourcePath := seedSourceSQLite(t, "CREATE TABLE desired_users (id INTEGER PRIMARY KEY);\n")
	dir := t.TempDir()
	from := filepath.Join(dir, "from.sql")
	c.Assert(os.WriteFile(from, []byte(""), 0o600), qt.IsNil)

	report, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"file://" + from},
		ToURLs:   []string{"sqlite://" + sourcePath},
	})

	c.Assert(err, qt.IsNil)
	sql, err := report.MarshalSQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "desired_users")
}

func TestDiff_DatabaseSourcesSynced(t *testing.T) {
	c := qt.New(t)
	ddl := "CREATE TABLE synced_users (id INTEGER PRIMARY KEY);\n"
	fromPath := seedSourceSQLite(t, ddl)
	toPath := seedSourceSQLite(t, ddl)

	report, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"sqlite://" + fromPath},
		ToURLs:   []string{"sqlite://" + toPath},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Changes, qt.HasLen, 0)
}

func TestDiff_MigrationDirToSource(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasMigrationDir(t, "CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n")
	dir := t.TempDir()
	from := filepath.Join(dir, "from.sql")
	c.Assert(os.WriteFile(from, []byte(""), 0o600), qt.IsNil)
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")

	report, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"file://" + from},
		ToURLs:   []string{"file://" + migrationsDir},
		DevURL:   devURL,
	})

	c.Assert(err, qt.IsNil)
	sql, err := report.MarshalSQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "replayed_users")
}

func TestDiff_MigrationDirRequiresDevURL(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasMigrationDir(t, "CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n")
	dir := t.TempDir()
	from := filepath.Join(dir, "from.sql")
	c.Assert(os.WriteFile(from, []byte(""), 0o600), qt.IsNil)

	_, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"file://" + from},
		ToURLs:   []string{"file://" + migrationsDir},
	})

	c.Assert(err, qt.ErrorMatches,
		`--to "file://.*" is a migration directory; --dev-url is required to replay it on a dev database`)
}

func TestDiff_DatabaseDialectConflict(t *testing.T) {
	c := qt.New(t)

	_, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"postgres://localhost:1/app"},
		ToURLs:   []string{"mysql://localhost:1/app"},
	})

	c.Assert(err, qt.ErrorMatches, `--to database dialect "mysql" does not match --from dialect "postgres"`)
}

func TestDiff_DevURLDialectConflictWithDatabaseSource(t *testing.T) {
	c := qt.New(t)

	_, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"postgres://localhost:1/app"},
		ToURLs:   []string{"postgres://localhost:1/app"},
		DevURL:   "sqlite://dev.db",
	})

	c.Assert(err, qt.ErrorMatches, `--from database dialect "postgres" does not match --dev-url dialect "sqlite"`)
}

func TestDiff_UnsupportedSchemeFails(t *testing.T) {
	c := qt.New(t)

	_, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"atlas://remote/app"},
		ToURLs:   []string{"sqlite://a.db"},
	})

	c.Assert(err, qt.ErrorMatches, `--from "atlas://remote/app": atlas:// registry URLs are not supported; use oci://.*`)
}

func TestPrepareApply_DatabaseSource(t *testing.T) {
	c := qt.New(t)
	sourcePath := seedSourceSQLite(t, "CREATE TABLE mirrored_users (id INTEGER PRIMARY KEY);\n")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	conn := connectSQLite(c, targetPath)
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasschema.PrepareApply(t.Context(), conn, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{"sqlite://" + sourcePath},
		TxMode: migrator.MigrationTxModeFile,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasChanges(), qt.IsTrue)
	c.Assert(plan.Execute(t.Context()), qt.IsNil)
	c.Assert(sqliteTableExists(c, targetPath, "mirrored_users"), qt.IsTrue)
}

func TestPrepareApply_DatabaseSourceDialectMismatch(t *testing.T) {
	c := qt.New(t)
	targetPath := filepath.Join(t.TempDir(), "target.db")
	conn := connectSQLite(c, targetPath)
	defer dbschema.CloseAndWarn(conn)

	_, err := atlasschema.PrepareApply(t.Context(), conn, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{"postgres://localhost:1/app"},
		TxMode: migrator.MigrationTxModeFile,
	})

	c.Assert(err, qt.ErrorMatches,
		`load --to schema: --to database dialect "postgres" does not match --url dialect "sqlite"`)
}

func TestPrepareApply_MigrationDirSource(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasMigrationDir(t, "CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n")
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	conn := connectSQLite(c, targetPath)
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasschema.PrepareApply(t.Context(), conn, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{"file://" + migrationsDir},
		DevURL: devURL,
		TxMode: migrator.MigrationTxModeFile,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasChanges(), qt.IsTrue)
	c.Assert(plan.Execute(t.Context()), qt.IsNil)
	c.Assert(sqliteTableExists(c, targetPath, "replayed_users"), qt.IsTrue)
}

func TestPrepareApply_MigrationDirSourceRequiresDevURL(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasMigrationDir(t, "CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	conn := connectSQLite(c, targetPath)
	defer dbschema.CloseAndWarn(conn)

	_, err := atlasschema.PrepareApply(t.Context(), conn, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{"file://" + migrationsDir},
		TxMode: migrator.MigrationTxModeFile,
	})

	c.Assert(err, qt.ErrorMatches,
		`load --to schema: --to "file://.*" is a migration directory; --dev-url is required to replay it on a dev database`)
}

// TestPlanApply_EmptyLocalDirectoryRefuses pins the one directory that is not a
// desired state. A directory of schema files is one since stokaro/ptah#940 item
// B; an empty directory holds no schema, and the community binary refuses it
// with the same sentence.
func TestPlanApply_EmptyLocalDirectoryRefuses(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	targetPath := filepath.Join(t.TempDir(), "target.db")
	conn := connectSQLite(c, targetPath)
	defer dbschema.CloseAndWarn(conn)

	_, err := atlasschema.PlanApply(t.Context(), conn, atlasschema.ApplyOptions{
		ToURLs: []string{"file://" + dir},
	})

	c.Assert(err, qt.ErrorMatches, `load --to schema: ".*" contains neither SQL nor HCL files`)
}

// TestPlanApply_LocalDirectoryOfSQLFiles is the regression test for
// stokaro/ptah#940 item B on the apply planner: a directory of .sql files used
// to fail with `schema file is a directory` and now plans both tables.
func TestPlanApply_LocalDirectoryOfSQLFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_users.sql"),
		[]byte("CREATE TABLE plan_dir_users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "2_posts.sql"),
		[]byte("CREATE TABLE plan_dir_posts (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	targetPath := filepath.Join(t.TempDir(), "target.db")
	conn := connectSQLite(c, targetPath)
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasschema.PlanApply(t.Context(), conn, atlasschema.ApplyOptions{
		ToURLs: []string{"file://" + dir},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.SQL(), qt.Contains, "plan_dir_users")
	c.Assert(plan.SQL(), qt.Contains, "plan_dir_posts")
}

// TestPreparePlanFile_MigrationDirStaysLocalOnly pins that `schema plan`
// keeps the pre-resolver local-file loading: a migration directory --to is not
// replayed there. Since stokaro/ptah#940 item B the loader also refuses to read
// a migration directory as a schema directory, so atlas.sum keeps meaning
// exactly one thing on both spellings.
func TestPreparePlanFile_MigrationDirStaysLocalOnly(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasMigrationDir(t, "CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);\n")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	conn := connectSQLite(c, targetPath)
	defer dbschema.CloseAndWarn(conn)

	_, err := atlasschema.PreparePlanFile(t.Context(), conn, atlasschema.PlanFileOptions{
		ToURLs: []string{"file://" + migrationsDir},
	})

	c.Assert(err, qt.ErrorMatches,
		`load --to schema: ".*" is a migration directory \(it contains atlas\.sum\), not a schema directory`)
}
