package migrator_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/examples/migrator"
	"go.5x5.cz/ptah/migration/migrationfile"
	"go.5x5.cz/ptah/migration/migrator"
)

// exampleSQLiteConnection opens a real SQLite database in a fresh temporary
// directory, so the examples in this file execute their migrations for real.
// SQLite ships compiled into ptah; a caller targeting a server passes that
// server's URL to dbschema.ConnectToDatabase instead.
func exampleSQLiteConnection() (conn *dbschema.DatabaseConnection, cleanup func()) {
	dir := must.Must(os.MkdirTemp("", "ptah-migrator-example"))
	conn = must.Must(dbschema.ConnectToDatabase(context.Background(), "sqlite://"+filepath.Join(dir, "app.db")))
	return conn, func() {
		_ = conn.Close()
		_ = os.RemoveAll(dir)
	}
}

// ExampleNewFSMigrator is the one-call filesystem path: discover the
// NNN_description.up.sql/.down.sql pairs, build the provider and the migrator
// in one step, and apply everything pending. MigrateUp creates the revision
// metadata table on first use, so a fresh database needs no setup beyond the
// connection. The constructor reads and validates the whole directory, so its
// error is one an embedder branches on; ExampleNewFSMigrator_errorHandling
// shows a directory it refuses.
func ExampleNewFSMigrator() {
	ctx := context.Background()
	conn, cleanup := exampleSQLiteConnection()
	defer cleanup()

	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE);"),
		},
		"0000000001_create_users.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;"),
		},
		"0000000002_add_name.up.sql": &fstest.MapFile{
			Data: []byte("ALTER TABLE users ADD COLUMN name TEXT;"),
		},
		"0000000002_add_name.down.sql": &fstest.MapFile{
			Data: []byte("ALTER TABLE users DROP COLUMN name;"),
		},
	}

	m, err := migrator.NewFSMigrator(conn, fsys)
	if err != nil {
		fmt.Println("new migrator:", err)
		return
	}
	if err := m.MigrateUp(ctx); err != nil {
		fmt.Println("migrate up:", err)
		return
	}

	fmt.Println("applied:", must.Must(m.GetAppliedMigrations(ctx)))

	// Output:
	// applied: [1 2]
}

// ExampleMigrator_MigrateTo moves a database to an exact version in whichever
// direction that takes: past the target it rolls migrations back with their
// down bodies, behind it it applies the pending up bodies. Here the schema is
// migrated all the way up and then back down to version 1.
func ExampleMigrator_MigrateTo() {
	ctx := context.Background()
	conn, cleanup := exampleSQLiteConnection()
	defer cleanup()

	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);"),
		},
		"0000000001_create_users.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;"),
		},
		"0000000002_add_email.up.sql": &fstest.MapFile{
			Data: []byte("ALTER TABLE users ADD COLUMN email TEXT;"),
		},
		"0000000002_add_email.down.sql": &fstest.MapFile{
			Data: []byte("ALTER TABLE users DROP COLUMN email;"),
		},
		"0000000003_add_index.up.sql": &fstest.MapFile{
			Data: []byte("CREATE UNIQUE INDEX idx_users_email ON users(email);"),
		},
		"0000000003_add_index.down.sql": &fstest.MapFile{
			Data: []byte("DROP INDEX idx_users_email;"),
		},
	}

	m := must.Must(migrator.NewFSMigrator(conn, fsys))
	if err := m.MigrateUp(ctx); err != nil {
		fmt.Println("migrate up:", err)
		return
	}
	fmt.Println("after MigrateUp:", must.Must(m.GetCurrentVersion(ctx)))

	if err := m.MigrateTo(ctx, 1); err != nil {
		fmt.Println("migrate to 1:", err)
		return
	}
	fmt.Println("after MigrateTo(1):", must.Must(m.GetCurrentVersion(ctx)))

	// Output:
	// after MigrateUp: 3
	// after MigrateTo(1): 1
}

// ExampleMigrator_MigrateDown rolls back exactly one migration: the newest
// applied version's down body runs and its revision row is removed, leaving
// the previous version current.
func ExampleMigrator_MigrateDown() {
	ctx := context.Background()
	conn, cleanup := exampleSQLiteConnection()
	defer cleanup()

	provider := migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "Create orders",
			"CREATE TABLE orders (id INTEGER PRIMARY KEY);",
			"DROP TABLE orders;"),
		migrator.CreateMigrationFromSQL(2, "Add total column",
			"ALTER TABLE orders ADD COLUMN total REAL;",
			"ALTER TABLE orders DROP COLUMN total;"),
	)
	m := migrator.NewMigrator(conn, provider)

	if err := m.MigrateUp(ctx); err != nil {
		fmt.Println("migrate up:", err)
		return
	}
	if err := m.MigrateDown(ctx); err != nil {
		fmt.Println("migrate down:", err)
		return
	}

	fmt.Println("current version:", must.Must(m.GetCurrentVersion(ctx)))

	// Output:
	// current version: 1
}

// ExampleMigrator_GetMigrationStatus reads the status an embedder actually
// renders: after applying one of two registered migrations
// (MigrateUpOptions.Amount limits the run), the report names the current
// version, what is applied, what is still pending, and whether work remains.
func ExampleMigrator_GetMigrationStatus() {
	ctx := context.Background()
	conn, cleanup := exampleSQLiteConnection()
	defer cleanup()

	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);"),
		},
		"0000000001_create_users.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;"),
		},
		"0000000002_create_posts.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE posts (id INTEGER PRIMARY KEY);"),
		},
		"0000000002_create_posts.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE posts;"),
		},
	}

	m := must.Must(migrator.NewFSMigrator(conn, fsys))
	if err := m.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{Amount: 1}); err != nil {
		fmt.Println("migrate up:", err)
		return
	}

	status, err := m.GetMigrationStatus(ctx)
	if err != nil {
		fmt.Println("get status:", err)
		return
	}
	fmt.Println("current version:", status.CurrentVersion)
	fmt.Println("applied:", status.AppliedMigrations)
	fmt.Println("pending:", status.PendingMigrations)
	fmt.Println("total migrations:", status.TotalMigrations)
	fmt.Println("has pending changes:", status.HasPendingChanges)

	// Output:
	// current version: 1
	// applied: [1]
	// pending: [2]
	// total migrations: 2
	// has pending changes: true
}

// ExampleWithStatementObserver installs the auditing hook for tools that need
// to see every successfully executed statement without taking over execution:
// the observer receives structured source and statement metadata after each
// statement runs, but no connection handle, so it cannot alter the migration
// path.
func ExampleWithStatementObserver() {
	ctx := context.Background()
	conn, cleanup := exampleSQLiteConnection()
	defer cleanup()

	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE INDEX idx_users_id ON users(id);"),
		},
		"0000000001_create_users.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;"),
		},
	}

	observer := migrator.StatementObserverFunc(func(_ context.Context, event migrator.StatementEvent) error {
		fmt.Printf("%s %d/%d: %s\n", event.SourcePath, event.Index, event.Total, event.Statement)
		return nil
	})
	provider := must.Must(migrator.NewFSMigrationProvider(fsys, migrator.WithStatementObserver(observer)))

	m := migrator.NewMigrator(conn, provider)
	if err := m.MigrateUp(ctx); err != nil {
		fmt.Println("migrate up:", err)
		return
	}

	// Output:
	// 0000000001_create_users.up.sql 1/2: CREATE TABLE users (id INTEGER PRIMARY KEY)
	// 0000000001_create_users.up.sql 2/2: CREATE INDEX idx_users_id ON users(id)
}

// ExampleParseChecks parses the pre-migration assertions a migration body
// declares, with no database involved. Checks are an ordered list rather than
// a merged map, a quoted assert value can carry spaces and equals signs, and a
// malformed directive is a hard error instead of a silently skipped safety
// gate.
func ExampleParseChecks() {
	sql := `-- +ptah check name=users_exist assert="SELECT COUNT(*) > 0 FROM users"
-- +ptah check name=emails_unique assert="SELECT COUNT(*) = COUNT(DISTINCT email) FROM users"
ALTER TABLE users ADD COLUMN verified BOOLEAN;`

	checks := must.Must(migrator.ParseChecks(sql, "postgres"))
	for _, check := range checks {
		fmt.Printf("%s | %s | on_fail=%s\n", check.Name, check.Assert, check.OnFail)
	}

	_, err := migrator.ParseChecks("-- +ptah check name=broken\nSELECT 1;", "postgres")
	fmt.Println("error:", err)

	// Output:
	// users_exist | SELECT COUNT(*) > 0 FROM users | on_fail=abort
	// emails_unique | SELECT COUNT(*) = COUNT(DISTINCT email) FROM users | on_fail=abort
	// error: +ptah check requires a non-empty assert predicate
}

// ExampleResolveAtlasDirectiveTxMode resolves a file's `-- atlas:txmode`
// directive against the migrator's global transaction mode as pure logic: the
// directive overrides the global mode, except under all, where the combination
// is refused with an error errors.As can identify as a
// migrationfile.AtlasTxModeDirectiveError.
func ExampleResolveAtlasDirectiveTxMode() {
	// A file directive overrides the global per-file default.
	mode := must.Must(migrator.ResolveAtlasDirectiveTxMode(
		migrator.MigrationTxModeFile, migrationfile.FileTxModeNone, "20240101_create_index.sql"))
	fmt.Println("global file + directive none:", mode)

	// A file with no directive inherits the global mode.
	mode = must.Must(migrator.ResolveAtlasDirectiveTxMode(
		migrator.MigrationTxModeNone, migrationfile.FileTxModeUnspecified, "20240102_add_column.sql"))
	fmt.Println("global none + no directive:", mode)

	// Under global all, one transaction spans the whole run, so a per-file
	// directive cannot be honored and the combination is refused.
	_, err := migrator.ResolveAtlasDirectiveTxMode(
		migrator.MigrationTxModeAll, migrationfile.FileTxModeNone, "20240101_create_index.sql")
	var directiveErr *migrationfile.AtlasTxModeDirectiveError
	fmt.Println("directive error:", errors.As(err, &directiveErr))
	fmt.Println(err)

	// Output:
	// global file + directive none: none
	// global none + no directive: none
	// directive error: true
	// cannot set txmode directive to "none" in "20240101_create_index.sql" when txmode "all" is set globally
}

// Example demonstrates the three sources a migrator takes its migrations from:
// an embedded filesystem, a directory on disk, and migrations built in memory.
// Every one of them produces a MigrationProvider, and NewMigrator turns any
// provider into a migrator. A real caller passes a connection from
// dbschema.ConnectToDatabase where this example passes nil.
func Example_migrationSources() {
	// Source 1: an embedded filesystem. The migration files must sit at the root
	// of the fs.FS the provider is handed, so a subdirectory is peeled off first.
	embeddedFS := must.Must(fs.Sub(examplemigrations.GetExampleMigrations(), "migrations"))
	embedded := must.Must(migrator.NewFSMigrationProvider(embeddedFS))

	// Source 2: a directory on disk, read through os.DirFS.
	dir, removeDir := exampleMigrationDir()
	defer removeDir()
	onDisk := must.Must(migrator.NewFSMigrationProvider(os.DirFS(dir)))

	// Source 3: migrations built in memory, with no files at all.
	inMemory := migrator.NewRegisteredMigrationProvider()
	inMemory.Register(migrator.CreateMigrationFromSQL(
		1,
		"Create users table",
		"CREATE TABLE users (id SERIAL PRIMARY KEY);",
		"DROP TABLE users;",
	))

	// NewFSMigrator is the shorthand for the two filesystem cases: it builds the
	// provider and the migrator in one call.
	shorthand := must.Must(migrator.NewFSMigrator(nil, embeddedFS))

	fmt.Printf("Embedded migrations: %d\n", len(embedded.Migrations()))
	fmt.Printf("On-disk migrations: %d\n", len(onDisk.Migrations()))
	fmt.Printf("In-memory migrations: %d\n", len(inMemory.Migrations()))
	fmt.Printf("Migrator built from a provider: %t\n", migrator.NewMigrator(nil, embedded) != nil)
	fmt.Printf("Migrator built by NewFSMigrator: %t\n", shorthand != nil)

	// Output:
	// Embedded migrations: 3
	// On-disk migrations: 1
	// In-memory migrations: 1
	// Migrator built from a provider: true
	// Migrator built by NewFSMigrator: true
}

// exampleMigrationDir writes one up/down migration pair into a fresh temporary
// directory and returns the directory together with its cleanup. It exists so
// the example above can show os.DirFS against a real directory instead of
// describing one in a comment.
func exampleMigrationDir() (dir string, cleanup func()) {
	dir = must.Must(os.MkdirTemp("", "ptah-migrator-example"))
	writeExampleMigration(dir, "0000000001_create_orders.up.sql", "CREATE TABLE orders (id SERIAL PRIMARY KEY);")
	writeExampleMigration(dir, "0000000001_create_orders.down.sql", "DROP TABLE orders;")
	return dir, func() { _ = os.RemoveAll(dir) }
}

func writeExampleMigration(dir, name, sql string) {
	must.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600))
}

// ExampleNewRegisteredMigrationProvider registers programmatic migrations in
// memory, with no files at all; Migrations returns them sorted by version.
// Register also accepts hand-built Migration values whose Up and Down are Go
// functions rather than SQL.
func ExampleNewRegisteredMigrationProvider() {
	provider := migrator.NewRegisteredMigrationProvider()

	provider.Register(migrator.CreateMigrationFromSQL(
		20240101120000,
		"Create users table",
		"CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE);",
		"DROP TABLE users;",
	))
	provider.Register(migrator.CreateMigrationFromSQL(
		20240101130000,
		"Add users index",
		"CREATE INDEX idx_users_email ON users(email);",
		"DROP INDEX IF EXISTS idx_users_email;",
	))

	fmt.Printf("Registered %d migrations\n", len(provider.Migrations()))
	fmt.Printf("First migration: v%d - %s\n",
		provider.Migrations()[0].Version,
		provider.Migrations()[0].Description)

	// Output:
	// Registered 2 migrations
	// First migration: v20240101120000 - Create users table
}

// ExampleNewFSMigrator_errorHandling shows the constructor refusing an
// incomplete directory: a version missing its up or down half fails loading,
// before anything touches a database.
func ExampleNewFSMigrator_errorHandling() {
	// Create a filesystem with incomplete migrations (missing down file)
	incompleteFS := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);"),
		},
		// Missing down file - this will cause validation to fail
	}

	m, err := migrator.NewFSMigrator(nil, incompleteFS)
	if err != nil {
		fmt.Printf("Failed to create migrator: %v\n", err)
		return
	}

	// This won't be reached due to validation error
	fmt.Printf("Migrator created successfully: %v\n", m != nil)

	// Output:
	// Failed to create migrator: incomplete migrations found (missing up or down files): [1]
}
