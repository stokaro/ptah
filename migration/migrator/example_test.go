package migrator_test

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"github.com/stokaro/ptah/dbschema"
	examples "github.com/stokaro/ptah/examples/migrator"
	"github.com/stokaro/ptah/migration/migrator"
)

// Example demonstrates how to use the migrator programmatically
func ExampleMigrator() {
	// This is a demonstration - in real usage you would have a valid database URL
	dbURL := "postgres://user:pass@localhost/db"

	// Connect to database
	conn, err := dbschema.ConnectToDatabase(dbURL)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Register a simple migration
	migration := &migrator.Migration{
		Version:     1,
		Description: "Create users table",
		Up: func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
			return conn.Writer().ExecuteSQL(`
				CREATE TABLE users (
					id SERIAL PRIMARY KEY,
					email VARCHAR(255) NOT NULL UNIQUE,
					created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
				)
			`)
		},
		Down: func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
			return conn.Writer().ExecuteSQL("DROP TABLE users")
		},
	}

	// Create a migrator and register all migrations with both up and down
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))

	// Run migrations
	err = m.MigrateUp(context.Background())
	if err != nil {
		fmt.Printf("Migration failed: %v\n", err)
		return
	}

	fmt.Println("Migration completed successfully")
}

// Example demonstrates how to use the filesystem-based migrator
func ExampleNewFSMigrator() {
	// This is a demonstration - in real usage you would have a valid database URL
	dbURL := "postgres://user:pass@localhost/db"

	// Connect to database
	conn, err := dbschema.ConnectToDatabase(dbURL)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Get example migrations filesystem
	exampleFS := examples.GetExampleMigrations()
	migrationsFS := must.Must(fs.Sub(exampleFS, "migrations"))

	mig, err := migrator.NewFSMigrator(conn, migrationsFS)
	if err != nil {
		fmt.Printf("Failed to create migrator: %v\n", err)
		return
	}

	// Run all migrations from the filesystem
	err = mig.MigrateUp(context.Background())
	if err != nil {
		fmt.Printf("Migration failed: %v\n", err)
		return
	}

	fmt.Println("All migrations completed successfully")
}

// Example demonstrates how to check migration status
func ExampleMigrator_GetMigrationStatus() {
	// This is a demonstration - in real usage you would have a valid database URL
	dbURL := "postgres://user:pass@localhost/db"

	// Connect to database
	conn, err := dbschema.ConnectToDatabase(dbURL)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Get example migrations filesystem
	exampleFS := examples.GetExampleMigrations()
	migrationsFS := must.Must(fs.Sub(exampleFS, "migrations"))

	mig, err := migrator.NewFSMigrator(conn, migrationsFS)
	if err != nil {
		fmt.Printf("Failed to create migrator: %v\n", err)
		return
	}

	// Get migration status
	status, err := mig.GetMigrationStatus(context.Background())
	if err != nil {
		fmt.Printf("Failed to get status: %v\n", err)
		return
	}

	fmt.Printf("Current version: %d\n", status.CurrentVersion)
	fmt.Printf("Total migrations: %d\n", status.TotalMigrations)
	fmt.Printf("Pending migrations: %d\n", len(status.PendingMigrations))
	fmt.Printf("Has pending changes: %t\n", status.HasPendingChanges)
}

// Example demonstrates how to create migrations from SQL strings
func ExampleCreateMigrationFromSQL() {
	upSQL := `
		CREATE TABLE products (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			price DECIMAL(10,2) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX idx_products_name ON products(name);
	`

	downSQL := `
		DROP INDEX IF EXISTS idx_products_name;
		DROP TABLE IF EXISTS products;
	`

	migration := migrator.CreateMigrationFromSQL(2, "Create products table", upSQL, downSQL)

	fmt.Printf("Migration version: %d\n", migration.Version)
	fmt.Printf("Migration description: %s\n", migration.Description)
	fmt.Printf("Has up function: %t\n", migration.Up != nil)
	fmt.Printf("Has down function: %t\n", migration.Down != nil)

	// Output:
	// Migration version: 2
	// Migration description: Create products table
	// Has up function: true
	// Has down function: true
}

// Example demonstrates how to register migrations from different filesystems
func Example_registerMigrationsCustomFilesystem() {
	// This is a demonstration - in real usage you would have a valid database URL
	dbURL := "postgres://user:pass@localhost/db"

	// Connect to database
	conn, err := dbschema.ConnectToDatabase(dbURL)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Option 1: Register from example migrations
	exampleFS := examples.GetExampleMigrations()
	migrationsFS := must.Must(fs.Sub(exampleFS, "migrations"))
	// Create a migrator and register all migrations with both up and down
	_, err = migrator.NewFSMigrator(conn, migrationsFS)
	if err != nil {
		fmt.Printf("Failed to register example migrations: %v\n", err)
		return
	}

	// Option 2: Register from a directory on disk
	// err = migrator.RegisterMigrationsFromDirectory(m, "/path/to/migrations")

	// Option 3: Register from a custom filesystem
	// customFS := os.DirFS("/custom/path")
	// err = migrator.RegisterMigrations(m, customFS)

	fmt.Println("Migrations registered successfully")
}

// Example demonstrates how to use RegisteredMigrationProvider for programmatic migrations
func ExampleNewRegisteredMigrationProvider() {
	// Create a provider and register multiple migrations
	provider := migrator.NewRegisteredMigrationProvider()

	// Register first migration
	migration1 := &migrator.Migration{
		Version:     20240101120000,
		Description: "Create users table",
		Up: func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
			return conn.Writer().ExecuteSQL(`
				CREATE TABLE users (
					id SERIAL PRIMARY KEY,
					email VARCHAR(255) NOT NULL UNIQUE,
					name VARCHAR(255) NOT NULL,
					created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
				)
			`)
		},
		Down: func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
			return conn.Writer().ExecuteSQL("DROP TABLE users")
		},
	}
	provider.Register(migration1)

	// Register second migration
	migration2 := migrator.CreateMigrationFromSQL(
		20240101130000,
		"Add users index",
		"CREATE INDEX idx_users_email ON users(email);",
		"DROP INDEX IF EXISTS idx_users_email;",
	)
	provider.Register(migration2)

	fmt.Printf("Registered %d migrations\n", len(provider.Migrations()))
	fmt.Printf("First migration: v%d - %s\n",
		provider.Migrations()[0].Version,
		provider.Migrations()[0].Description)

	// Output:
	// Registered 2 migrations
	// First migration: v20240101120000 - Create users table
}

// Example demonstrates migration rollback operations
func ExampleMigrator_MigrateDown() {
	// This is a demonstration - in real usage you would have a valid database URL
	dbURL := "postgres://user:pass@localhost/db"

	conn, err := dbschema.ConnectToDatabase(dbURL)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Create migrations
	provider := migrator.NewRegisteredMigrationProvider()

	migration1 := migrator.CreateMigrationFromSQL(1, "Create table",
		"CREATE TABLE test (id SERIAL PRIMARY KEY);",
		"DROP TABLE test;")
	migration2 := migrator.CreateMigrationFromSQL(2, "Add column",
		"ALTER TABLE test ADD COLUMN name VARCHAR(255);",
		"ALTER TABLE test DROP COLUMN name;")

	provider.Register(migration1)
	provider.Register(migration2)

	m := migrator.NewMigrator(conn, provider)

	// Roll back one migration (to previous version)
	err = m.MigrateDown(context.Background())
	if err != nil {
		fmt.Printf("Rollback failed: %v\n", err)
		return
	}

	fmt.Println("Successfully rolled back one migration")
}

// Example demonstrates migrating to a specific version
func ExampleMigrator_MigrateTo() {
	// This is a demonstration - in real usage you would have a valid database URL
	dbURL := "postgres://user:pass@localhost/db"

	conn, err := dbschema.ConnectToDatabase(dbURL)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Create test filesystem with migrations
	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);"),
		},
		"0000000001_create_users.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;"),
		},
		"0000000002_add_email.up.sql": &fstest.MapFile{
			Data: []byte("ALTER TABLE users ADD COLUMN email VARCHAR(255);"),
		},
		"0000000002_add_email.down.sql": &fstest.MapFile{
			Data: []byte("ALTER TABLE users DROP COLUMN email;"),
		},
		"0000000003_add_index.up.sql": &fstest.MapFile{
			Data: []byte("CREATE INDEX idx_users_email ON users(email);"),
		},
		"0000000003_add_index.down.sql": &fstest.MapFile{
			Data: []byte("DROP INDEX IF EXISTS idx_users_email;"),
		},
	}

	m, err := migrator.NewFSMigrator(conn, fsys)
	if err != nil {
		fmt.Printf("Failed to create migrator: %v\n", err)
		return
	}

	// Migrate to specific version (version 2)
	err = m.MigrateTo(context.Background(), 2)
	if err != nil {
		fmt.Printf("Migration to version 2 failed: %v\n", err)
		return
	}

	fmt.Println("Successfully migrated to version 2")
}

// Example demonstrates using custom logger with migrator
func ExampleMigrator_WithLogger() {
	// Create a custom logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create migrator with custom logger
	provider := migrator.NewRegisteredMigrationProvider()
	m := migrator.NewMigrator(nil, provider).WithLogger(logger)

	fmt.Printf("Migrator configured with custom logger: %t\n", m != nil)

	// Output:
	// Migrator configured with custom logger: true
}

// Example demonstrates checking for pending migrations
func ExampleMigrator_GetPendingMigrations() {
	// This is a demonstration - in real usage you would have a valid database URL
	dbURL := "postgres://user:pass@localhost/db"

	conn, err := dbschema.ConnectToDatabase(dbURL)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Create migrations
	provider := migrator.NewRegisteredMigrationProvider()

	migration1 := migrator.CreateMigrationFromSQL(1, "Create users",
		"CREATE TABLE users (id SERIAL PRIMARY KEY);",
		"DROP TABLE users;")
	migration2 := migrator.CreateMigrationFromSQL(2, "Create products",
		"CREATE TABLE products (id SERIAL PRIMARY KEY);",
		"DROP TABLE products;")

	provider.Register(migration1)
	provider.Register(migration2)

	m := migrator.NewMigrator(conn, provider)

	// Get pending migrations
	pending, err := m.GetPendingMigrations(context.Background())
	if err != nil {
		fmt.Printf("Failed to get pending migrations: %v\n", err)
		return
	}

	fmt.Printf("Found %d pending migrations\n", len(pending))
	for _, version := range pending {
		fmt.Printf("- Migration version: %d\n", version)
	}
}

// Example demonstrates working with migration file utilities
func ExampleParseMigrationFileName() {
	filenames := []string{
		"0000000001_create_users_table.up.sql",
		"0000000002_add_email_index.down.sql",
		"invalid_filename.sql",
	}

	for _, filename := range filenames {
		migrationFile, err := migrator.ParseMigrationFileName(filename)
		if err != nil {
			fmt.Printf("Invalid filename: %s\n", filename)
			continue
		}

		fmt.Printf("File: %s\n", filename)
		fmt.Printf("  Version: %d\n", migrationFile.Version)
		fmt.Printf("  Name: %s\n", migrationFile.Name)
		fmt.Printf("  Direction: %s\n", migrationFile.Direction)
	}

	// Output:
	// File: 0000000001_create_users_table.up.sql
	//   Version: 1
	//   Name: Create Users Table
	//   Direction: up
	// File: 0000000002_add_email_index.down.sql
	//   Version: 2
	//   Name: Add Email Index
	//   Direction: down
	// Invalid filename: invalid_filename.sql
}

// Example demonstrates generating migration filenames
func ExampleGenerateMigrationFileName() {
	// Generate filenames for a new migration
	version := 20240101120000
	description := "Add User Preferences Table"

	upFilename := migrator.GenerateMigrationFileName(version, description, "up")
	downFilename := migrator.GenerateMigrationFileName(version, description, "down")

	fmt.Printf("Up migration file: %s\n", upFilename)
	fmt.Printf("Down migration file: %s\n", downFilename)

	// Output:
	// Up migration file: 20240101120000_add_user_preferences_table.up.sql
	// Down migration file: 20240101120000_add_user_preferences_table.down.sql
}

// Example demonstrates creating migrations from filesystem with error handling
func ExampleNewFSMigrator_errorHandling() {
	// Create a filesystem with incomplete migrations (missing down file)
	incompleteFS := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);"),
		},
		// Missing down file - this will cause the migration to use NoopMigrationFunc
	}

	m, err := migrator.NewFSMigrator(nil, incompleteFS)
	if err != nil {
		fmt.Printf("Failed to create migrator: %v\n", err)
		return
	}

	// The migrator will be created but the migration will have NoopMigrationFunc for down
	migrations := m.MigrationProvider().Migrations()
	fmt.Printf("Created migrator with %d migrations\n", len(migrations))
	fmt.Printf("Migration: v%d - %s\n", migrations[0].Version, migrations[0].Description)

	// Output:
	// Created migrator with 1 migrations
	// Migration: v1 - Create Users
}

// Example demonstrates a complete migration workflow with status checking
func Example_completeWorkflow() {
	// This is a demonstration - in real usage you would have a valid database URL
	dbURL := "postgres://user:pass@localhost/db"

	conn, err := dbschema.ConnectToDatabase(dbURL)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Create a realistic set of migrations
	provider := migrator.NewRegisteredMigrationProvider()

	// Migration 1: Create users table
	migration1 := migrator.CreateMigrationFromSQL(
		20240101120000,
		"Create users table",
		`CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			email VARCHAR(255) NOT NULL UNIQUE,
			name VARCHAR(255) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`DROP TABLE IF EXISTS users;`,
	)

	// Migration 2: Create products table
	migration2 := migrator.CreateMigrationFromSQL(
		20240101130000,
		"Create products table",
		`CREATE TABLE products (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			price DECIMAL(10,2) NOT NULL,
			user_id INTEGER REFERENCES users(id),
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`DROP TABLE IF EXISTS products;`,
	)

	// Migration 3: Add indexes
	migration3 := migrator.CreateMigrationFromSQL(
		20240101140000,
		"Add performance indexes",
		`CREATE INDEX idx_users_email ON users(email);
		 CREATE INDEX idx_products_user_id ON products(user_id);
		 CREATE INDEX idx_products_created_at ON products(created_at);`,
		`DROP INDEX IF EXISTS idx_users_email;
		 DROP INDEX IF EXISTS idx_products_user_id;
		 DROP INDEX IF EXISTS idx_products_created_at;`,
	)

	provider.Register(migration1)
	provider.Register(migration2)
	provider.Register(migration3)

	m := migrator.NewMigrator(conn, provider)

	// Check initial status
	status, err := m.GetMigrationStatus(context.Background())
	if err != nil {
		fmt.Printf("Failed to get status: %v\n", err)
		return
	}

	fmt.Printf("Initial status:\n")
	fmt.Printf("  Current version: %d\n", status.CurrentVersion)
	fmt.Printf("  Total migrations: %d\n", status.TotalMigrations)
	fmt.Printf("  Pending migrations: %d\n", len(status.PendingMigrations))
	fmt.Printf("  Has pending changes: %t\n", status.HasPendingChanges)

	// Apply all migrations
	err = m.MigrateUp(context.Background())
	if err != nil {
		fmt.Printf("Migration failed: %v\n", err)
		return
	}

	// Check final status
	status, err = m.GetMigrationStatus(context.Background())
	if err != nil {
		fmt.Printf("Failed to get final status: %v\n", err)
		return
	}

	fmt.Printf("\nFinal status:\n")
	fmt.Printf("  Current version: %d\n", status.CurrentVersion)
	fmt.Printf("  Pending migrations: %d\n", len(status.PendingMigrations))
	fmt.Printf("  Has pending changes: %t\n", status.HasPendingChanges)

	fmt.Println("\nMigration workflow completed successfully!")
}
