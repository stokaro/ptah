package migrator

import (
	"context"
	_ "embed"
	"fmt"
	"io/fs"
	"strings"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
)

//go:embed base/schema.sql
var migrationsSchemaSQL string

//go:embed base/get_version.sql
var getVersionSQL string

//go:embed base/record_migration.sql
var recordMigrationSQL string

//go:embed base/delete_migration.sql
var deleteMigrationSQL string

// MigrationFunc represents a migration function that operates on a database connection
type MigrationFunc func(context.Context, *dbschema.DatabaseConnection) error

// SplitSQLStatements splits a SQL string into individual statements using AST-based parsing.
// This is needed because MySQL doesn't handle multiple statements in a single ExecuteSQL call.
// Unlike simple string splitting, this properly handles semicolons within string literals and comments.
func SplitSQLStatements(sql string) []string {
	return sqlutil.SplitSQLStatements(sqlutil.StripComments(sql))
}

// MigrationFuncFromSQLFilename returns a migration function that reads SQL from a file
// in the provided filesystem and executes it using the database connection
func MigrationFuncFromSQLFilename(filename string, fsys fs.FS) MigrationFunc {
	return func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		sql, err := fs.ReadFile(fsys, filename)
		if err != nil {
			return fmt.Errorf("failed to read migration file: %w", err)
		}

		// Split SQL into individual statements for better MySQL compatibility
		statements := SplitSQLStatements(string(sql))

		// Execute each statement separately
		for _, stmt := range statements {
			if err := conn.Writer().ExecuteSQL(stmt); err != nil {
				return fmt.Errorf("failed to execute migration SQL: %w", err)
			}
		}

		return nil
	}
}

// NoopMigrationFunc is a no-op migration function
func NoopMigrationFunc(_ctx context.Context, _conn *dbschema.DatabaseConnection) error {
	return nil
}

// Migration represents a database migration
type Migration struct {
	Version     int
	Description string
	Up          MigrationFunc
	Down        MigrationFunc
}

// CreateMigrationFromSQL creates a migration from SQL strings
// This is useful for programmatically creating migrations
func CreateMigrationFromSQL(version int, description, upSQL, downSQL string) *Migration {
	upFunc := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		return executeSQLStatements(conn, upSQL)
	}

	downFunc := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		return executeSQLStatements(conn, downSQL)
	}

	return &Migration{
		Version:     version,
		Description: description,
		Up:          upFunc,
		Down:        downFunc,
	}
}

// executeSQLStatements splits SQL into individual statements and executes them
func executeSQLStatements(conn *dbschema.DatabaseConnection, sql string) error {
	// Split SQL by semicolons and execute each statement
	statements := SplitSQLStatements(sql)

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue // Skip empty statements and comments
		}

		_, err := conn.Exec(stmt)
		if err != nil {
			return fmt.Errorf("failed to execute SQL statement: %w\nSQL: %s", err, stmt)
		}
	}

	return nil
}
