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

// StatementInterceptor lets an external executor take over individual
// migration statements — for example, routing ALTER TABLE statements through
// an online-DDL tool (gh-ost, pt-online-schema-change) instead of executing
// them on the migration connection.
//
// ValidateDirectives is called once per migration, before any statement runs,
// so a bad directive fails the migration cleanly with nothing applied instead
// of surfacing only when the first affected statement is reached.
//
// ExecuteStatement receives one statement (comments stripped) together with
// the file-level directives of the migration it came from (see
// ParseFileDirectives). It returns handled=true when it fully executed the
// statement itself; on handled=false the migrator executes the statement
// normally. A non-nil error aborts the migration.
type StatementInterceptor interface {
	ValidateDirectives(directives map[string]string) error
	ExecuteStatement(ctx context.Context, conn *dbschema.DatabaseConnection, stmt string, directives map[string]string) (handled bool, err error)
}

// MigrationFuncFromSQLFilename returns a migration function that reads SQL from a file
// in the provided filesystem and executes it using the database connection
func MigrationFuncFromSQLFilename(filename string, fsys fs.FS) MigrationFunc {
	return MigrationFuncFromSQLFilenameWithInterceptor(filename, fsys, nil)
}

// MigrationFuncFromSQLFilenameWithInterceptor is MigrationFuncFromSQLFilename
// with an optional StatementInterceptor consulted for every statement; nil
// behaves exactly like MigrationFuncFromSQLFilename.
func MigrationFuncFromSQLFilenameWithInterceptor(filename string, fsys fs.FS, interceptor StatementInterceptor) MigrationFunc {
	return func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		sql, err := fs.ReadFile(fsys, filename)
		if err != nil {
			return fmt.Errorf("failed to read migration file: %w", err)
		}

		// Directives live in comments, so they must be read from the raw
		// file before comment stripping, and validated before any statement
		// runs so an invalid directive leaves nothing half-applied.
		var directives map[string]string
		if interceptor != nil {
			directives = ParseFileDirectives(string(sql))
			if err := interceptor.ValidateDirectives(directives); err != nil {
				return fmt.Errorf("invalid migration directives in %s: %w", filename, err)
			}
		}

		// Split SQL into individual statements for better MySQL compatibility
		statements := SplitSQLStatements(string(sql))

		// Execute each statement separately
		for _, stmt := range statements {
			if interceptor != nil {
				handled, err := interceptor.ExecuteStatement(ctx, conn, stmt, directives)
				if err != nil {
					return fmt.Errorf("failed to execute migration SQL: %w", err)
				}
				if handled {
					continue
				}
			}
			if err := conn.Writer().ExecuteSQL(ctx, stmt); err != nil {
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
		return executeSQLStatements(ctx, conn, upSQL)
	}

	downFunc := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		return executeSQLStatements(ctx, conn, downSQL)
	}

	return &Migration{
		Version:     version,
		Description: description,
		Up:          upFunc,
		Down:        downFunc,
	}
}

// executeSQLStatements splits SQL into individual statements and executes them
func executeSQLStatements(ctx context.Context, conn *dbschema.DatabaseConnection, sql string) error {
	// Split SQL by semicolons and execute each statement
	statements := SplitSQLStatements(sql)

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue // Skip empty statements and comments
		}

		// Use conn.ExecContext() instead of conn.Writer().ExecuteSQL() to execute
		// each statement in its own transaction. This is critical for PostgreSQL
		// enum safety — PostgreSQL prevents using newly added enum values within
		// the same transaction where they were added. By using separate
		// transactions, enum modifications and subsequent usage (like setting
		// defaults) work correctly.
		// See: https://www.postgresql.org/docs/current/sql-altertype.html
		_, err := conn.ExecContext(ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed to execute SQL statement: %w\nSQL: %s", err, stmt)
		}
	}

	return nil
}
