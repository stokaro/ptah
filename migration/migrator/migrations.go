package migrator

import (
	"context"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
)

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

type migrationExecutionMode int

const (
	migrationExecutionTransactional migrationExecutionMode = iota
	migrationExecutionNoTransaction
)

type sqlMigrationFunc func(context.Context, *dbschema.DatabaseConnection, migrationExecutionMode) error

type sqlMigrationFile struct {
	fn            sqlMigrationFunc
	timeouts      MigrationTimeouts
	noTransaction bool
}

func (f sqlMigrationFile) executionMode() migrationExecutionMode {
	if f.noTransaction {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

// DirectiveNoTransaction opts a SQL migration file out of the per-migration
// transaction. It is intended for database operations that cannot run inside a
// transaction, such as PostgreSQL enum value additions that are used by later
// statements in the same migration.
const DirectiveNoTransaction = "no_transaction"

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

		noTransaction, err := parseNoTransactionDirective(ParseFileDirectives(string(sql)))
		if err != nil {
			return fmt.Errorf("invalid migration directives in %s: %w", filename, err)
		}

		mode := migrationExecutionTransactional
		if noTransaction {
			mode = migrationExecutionNoTransaction
		}
		return executeMigrationFileSQL(ctx, conn, filename, string(sql), interceptor, mode)
	}
}

// MigrationFuncFromSQLFilenameWithTimeouts returns a migration function and any
// file-level +ptah timeout directives parsed from the top of the SQL file.
func MigrationFuncFromSQLFilenameWithTimeouts(filename string, fsys fs.FS) (MigrationFunc, MigrationTimeouts, error) {
	return MigrationFuncFromSQLFilenameWithTimeoutsAndInterceptor(filename, fsys, nil)
}

// MigrationFuncFromSQLFilenameWithTimeoutsAndInterceptor returns a migration
// function, file-level timeout directives, and optional statement-interceptor
// support for the SQL file.
func MigrationFuncFromSQLFilenameWithTimeoutsAndInterceptor(
	filename string,
	fsys fs.FS,
	interceptor StatementInterceptor,
) (MigrationFunc, MigrationTimeouts, error) {
	migrationFile, err := migrationFuncFromSQLFilenameWithMetadata(filename, fsys, interceptor)
	if err != nil {
		return nil, MigrationTimeouts{}, err
	}
	return func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		return migrationFile.fn(ctx, conn, migrationFile.executionMode())
	}, migrationFile.timeouts, nil
}

func migrationFuncFromSQLFilenameWithMetadata(
	filename string,
	fsys fs.FS,
	interceptor StatementInterceptor,
) (sqlMigrationFile, error) {
	sql, err := fs.ReadFile(fsys, filename)
	if err != nil {
		return sqlMigrationFile{}, fmt.Errorf("failed to read migration file: %w", err)
	}

	timeouts, err := parseMigrationTimeoutDirectives(string(sql))
	if err != nil {
		return sqlMigrationFile{}, err
	}

	noTransaction, err := parseNoTransactionDirective(ParseFileDirectives(string(sql)))
	if err != nil {
		return sqlMigrationFile{}, fmt.Errorf("invalid migration directives in %s: %w", filename, err)
	}

	return sqlMigrationFile{
		fn: func(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrationExecutionMode) error {
			return executeMigrationFileSQL(ctx, conn, filename, string(sql), interceptor, mode)
		},
		timeouts:      timeouts,
		noTransaction: noTransaction,
	}, nil
}

// NoopMigrationFunc is a no-op migration function
func NoopMigrationFunc(_ctx context.Context, _conn *dbschema.DatabaseConnection) error {
	return nil
}

// Migration represents a database migration
type Migration struct {
	Version      int
	Description  string
	Up           MigrationFunc
	Down         MigrationFunc
	UpTimeouts   MigrationTimeouts
	DownTimeouts MigrationTimeouts
	// NoTransaction runs the migration body and metadata update outside the
	// normal per-migration transaction. Use this only for statements that cannot
	// run transactionally; ordinary migrations should leave it false so dry-run,
	// timeout, and rollback behavior all go through the dialect writer.
	NoTransaction bool
}

func (m *Migration) executionMode() migrationExecutionMode {
	if m.NoTransaction {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

// CreateMigrationFromSQL creates a migration from SQL strings
// This is useful for programmatically creating migrations
func CreateMigrationFromSQL(version int, description, upSQL, downSQL string) *Migration {
	upNoTransaction, upDirectiveErr := parseNoTransactionDirective(ParseFileDirectives(upSQL))
	downNoTransaction, downDirectiveErr := parseNoTransactionDirective(ParseFileDirectives(downSQL))

	migration := &Migration{
		Version:       version,
		Description:   description,
		NoTransaction: upNoTransaction || downNoTransaction,
	}

	upFunc := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		if upDirectiveErr != nil {
			return fmt.Errorf("invalid up migration directives: %w", upDirectiveErr)
		}
		return executeSQLStatements(ctx, conn, upSQL, migration.executionMode())
	}

	downFunc := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		if downDirectiveErr != nil {
			return fmt.Errorf("invalid down migration directives: %w", downDirectiveErr)
		}
		return executeSQLStatements(ctx, conn, downSQL, migration.executionMode())
	}

	migration.Up = upFunc
	migration.Down = downFunc
	return migration
}

// executeSQLStatements splits SQL into individual statements and executes them
func executeSQLStatements(ctx context.Context, conn *dbschema.DatabaseConnection, sql string, mode migrationExecutionMode) error {
	// Split SQL by semicolons and execute each statement
	statements := SplitSQLStatements(sql)

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue // Skip empty statements and comments
		}

		if err := executeMigrationStatement(ctx, conn, stmt, mode); err != nil {
			return fmt.Errorf("failed to execute SQL statement: %w\nSQL: %s", err, stmt)
		}
	}

	return nil
}

func executeMigrationFileSQL(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	filename string,
	sql string,
	interceptor StatementInterceptor,
	mode migrationExecutionMode,
) error {
	// Directives live in comments, so they must be read from the raw file
	// before comment stripping, and validated before any statement runs so an
	// invalid directive leaves nothing half-applied.
	var directives map[string]string
	if interceptor != nil {
		directives = ParseFileDirectives(sql)
		if err := interceptor.ValidateDirectives(directives); err != nil {
			return fmt.Errorf("invalid migration directives in %s: %w", filename, err)
		}
	}

	statements := SplitSQLStatements(sql)
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		if interceptor != nil {
			handled, err := interceptor.ExecuteStatement(ctx, conn, stmt, directives)
			if err != nil {
				return fmt.Errorf("failed to execute migration SQL: %w", err)
			}
			if handled {
				continue
			}
		}

		if err := executeMigrationStatement(ctx, conn, stmt, mode); err != nil {
			return fmt.Errorf("failed to execute migration SQL: %w", err)
		}
	}
	return nil
}

func executeMigrationStatement(ctx context.Context, conn *dbschema.DatabaseConnection, stmt string, mode migrationExecutionMode) error {
	if mode == migrationExecutionTransactional {
		return conn.Writer().ExecuteSQL(ctx, stmt)
	}
	return executeSQLOutsideTransaction(ctx, conn, stmt)
}

func executeSQLOutsideTransaction(ctx context.Context, conn *dbschema.DatabaseConnection, sql string, args ...any) error {
	if conn.Writer().IsDryRun() {
		return conn.Writer().ExecuteSQL(ctx, sql, args...)
	}

	// Deliberate transaction escape hatch. This is used only for migrations
	// marked no_transaction, where the database rejects transactional execution
	// (for example PostgreSQL ALTER TYPE ADD VALUE followed by using that value).
	_, err := conn.ExecContext(ctx, sql, args...)
	return err
}

func parseNoTransactionDirective(directives map[string]string) (bool, error) {
	value, ok := directives[DirectiveNoTransaction]
	if !ok {
		return false, nil
	}
	noTransaction, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid +ptah %s value %q: expected true or false", DirectiveNoTransaction, value)
	}
	return noTransaction, nil
}
