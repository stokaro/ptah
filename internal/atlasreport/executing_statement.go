package atlasreport

import (
	"errors"
	"fmt"

	"ptah.run/internal/dbanswer"
	"ptah.run/internal/migrationreplay"
	"ptah.run/internal/sourcestatement"
	"ptah.run/migration/migrator"
)

// ExecutingStatementError is a migration statement the database answered with
// an error, worded as the pinned community binary v1.3.0 words it:
//
//	sql/migrate: executing statement "<statement>" from version "<version>": <answer>
//
// The statement is the one the file spells, terminator included where the
// file wrote one, quoted as Go quotes a string, so a statement that spans lines
// stays on one line. Measured on SQLite: a failed INSERT across two lines with a
// double quote in it prints `"INSERT INTO \"nosuch\"\n\tVALUES ('a\"b');"`, a
// comment above the statement is left out, one inside it is kept, and a last
// statement with no semicolon is quoted without one.
//
// The answer is the driver's message as it stands. Ptah's SQLite driver puts
// the engine's result code around SQLite's own text (`SQL logic error: ...
// (1)`) where the community binary's driver prints the text alone; that is a
// driver difference, recorded in stokaro/ptah#1196.
//
// Unwrap returns the failure the error was read from, so [errors.Is] and
// [errors.As] see the chain a native run reports.
type ExecutingStatementError struct {
	// Statement is the failed statement as the migration file spells it.
	Statement string
	// Version is the failed migration's revision identity.
	Version string
	// Answer is what the database answered.
	Answer error

	cause error
}

func (e *ExecutingStatementError) Error() string {
	return fmt.Sprintf("sql/migrate: executing statement %q from version %q: %v", e.Statement, e.Version, e.Answer)
}

func (e *ExecutingStatementError) Unwrap() error { return e.cause }

// NewApplyStatementError reads a failed apply out of err as the community
// binary reports it. migrations are the migrations the run could apply, and
// dialect names the engine.
//
// It returns nil unless a statement the database answered is what failed: a
// commit, a check, a statement refused before it ran and a failure that names
// no migration keep Ptah's own wording, because the community binary's line
// claims a statement was executed.
func NewApplyStatementError(err error, migrations []*migrator.Migration, dialect string) *ExecutingStatementError {
	failed, ok := errors.AsType[*migrator.FailedMigrationError](err)
	if !ok {
		return nil
	}
	statement, ok := errors.AsType[*migrator.MigrationExecutionError](failed.Err)
	if !ok || !statement.Sent() {
		return nil
	}
	written := statement.Statement
	for _, migration := range migrations {
		if migration != nil && migration.RevisionVersion() == failed.VersionKey {
			written = sourcestatement.Written(migration.UpSQL, dialect, statement.StatementIndex, statement.Statement)
			break
		}
	}
	return &ExecutingStatementError{
		Statement: written,
		Version:   failed.VersionKey,
		Answer:    dbanswer.Of(statement.Err),
		cause:     err,
	}
}

// NewValidateStatementError reads a failed `migrate validate` replay out of err
// as the community binary reports it:
//
//	replaying the migration directory: sql/migrate: read migration directory state: sql/migrate: executing statement ...
//
// It returns nil where [NewApplyStatementError] does.
func NewValidateStatementError(err error) error {
	failed, ok := errors.AsType[*migrationreplay.MigrationError](err)
	if !ok || failed.Statement == nil {
		return nil
	}
	executed := &ExecutingStatementError{
		Statement: failed.Statement.Written,
		Version:   failed.VersionKey,
		Answer:    failed.Statement.Answer,
		cause:     err,
	}
	return fmt.Errorf("replaying the migration directory: sql/migrate: read migration directory state: %w", executed)
}
