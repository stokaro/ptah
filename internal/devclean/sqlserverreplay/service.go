// Package sqlserverreplay is the SQL Server migration replay guard: it
// refuses a T-SQL statement whose effects cannot be confined to the dev
// database, such as USE, a remote EXECUTE, an EXEC other than the sp_rename
// calls and default batches the renderer writes, a server-scoped object or a
// temporary one, a name of another database, a read included, a DBCC, and a
// function that reads a file on the server. On a run that trusts its SQL it
// lifts the refusals whose only reason is confinement. internal/devclean asks
// it through the internal/devclean/replay contract, and reads the dev
// session's database for it once; see [guard.ValidateSession].
//
// T-SQL needs no separator between statements, so the guard reads the text it
// is given into the statements the server runs from it, with
// internal/sqlserverbatch, and judges each one. Text whose statement
// boundaries the reader cannot find is refused, trusted or not.
package sqlserverreplay

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlserverbatch"
)

// Services returns the SQL Server replay guard. It has read no session, so
// it takes no database part of a name for the dev database; devclean asks it
// for the guard of the dev session with [guard.ValidateSession].
func Services() []replay.Service {
	return []replay.Service{{Dialect: platform.SQLServer, Guard: guard{}}}
}

// guard judges the statements of a run on a dev session in database, the
// name DB_NAME() returned on that session. A guard that read no session has
// no database.
type guard struct {
	database string
}

// databaseQuery reads the database the dev session is in.
const databaseQuery = "SELECT DB_NAME()"

// ValidateSession reads the database the dev session is in, and returns the
// guard that takes a name of that database for a local one. It refuses no
// session. The database is read under trust too, where the guard does not use
// it, so that every run reads the session the same way.
//
// catalog.ServerInfo.Schema, which devclean gives as [replay.Scope.Database],
// names the schema a SQL Server connection writes to, such as dbo, not its
// database. A session cannot leave its database during the run, since USE is
// refused, so the name read once holds for every statement the run executes.
func (guard) ValidateSession(ctx context.Context, session replay.Session, _ replay.Scope) (replay.Guard, error) {
	var database string
	if err := session.QueryRowContext(ctx, databaseQuery).Scan(&database); err != nil {
		return nil, fmt.Errorf("read the database of the dev session: %w", err)
	}
	return guard{database: database}, nil
}

// Validate refuses a statement whose effects cannot be confined to the dev
// database. No realm widens what it accepts; a run that trusts its SQL lifts
// the refusals whose only reason is confinement, see
// [validateSQLServerReplayStatement].
//
// stmt is read as one batch, and every statement the server runs from it is
// judged: one after another, the statements a block, an IF or a WHILE runs,
// and the batch a constant EXEC runs. A procedure named with no EXEC, which
// the server runs as the first statement of a batch, is judged as EXEC of the
// same name.
//
// The statements are judged with scope.Database set to the database the
// guard read from the session, or to "" when it read none; see
// [guard.ValidateSession].
func (g guard) Validate(stmt string, scope replay.Scope) error {
	scope.Database = g.database
	return validateSQLServerBatch(stmt, scope, 0)
}

// unreadableRemedy says how to rewrite T-SQL whose statement boundaries the
// guard cannot read. Trust does not lift the refusal: a statement the guard
// cannot find is one it cannot judge, nor can it tell which statements the
// operator trusted.
const unreadableRemedy = "end each statement with a semicolon, and put the statements an IF, a WHILE or a block runs " +
	"between BEGIN and END; trusting the replayed SQL does not lift this refusal"

// maxDynamicDepth is how many constant EXECs, one inside another, the guard
// reads before it refuses the next one.
const maxDynamicDepth = 8

// validateSQLServerBatch judges every statement of text, a batch the server
// runs at the given depth of constant EXECs.
func validateSQLServerBatch(text string, scope replay.Scope, depth int) error {
	statements, err := sqlserverbatch.Read(text)
	if unreadable, ok := errors.AsType[*sqlserverbatch.UnreadableError](err); ok {
		return &replay.Refusal{
			Dialect:   platform.SQLServer,
			Operation: fmt.Sprintf("T-SQL at offset %d", unreadable.Offset),
			Reason:    "Ptah cannot read where its statements begin and end there: " + unreadable.Reason,
			Remedy:    unreadableRemedy,
		}
	}
	if err != nil {
		return err
	}
	return validateSQLServerStatements(statements, scope, depth)
}

func validateSQLServerStatements(statements []sqlserverbatch.Statement, scope replay.Scope, depth int) error {
	for _, statement := range statements {
		if err := validateSQLServerStatement(statement, scope, depth); err != nil {
			return err
		}
	}
	return nil
}

// validateSQLServerStatement judges one statement and the statements it runs.
// The Head of an IF, a WHILE and an INSERT whose rows an EXEC returns is
// judged as a statement of its own, so its condition or its target is read.
func validateSQLServerStatement(statement sqlserverbatch.Statement, scope replay.Scope, depth int) error {
	switch statement.Kind {
	case sqlserverbatch.Label:
		return nil
	case sqlserverbatch.Block:
		return validateSQLServerStatements(statement.Statements, scope, depth)
	case sqlserverbatch.Call:
		return validateSQLServerUnit("EXEC "+statement.SQL, scope, depth)
	}
	if err := validateSQLServerUnit(statement.Head, scope, depth); err != nil {
		return err
	}
	return validateSQLServerStatements(statement.Statements, scope, depth)
}

// validateSQLServerUnit judges one statement that runs no other.
//
// The EXEC statements the renderer writes are read first, in the shape it
// writes them; see [validateSQLServerWrittenStatement]. Read as words, a new
// name such as N'at' would be the AT of a remote EXECUTE. An EXEC of a
// constant runs that text as a batch of its own, and the guard judges the
// batch instead of refusing the EXEC; see [constantSQLServerExec].
func validateSQLServerUnit(sql string, scope replay.Scope, depth int) error {
	if !scope.TrustedSQL {
		if written, err := validateSQLServerWrittenStatement(sql, scope); written {
			return err
		}
		if batch, ok := constantSQLServerExec(sql); ok {
			if depth >= maxDynamicDepth {
				return replay.Refuse(platform.SQLServer, "EXEC nested in more than 8 constant EXECs")
			}
			return validateSQLServerBatch(batch, scope, depth+1)
		}
	}
	tokens := replay.Tokens(sql, lexerOptions())
	for len(tokens) > 0 && tokens[0].MatchOperatorValue("(") {
		tokens = tokens[1:]
	}
	return validateSQLServerReplayStatement(tokens, scope)
}

// constantSQLServerExec returns the batch an EXEC or EXECUTE of one string
// constant runs, as in the EXEC('CREATE SCHEMA [app]') the renderer writes to
// create a schema only where it is missing.
//
// A constant without N is varchar, which holds a character outside ASCII in
// the database's code page; there CONVERT reads U+FF29, a fullwidth I, as I and
// U+02BC as an apostrophe (measured on SQL Server 2025 with
// SQL_Latin1_General_CP1_CI_AS), so the batch that runs could differ from the
// one the guard reads. Such a constant reads as no batch, and the guard judges
// the EXEC as it judges every other.
func constantSQLServerExec(sql string) (string, bool) {
	tokens := replay.Tokens(sql, lexerOptions())
	if len(tokens) == 0 || !tokens[0].MatchIdentifierValue("EXEC") && !tokens[0].MatchIdentifierValue("EXECUTE") {
		return "", false
	}
	arguments := tokens[1:]
	unicode := len(arguments) == 4 && arguments[1].MatchIdentifierValue("N") && arguments[1].End == arguments[2].Start
	if unicode {
		arguments = []lexer.Token{arguments[0], arguments[2], arguments[3]}
	}
	if len(arguments) != 3 || !arguments[0].MatchOperatorValue("(") || !arguments[2].MatchOperatorValue(")") {
		return "", false
	}
	literal := arguments[1].Value
	if arguments[1].Type != lexer.TokenString || len(literal) < 2 ||
		!strings.HasPrefix(literal, "'") || !strings.HasSuffix(literal, "'") {
		return "", false
	}
	batch := strings.ReplaceAll(literal[1:len(literal)-1], "''", "'")
	if !unicode && strings.ContainsFunc(batch, func(r rune) bool { return r >= utf8.RuneSelf }) {
		return "", false
	}
	return batch, true
}

// lexerOptions reads bracketed identifiers, and a # as the start of a
// temporary name rather than of a comment.
func lexerOptions() lexer.Options {
	return dialectlexer.Options(platform.SQLServer)
}
