// Package sqlserverbatch reads T-SQL text into the statements SQL Server runs
// from it, finding each statement's end where the server finds it.
//
// T-SQL needs no separator between statements. `ALTER TABLE t ADD DEFAULT 1
// FOR c DROP DATABASE other` is two statements, and the server runs both, so a
// reader that judges a statement by its first word sees an ALTER TABLE and
// misses the DROP. A statement ends at a semicolon, and also where a word that
// can only start a statement follows a word that can end one. Control flow
// nests: an IF runs one statement, a BEGIN ... END block runs several, and the
// reader returns each with the statements it runs.
//
// Four callers read T-SQL statements: the statement splitter in
// ptah.run/core/sqlutil, which cuts a script into the statements the migrator
// sends; the safety judgment in ptah.run/migration/safety and the dev replay
// guard in internal/devclean/sqlserverreplay, which judge each statement the
// server runs; and the routine body reader in
// ptah.run/dialect/mssql/mssqlroutine. They share this package because two
// readers agree when the second is written and stop agreeing when the first
// one changes, and a judge that splits text differently from the server
// judges statements that never run and misses ones that do.
//
// The reader fails closed. Text whose statement boundaries it cannot find,
// such as an END that closes no block or a statement that starts with a word
// no statement starts with, is an [*UnreadableError], and the guard refuses
// it.
// The boundaries were measured on SQL Server 2022 and 2025 through
// go-mssqldb; where the server refuses a batch outright, so that none of it
// runs, the reader may read it either way.
package sqlserverbatch

import (
	"errors"
	"fmt"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
)

// Reads reports whether the text of dialect is T-SQL, the language [Read]
// reads: the dialect's descriptor names Transact-SQL bodies
// ([descriptor.BodiesTransactSQL]). The statement splitter of
// ptah.run/core/sqlutil cuts such a dialect's scripts where [Read] ends each
// statement, and ptah.run/migration/safety judges the statements [Read] finds
// in each piece, so the statement Ptah sends and the statements it judges in
// it are found by one reader. A blank or unknown dialect reads as false.
func Reads(dialect string) bool {
	return transactSQL.Get(dialect)
}

// transactSQL holds, per dialect, whether its descriptor names Transact-SQL
// bodies.
var transactSQL = descriptortable.New(func(described descriptor.Dialect) bool {
	return described.Scripts.Bodies == descriptor.BodiesTransactSQL
})

// ErrUnreadable is what the error of [Read] matches with [errors.Is] when it
// cannot find where a statement of the text ends.
var ErrUnreadable = errors.New("cannot find the T-SQL statement boundaries")

// UnreadableError is the error [Read] returns for text whose statement
// boundaries it cannot find.
type UnreadableError struct {
	// Reason says what the reader found, such as "an END that closes no
	// BEGIN".
	Reason string
	// Offset is where in the text the reader found it.
	Offset int
}

// Error returns [ErrUnreadable]'s text, the reason and the offset.
func (e *UnreadableError) Error() string {
	return fmt.Sprintf("%s: %s at offset %d", ErrUnreadable, e.Reason, e.Offset)
}

// Unwrap returns [ErrUnreadable].
func (e *UnreadableError) Unwrap() error {
	return ErrUnreadable
}

// Kind is what a statement is, as far as finding the statements it runs
// needs to know.
type Kind int

const (
	// Simple is a statement that runs as written.
	Simple Kind = iota
	// Block is BEGIN ... END, BEGIN TRY ... END TRY BEGIN CATCH ... END
	// CATCH, or BEGIN ATOMIC ... END. Its Statements are the ones it runs, the
	// CATCH block's after the TRY block's.
	Block
	// If is an IF and the one statement it runs, with the one an ELSE runs
	// after it. Its Head is IF and the condition.
	If
	// While is a WHILE and the one statement it runs. Its Head is WHILE and the
	// condition.
	While
	// Module is CREATE, ALTER or CREATE OR ALTER of a procedure, function,
	// trigger or view. Its body is the rest of the batch, which the server
	// stores rather than runs, so the statement ends where the text does.
	Module
	// Call is a procedure named with no EXEC in front of it. The server reads
	// one only as the first statement of a batch, and runs it as EXEC of the
	// same name.
	Call
	// Label is a label, such as `retry:`, that a GOTO jumps to. It runs
	// nothing.
	Label
	// Schema is CREATE SCHEMA and the schema elements after it. The server
	// reads the rest of the batch as its elements: CREATE TABLE, CREATE
	// VIEW, GRANT, REVOKE and DENY run in the new schema, and any other
	// statement after it, with or without a semicolon between them, fails the
	// batch with Msg 156. Its Head is CREATE SCHEMA and the name, and its
	// Statements are the elements. Only a statement of the batch itself is
	// one: inside a block or after IF the server refuses CREATE SCHEMA, and the
	// reader reads it as a [Simple] statement.
	Schema
)

// Statement is one statement the server runs.
type Statement struct {
	// Kind is what the statement is.
	Kind Kind
	// SQL is the statement's text, from its first word to its last, with the
	// semicolon that ends it. A statement that runs others carries their text
	// too.
	SQL string
	// Start and End are the offsets of SQL in the text [Read] read.
	Start, End int
	// Head is the part of the statement that is not one of its Statements:
	// IF or WHILE and its condition, CREATE SCHEMA and the name, or an INSERT
	// whose rows an EXEC returns, up to the EXEC. It is SQL for a statement
	// that runs no other, and empty for a block.
	Head string
	// Statements are the statements this one runs, in the order the text
	// writes them: a block's, the statement an IF runs and the one its ELSE
	// runs, the statement a WHILE runs, the elements of CREATE SCHEMA, and the
	// EXEC whose rows an INSERT stores, which the server runs as a statement
	// of its own.
	Statements []Statement
	// KeepsSemicolon reports that the semicolon SQL ends with is one the
	// server requires: the statement is a MERGE, or an IF or a WHILE whose
	// last statement is one. A MERGE without it fails the batch with Msg
	// 10713, so text that sends the statement on its own keeps it.
	KeepsSemicolon bool
}

// Read reads text, which is one batch, into the statements SQL Server runs
// from it, in order. Text with nothing but whitespace, comments and
// semicolons has none. For text whose statement boundaries the reader cannot
// find, the error is an [*UnreadableError] with the offset of the word it
// stopped at.
func Read(text string) ([]Statement, error) {
	p, err := newParser(text)
	if err != nil {
		return nil, err
	}
	statements, next, err := p.list(0, true)
	if err != nil {
		return nil, err
	}
	if next < len(p.tokens) {
		if p.isElse(next) {
			return nil, p.unreadable(next, "an ELSE that follows no IF")
		}
		return nil, p.unreadable(next, "an END that closes no BEGIN")
	}
	return statements, nil
}
