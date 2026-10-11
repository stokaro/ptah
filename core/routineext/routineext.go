// Package routineext is the contract between the hosts that analyze a routine
// body and the engine owner that reads it.
//
// A routine body is written in the engine's own language: PL/pgSQL, T-SQL,
// MySQL's compound statements. Only the engine's owner knows where a statement
// of that language ends, which statements steer others, and which build SQL at
// run time. A host that asks what a body writes, what it reads, or where static
// analysis has to stop hands the owner every body of one document in one call
// and reads the answer back as data: each statement's text, and which of four
// kinds it is. A host then reads the text of a plain statement with its own
// SQL reading, which is shared by every engine.
//
// The contract carries no Go closure, so an owner may answer it from another
// process.
package routineext

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// ErrInvalid reports a contract value that does not hold: a reading that does
// not answer its request, a statement of an unknown kind, contents on a
// statement that steers none. A host refuses such a reply rather than read part
// of it.
var ErrInvalid = errors.New("invalid routine body contract value")

// Body is one routine body to read.
type Body struct {
	// Kind is the routine kind: function, procedure, or do for an anonymous
	// block. Empty means function.
	Kind string `json:"kind,omitempty"`
	// Language is the LANGUAGE the routine states, as written, empty when it
	// states none.
	Language string `json:"language,omitempty"`
	// SQL is the body text, without the quoting it was written between.
	SQL string `json:"sql"`
}

// ReadRequest asks a target's owner to read the routine bodies of one
// document, all of them in one call.
type ReadRequest struct {
	// Target is the canonical name of the target whose language the bodies
	// are written in.
	Target string `json:"target"`
	// Bodies are the bodies to read, in the host's order.
	Bodies []Body `json:"bodies"`
}

// StatementKind is what a statement of a body does, as far as a host that
// analyzes the body needs to know.
type StatementKind string

const (
	// StatementSQL runs as written. A host reads its text for what it writes
	// and reads.
	StatementSQL StatementKind = "sql"
	// StatementInert writes no table: a declaration, RETURN, RAISE, a query
	// whose rows only fill variables. A host still reads its text for what it
	// reads. The owner decides which of its statements are inert.
	StatementInert StatementKind = "inert"
	// StatementDynamic runs a statement composed at run time, such as
	// PL/pgSQL's EXECUTE. Nothing about its text says what it reads or
	// writes, so a host's analysis of the body stops there.
	StatementDynamic StatementKind = "dynamic"
	// StatementControl steers other statements: a block, a branch, a loop.
	// Its Statements are the ones it runs, unless it is Opaque.
	StatementControl StatementKind = "control"
)

// statementKinds are the kinds this package defines.
var statementKinds = []StatementKind{StatementSQL, StatementInert, StatementDynamic, StatementControl}

// Statement is one statement of a body.
type Statement struct {
	// Kind is what the statement does.
	Kind StatementKind `json:"kind"`
	// Label is the engine's lowercase name for the statement, which a host
	// names it by in a message: if, loop, execute.
	Label string `json:"label,omitempty"`
	// SQL is the statement's text as the body writes it. A control statement
	// carries all of its text, its contents included.
	SQL string `json:"sql"`
	// Statements are the statements a control statement runs, in body order.
	// Only a control statement carries any.
	Statements []Statement `json:"statements,omitempty"`
	// Opaque marks a control statement whose contents the owner could not
	// split. Its Statements are empty, and a host reports what the contents
	// do as unknown rather than as nothing.
	Opaque bool `json:"opaque,omitempty"`
}

// Reading is an owner's answer about one body.
type Reading struct {
	// Statements are the body's statements, in body order.
	Statements []Statement `json:"statements"`
	// ColumnReads reports that a name in the body's statements that names a
	// column of a table the statement reads is that column, never a variable
	// of the same name, so a host may attribute column reads to the body. An
	// engine that resolves such a name to the variable leaves it false.
	ColumnReads bool `json:"column_reads,omitempty"`
}

// ReadResult answers a [ReadRequest].
type ReadResult struct {
	// Bodies holds one reading per body of the request, in request order.
	Bodies []Reading `json:"bodies"`
}

// Reader reads routine bodies for one dialect. A host calls it once per
// document with every body of the document.
type Reader interface {
	// ReadBodies returns one reading per body of the request, or an error and
	// no result. A target the reader does not serve wraps
	// ptaherr.ErrUnsupportedDialect.
	ReadBodies(ctx context.Context, request ReadRequest) (ReadResult, error)
}

// ValidateRead reports a result that does not answer request: a reading count
// other than the request's body count, a statement of a kind this package does
// not define, statements or Opaque on a statement that is not a control
// statement, and an Opaque statement that carries statements. Each failure
// wraps [ErrInvalid].
func ValidateRead(request ReadRequest, result ReadResult) error {
	if len(result.Bodies) != len(request.Bodies) {
		return fmt.Errorf("%w: a reading of %d bodies returned %d", ErrInvalid, len(request.Bodies), len(result.Bodies))
	}
	for i, reading := range result.Bodies {
		if err := validateStatements(reading.Statements); err != nil {
			return fmt.Errorf("body %d: %w", i, err)
		}
	}
	return nil
}

// validateStatements checks a list of statements and everything they carry.
func validateStatements(statements []Statement) error {
	for i, statement := range statements {
		switch {
		case !slices.Contains(statementKinds, statement.Kind):
			return fmt.Errorf("%w: statement %d has unknown kind %q", ErrInvalid, i, statement.Kind)
		case statement.Kind != StatementControl && (len(statement.Statements) > 0 || statement.Opaque):
			return fmt.Errorf("%w: statement %d of kind %q carries contents", ErrInvalid, i, statement.Kind)
		case statement.Opaque && len(statement.Statements) > 0:
			return fmt.Errorf("%w: statement %d is opaque and carries statements", ErrInvalid, i)
		}
		if err := validateStatements(statement.Statements); err != nil {
			return fmt.Errorf("statement %d: %w", i, err)
		}
	}
	return nil
}
