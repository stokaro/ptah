// Package migrationsplit carries a provider's split of a migration body from
// internal/providerhost, which asks the provider for it, to
// migration/migrator, which runs it. A provider that negotiated
// units.migration-split divides a body into the units its engine runs, and
// gives a verdict on each check assertion of the migration; the migrator
// counts, records, resumes and places those units. This package holds the
// values and the session interface the two sides share, and names no engine.
package migrationsplit

import (
	"context"
	"errors"

	"ptah.run/catalog"
)

// Placement says where the migrator runs a unit.
type Placement uint8

const (
	// WithCheckpoint is a unit that changes data only. The migrator runs it
	// in a serializable transaction of its own, writes its checkpoint in the
	// same transaction and commits both together.
	WithCheckpoint Placement = iota + 1
	// Outside is a unit the engine runs outside any transaction. The
	// migrator marks it in flight, runs it and checkpoints it.
	Outside
)

// Unit is one unit a body runs as.
type Unit struct {
	// SQL is the text the provider executes, with every definition and
	// setting it needs carried in from earlier statements.
	SQL string
	// Source is the unit's own statements as written. Partial hashes and
	// failure records read it.
	Source string
	// Placement says where the unit runs.
	Placement Placement
}

// Refusal is a body the provider cannot run as units.
type Refusal struct {
	// Start and End are the byte range of the refused text in the body.
	Start, End int
	// Reason is one line that quotes nothing from the body.
	Reason string
}

// Verdict is the provider's answer for one check assertion.
type Verdict struct {
	// ReadOnly reports that the assertion is one statement that changes
	// nothing.
	ReadOnly bool
	// Reason says why it is not, on one line. It is empty when ReadOnly is
	// set.
	Reason string
}

// Split is the provider's answer for one body and its assertions.
type Split struct {
	// Units are in run order. They are empty with a refusal, and for a body
	// with no statement.
	Units []Unit
	// Refusal is set when the provider cannot run the body as units.
	Refusal *Refusal
	// Verdicts hold one verdict per assertion, in the order asked.
	Verdicts []Verdict
}

// ErrTransactionAborted reports a transaction the engine aborted for a
// conflict with another one, having applied nothing. Only a unit transaction
// returns it, from the unit's execution or from the commit, and a new attempt
// may succeed.
var ErrTransactionAborted = errors.New("the engine aborted the transaction for a conflict and applied nothing")

// Splitter is the schema writer of a session whose owner splits migration
// bodies: an executable provider's session, when it negotiated
// units.migration-split.
type Splitter interface {
	// SplitsMigrations reports whether the owner negotiated the feature. The
	// other methods refuse when it did not.
	SplitsMigrations() bool
	// SplitMigration asks for the units of body and the verdicts on
	// assertions. The answer reads the text alone, so one body has one
	// answer; an implementation may keep it and answer a repeated question
	// without asking again. An empty body has no unit and is not sent.
	SplitMigration(ctx context.Context, body string, assertions []string) (Split, error)
	// BeginUnitTransaction opens a serializable transaction that runs one
	// WithCheckpoint unit and the history write that records it. It needs no
	// transactional DDL: the unit changes data only. Its executor accepts
	// one execution, and the history store of a connection that uses it as
	// its executor writes in the transaction. A commit or an execution the
	// engine aborted for a conflict returns an error that matches
	// ErrTransactionAborted.
	BeginUnitTransaction(ctx context.Context) (catalog.SchemaTransaction, error)
}
