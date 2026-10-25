// Package migratorext declares the services an engine gives migration/migrator
// beyond what its dialect descriptor states: behavior a statement of data
// cannot carry, such as how to ask the engine's catalog about one of the
// migrator's own tables, how to bound a migration's waits, or how to split a
// migration body into the queries the engine runs.
//
// The migrator looks an engine's [Services] up by dialect name and takes the
// generic behavior each field names where an engine gives none. The built-in
// engines' services live in the packages below this one, one per engine
// family, and internal/migratorext/builtin lists them. This package names no
// engine.
package migratorext

import (
	"context"
	"database/sql"
	"strings"

	"ptah.run/core/dataext"
	"ptah.run/internal/filetx"
	"ptah.run/internal/lexer"
	"ptah.run/migration/migrationfile"
)

// Queryer is the connection a service reads and writes through.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) dataext.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	// Conn reserves one physical connection, which the caller closes.
	Conn(ctx context.Context) (*sql.Conn, error)
}

// Table names one of the migrator's own tables.
type Table struct {
	// ConfiguredSchema is the schema the migrator was configured with, as
	// the migrator qualifies its tables by it, or empty.
	ConfiguredSchema string
	// ConnectionSchema is the schema the connection reports.
	ConnectionSchema string
	// Name is the table's own name.
	Name string
	// Qualified is the quoted, schema-qualified name a statement uses.
	Qualified string
	// Quote quotes one identifier as the engine does.
	Quote func(string) string
}

// ConfiguredOrConnectionSchema returns the configured schema, or the
// connection's when none is configured.
func (t Table) ConfiguredOrConnectionSchema() string {
	if t.ConfiguredSchema != "" {
		return t.ConfiguredSchema
	}
	return t.ConnectionSchema
}

// Catalog asks an engine's catalog about the migrator's own tables.
type Catalog interface {
	// PresenceQuery returns the statement that counts the tables named like
	// table, with ? placeholders, and the values it binds in order.
	PresenceQuery(table Table) (query string, args []any, err error)
	// ColumnExists reports whether table has a column named column.
	ColumnExists(ctx context.Context, q Queryer, table Table, column string) (bool, error)
}

// VersionColumn widens the version column of a revision table an older
// layout created narrower than BIGINT.
type VersionColumn interface {
	WidenVersion(ctx context.Context, q Queryer, table Table) error
}

// Owners answers who owns one of the migrator's tables.
type Owners interface {
	// OwnerQuery returns the catalog query that answers, as (owner, current
	// role, whether the current role is the owner or a member of it), who
	// owns table, and the values it binds in order.
	OwnerQuery(table Table) (query string, args []any)
}

// Scope is what a migration's timeouts are attached to.
type Scope uint8

const (
	// ScopeTransaction bounds the statements of one transaction.
	ScopeTransaction Scope = iota
	// ScopeSession bounds every statement on one pinned session, for a
	// migration that runs outside a transaction.
	ScopeSession
)

// Timeouts spells the statements that bound a migration's lock waits and
// statements, and the ones that put the session back afterwards.
type Timeouts interface {
	// Statements returns them for timeouts in scope.
	Statements(timeouts migrationfile.Timeouts, scope Scope) (setup, restore []string, err error)
}

// Unit is one query a migration body runs as.
type Unit struct {
	// Text is what is sent to the server.
	Text string
	// Source is the body's text the query came from.
	Source string
	// Statements are the statements the query holds.
	Statements []string
	// Mixed reports that the query runs a schema statement and a statement
	// that reads or writes a table, which the engine refuses whole.
	Mixed bool
}

// Units splits a migration body into the queries an engine runs it as, each
// on its own, in place of statements. The migrator counts, records and
// resumes over those queries, runs a schema query outside any transaction,
// and commits a data query together with the checkpoint that records it. A
// query carries every definition and setting it needs, and the engine keeps
// no session state between queries, so a committed one leaves nothing a
// resumed run has to replay.
type Units interface {
	// Split returns the queries of sqlText, or an error for a body the engine
	// cannot run as queries.
	Split(sqlText string) ([]Unit, error)
	// IsData reports whether query reads or writes data rather than schema.
	IsData(query string) bool
}

// Checks holds what an engine adds to proving a check assertion read-only.
type Checks interface {
	// Validate refuses an assertion whose text the shared rules accept and
	// the engine cannot run read-only.
	Validate(assertion string) error
}

// ReadProver proves, before a check assertion runs, that every object it
// reads keeps its rows in the database the check is sent to, where the text
// alone cannot say so.
type ReadProver interface {
	// ReadProof returns that proof, reading what it needs over session,
	// which the caller closes once ReadProof returns.
	ReadProof(ctx context.Context, session *sql.Conn) (func(ctx context.Context, assertion string) error, error)
}

// Services is what one engine gives the migrator. A nil service, or a false
// flag, takes the generic behavior the field names.
type Services struct {
	// Dialects are the canonical names of the dialects these services serve.
	Dialects []string
	// Catalog asks the engine's catalog about the migrator's tables. Nil
	// asks information_schema for a column, by the configured schema, and
	// refuses to ask whether a table exists.
	Catalog Catalog
	// VersionColumn widens an older revision table's version column. Nil
	// leaves it as it is.
	VersionColumn VersionColumn
	// Owners answers who owns a migrator table. Nil means the engine has no
	// such question: nothing a squatter attaches to a table runs as the
	// migrating account.
	Owners Owners
	// Timeouts spells a migration's timeouts. Nil means the engine has no
	// statement for them.
	Timeouts Timeouts
	// QueryDeadlines reports that the statement timeout is a deadline the
	// migrator puts on each query rather than a setting it sends.
	QueryDeadlines bool
	// Units splits a body into the queries the engine runs it as. Nil runs a
	// body statement by statement.
	Units Units
	// Checks adds to proving check assertions read-only. Nil adds nothing.
	Checks Checks
	// ReadProof proves what a check assertion reads stays in the database.
	// Nil leaves it to the shared rules, which refuse every construct that
	// reaches another server by name.
	ReadProof ReadProver
	// ReadOnlyChecks reports that the server enforces a read-only session,
	// so a check runs in one and a statement past the static rules is still
	// refused by something other than Ptah.
	ReadOnlyChecks bool
	// PinnedMetadata keeps a no-transaction run's metadata writes on the
	// session it pins, qualified against the connection's schema, where a
	// database may have a single physical connection.
	PinnedMetadata bool
	// BlockBegin reports that BEGIN and END delimit a block, and only BEGIN
	// TRAN, BEGIN TRANSACTION or BEGIN DISTRIBUTED opens a transaction.
	BlockBegin bool
	// HashTemporaryNames reports that a name beginning with # names a
	// temporary object.
	HashTemporaryNames bool
	// AutocommitControl reports that SET autocommit controls the
	// transaction.
	AutocommitControl bool
	// Indexes guards the revision table against an index a failed build
	// left unusable. Nil means the engine leaves none.
	Indexes IndexGuard
	// OwnTransactions reads a migration body, split into statements, for the
	// transactions it opens and closes itself; see internal/filetx. Nil reads
	// none, so a migrator that runs such bodies as written wraps every body of
	// the engine as usual.
	OwnTransactions func(statements []string) (filetx.Blocks, error)
	// TriggerGrant reports that the catalog shows a table's triggers only to
	// an account holding the TRIGGER privilege, so a run that must prove a
	// table has no hidden writer proves the privilege first.
	TriggerGrant bool
}

// ContainsIdentifierSequence reports whether source holds the identifiers of
// sequence one after another, compared without regard to case, with only
// whitespace and comments between them, read with options.
func ContainsIdentifierSequence(source string, options lexer.Options, sequence ...string) bool {
	if len(sequence) == 0 {
		return false
	}
	lexr := lexer.NewLexerWithOptions(source, options)
	matched := 0
	for {
		tok := lexr.NextToken()
		switch tok.Type {
		case lexer.TokenEOF:
			return false
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		case lexer.TokenIdentifier:
			if matched < len(sequence) && strings.EqualFold(tok.Value, sequence[matched]) {
				matched++
				if matched == len(sequence) {
					return true
				}
				continue
			}
			if strings.EqualFold(tok.Value, sequence[0]) {
				matched = 1
			} else {
				matched = 0
			}
		default:
			matched = 0
		}
	}
}
