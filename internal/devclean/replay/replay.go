// Package replay is the contract between internal/devclean's replay guard and
// the engines that guard statements for it. devclean decides how much of the
// dev server a run may change and hands an engine's [Guard] that decision as a
// [Scope]; the engine reads its own SQL and refuses, with [Refuse], what it
// cannot confine to that scope. The package also holds the token reading the
// engines share, which knows no engine.
//
// The guards of the built-in engines are listed by
// internal/devclean/replayguards. A dialect no guard serves is refused as
// unsupported.
package replay

import (
	"errors"
	"fmt"

	"ptah.run/internal/lexer"
)

// Scope is how much of the dev server one statement may change. The zero
// value confines it to the dev database.
type Scope struct {
	// Session reports that replay owns one pinned physical session and
	// discards it after every outcome. Session-local objects cannot survive
	// that boundary; it does not grant access to another session's objects.
	Session bool
	// Database is the database the dev connection selected, as
	// catalog.ServerInfo.Schema names it. It is empty when the connection
	// selected none.
	Database string
	// Server reports that the run owns the whole server: the operator
	// declared it disposable, or this process provisioned it. A role, another
	// database or a routine body cannot reach anything the run does not own.
	Server bool
	// Provisioned reports that this process provisioned the server and
	// removes it afterwards. It holds only together with Server.
	Provisioned bool
	// Databases reports that the run may change every user database of a
	// server the operator named as a whole, and nothing beyond them: a role,
	// a user, a privilege and a stored body stay refused.
	Databases bool
	// SchemaStateReturned reports that a plan rehearsal runs the statement on a
	// dev database whose every reset returns a schema it keeps to the owner,
	// access list and comment the claim recorded, and drops the rest. A schema's
	// own comment and privileges then stay in the dev database and end with the
	// run.
	SchemaStateReturned bool
	// SchemaCommentReturned reports that a migration replay runs the statement
	// on a dev database whose every reset does the same. A comment on a schema
	// then ends with the run, whoever wrote it. A privilege on a schema stays
	// refused in a replay, because it depends on the server's roles.
	SchemaCommentReturned bool
	// TrustedSQL reports that the operator trusts the SQL the run executes,
	// with the privileges of the dev connection. A guard then accepts a
	// statement it refuses only because it cannot confine the statement's
	// effects to the scope above: an opaque body such as a routine, a DO block
	// or a CALL, a role, a database or a server-level privilege, a write to
	// another database, and data sent to or read from outside the server.
	//
	// A body is opaque to the guard however it is written, and a DO block can
	// create a role as well as a bare CREATE ROLE can, so the guard lifts every
	// refusal of that kind rather than a list of spellings, which would protect
	// nothing and only refuse one spelling of what it accepts in another.
	//
	// It keeps every refusal that protects the run rather than the server:
	// session and transaction state, the identity the session runs as, the
	// system catalogs, server configuration, an object the realm cleanup
	// cannot remove, and a TEMP object outside a session scope. It never widens
	// the scope itself: what the run cleans is the realm's, above.
	TrustedSQL bool
	// Publications is what every cleanup of the run does with the dev
	// database's publications; see [PublicationScope]. The zero value is a
	// cleanup that drops none, and a guard then refuses publication DDL.
	Publications PublicationScope
	// Survivors names the extensions and schemas that outlive every cleanup
	// of the run; see [CleanupSurvivors]. The zero value names none, and a
	// guard then treats every extension and schema as one that outlives the
	// run.
	Survivors CleanupSurvivors
	// DatabaseRoles is what every cleanup of the run does with the database
	// roles of a SQL Server dev database; see [DatabaseRoleScope]. The zero
	// value is a cleanup that drops none, and the guard then refuses a
	// database role.
	DatabaseRoles DatabaseRoleScope
}

// DatabaseRoleScope is what the cleanups of a run do with the database roles
// of a SQL Server dev database. A database role lives in the database that
// holds it, unlike a PostgreSQL role, so a cleanup that drops the ones the run
// created confines a CREATE ROLE to the dev database.
type DatabaseRoleScope struct {
	// Dropped reports that every cleanup of the run drops each database role
	// the claim did not find, because the claim listed them.
	Dropped bool
	// Kept names the database roles the claim found. They are the dev
	// database's own and outlive the run, so the drop of one would outlive it
	// too.
	Kept []string
}

// CleanupSurvivors names the extensions and schemas of the dev database that
// outlive every cleanup of the run, whoever created them: the ones the claim
// found, and the ones a cleanup keeps or creates again whether the claim found
// them or not. A comment on one of them outlives the run as well, and the next
// run on the same dev database reads it as the database's own. Every cleanup
// drops an extension or a schema under any other name, which only the run can
// have created, and the comment on it goes with it.
type CleanupSurvivors struct {
	// Recorded reports that the claim read the names, and that every cleanup
	// of the run drops each extension and each schema they do not name.
	Recorded bool
	// Extensions are the extension names, sorted.
	Extensions []string
	// Schemas are the schema names, sorted.
	Schemas []string
}

// PublicationScope is what the cleanups of a run do with the publications of
// its dev database. A publication belongs to the whole database rather than to
// a schema, so emptying the schemas leaves it, and only a cleanup that tells
// the run's publications from the dev database's own can take one away.
type PublicationScope struct {
	// Dropped reports that every cleanup of the run drops each publication the
	// claim did not find, because the claim listed them.
	Dropped bool
	// Kept names the publications the claim found. They are the dev database's
	// own, such as Supabase's supabase_realtime, and outlive the run, so a
	// change to one or its drop would outlive it too.
	Kept []string
}

// Guard refuses a statement whose effects cannot be confined to a scope, or,
// under [Scope.TrustedSQL], one that would mislead the run or its cleanup. It
// returns nil for a statement it accepts and, for one it refuses, an error
// that [Refuse] built, so devclean can word the refusal for the caller.
type Guard interface {
	Validate(stmt string, scope Scope) error
}

// Service is the guard one engine provides, under the canonical name of the
// dialect it serves.
type Service struct {
	// Dialect is the canonical dialect name, as platform.NormalizeDialect
	// returns it.
	Dialect string
	// Guard validates the dialect's statements.
	Guard Guard
}

// Tokens returns the tokens of stmt that carry meaning, read with opts:
// whitespace, comments and semicolons are left out.
func Tokens(stmt string, opts lexer.Options) []lexer.Token {
	lex := lexer.NewLexerWithOptions(stmt, opts)
	var tokens []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenSemicolon:
			continue
		case lexer.TokenEOF:
			return tokens
		default:
			tokens = append(tokens, token)
		}
	}
}

// Refuse returns the refusal of one statement: dialect is the engine whose
// guard refused it, and operation what the statement does that cannot be
// confined.
func Refuse(dialect, operation string) error {
	return &Refusal{Dialect: dialect, Operation: operation}
}

// RefuseWithRemedy is [Refuse] for a statement the author can rewrite so that
// the guard accepts it, and remedy says how.
func RefuseWithRemedy(dialect, operation, remedy string) error {
	return &Refusal{Dialect: dialect, Operation: operation, Remedy: remedy}
}

// ErrRefused is what every refusal of a replay guard matches with
// [errors.Is]: a [*Refusal] of a statement and a [*SessionRefusal] of a
// session. A failure to read the statement or the session does not match it.
var ErrRefused = errors.New("the replay guard refused the SQL")

// Refusal is the error [Refuse] returns. A caller that rehearses a baseline
// rather than replaying a migration sets Baseline on a copy, and the refusal
// says it is the baseline's.
type Refusal struct {
	// Dialect is the engine whose guard refused the statement.
	Dialect string
	// Operation is what the statement does that cannot be confined.
	Operation string
	// Baseline words the refusal as a plan rehearsal baseline's.
	Baseline bool
	// Remedy says how the author can rewrite the statement so that the guard
	// accepts it, and is empty where the guard has nothing to suggest.
	Remedy string
	// Reason says why the guard refuses the statement. Empty, the reason is
	// that its effects cannot be confined to the dev database realm.
	Reason string
}

// Is reports whether target is [ErrRefused].
func (e *Refusal) Is(target error) bool { return target == ErrRefused }

func (e *Refusal) Error() string {
	refuses, reason := "migration replay rejects", "its effects cannot be confined to the disposable database realm"
	if e.Baseline {
		refuses, reason = "rehearsal baseline refuses", "its effects cannot be confined to the dev database realm"
	}
	if e.Reason != "" {
		reason = e.Reason
	}
	refusal := fmt.Sprintf("%s %s %s because %s", e.Dialect, refuses, e.Operation, reason)
	if e.Remedy == "" {
		return refusal
	}
	return refusal + "; " + e.Remedy
}
