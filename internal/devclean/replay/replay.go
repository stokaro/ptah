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
	"fmt"

	"ptah.run/internal/lexer"
)

// Scope is how much of the dev server one statement may change. The zero
// value confines it to the dev database.
type Scope struct {
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
}

// Guard refuses a statement whose effects cannot be confined to a scope. It
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
}

func (e *Refusal) Error() string {
	if e.Baseline {
		return fmt.Sprintf("%s rehearsal baseline refuses %s because its effects cannot be confined to the dev database realm",
			e.Dialect, e.Operation)
	}
	return fmt.Sprintf("%s migration replay rejects %s because its effects cannot be confined to the disposable database realm",
		e.Dialect, e.Operation)
}
