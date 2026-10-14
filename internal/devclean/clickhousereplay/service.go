// Package clickhousereplay is the ClickHouse migration replay guard: it
// refuses a statement whose effects cannot be confined to the dev database,
// such as ON CLUSTER, a table engine or a dictionary source that reaches
// outside it, or a write to another database. internal/devclean asks it
// through the internal/devclean/replay contract.
package clickhousereplay

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// Services returns the ClickHouse replay guard.
func Services() []replay.Service {
	return []replay.Service{{Dialect: platform.ClickHouse, Guard: guard{}}}
}

type guard struct{}

// Validate refuses a statement whose effects cannot be confined to the dev
// database scope names. No scope widens what it accepts.
func (guard) Validate(stmt string, scope replay.Scope) error {
	return validateClickHouseReplayStatement(scope.Database, replay.Tokens(stmt, lexerOptions()))
}

// lexerOptions reads backslash escapes in string literals.
func lexerOptions() lexer.Options {
	return lexer.Options{StandardStrings: true, BackslashEscapes: true}
}
