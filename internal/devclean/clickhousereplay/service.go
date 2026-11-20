// Package clickhousereplay is the ClickHouse migration replay guard: it
// refuses a statement whose effects cannot be confined to the dev database,
// such as ON CLUSTER, a table engine or a dictionary source that reaches
// outside it, or a write to another database, and on a run that trusts its SQL
// lifts the refusals whose only reason is confinement. internal/devclean asks
// it through the internal/devclean/replay contract.
package clickhousereplay

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Services returns the ClickHouse replay guard.
func Services() []replay.Service {
	return []replay.Service{{Dialect: platform.ClickHouse, Guard: guard{}}}
}

type guard struct{}

// Validate refuses a statement whose effects cannot be confined to the dev
// database scope names. No realm widens what it accepts; a run that trusts its
// SQL lifts the refusals whose only reason is confinement, see
// [validateClickHouseReplayStatement].
func (guard) Validate(stmt string, scope replay.Scope) error {
	return validateClickHouseReplayStatement(replay.Tokens(stmt, lexerOptions()), scope)
}

// lexerOptions reads backslash escapes in string literals, and the comment
// markers the ClickHouse descriptor lists.
func lexerOptions() lexer.Options {
	return dialectlexer.Options(platform.ClickHouse)
}
