// Package ydbreplay is the YDB migration replay guard: it refuses a YQL
// statement whose effects cannot be confined to the dev realm, the directory
// a replay runs in. internal/devclean asks it through the
// internal/devclean/replay contract.
package ydbreplay

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/dialectlexer"
)

// Services returns the YDB replay guard.
func Services() []replay.Service {
	return []replay.Service{{Dialect: platform.YDB, Guard: guard{}}}
}

type guard struct{}

// Validate refuses a statement whose effects cannot be confined to scope.
func (guard) Validate(stmt string, scope replay.Scope) error {
	return validateYDBReplayStatement(replay.Tokens(stmt, dialectlexer.Options(platform.YDB)), scope)
}
