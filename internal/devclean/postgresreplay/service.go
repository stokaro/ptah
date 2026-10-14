// Package postgresreplay is the PostgreSQL family's migration replay guard:
// it refuses a statement on PostgreSQL, CockroachDB, YugabyteDB or Spanner
// whose effects cannot be confined to the dev database, and on a server the
// run owns it lifts the refusals whose only reason is the rest of the server.
// internal/devclean asks it through the internal/devclean/replay contract.
package postgresreplay

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// Services returns the replay guard of each dialect of the PostgreSQL family.
// They share one reading of the SQL; CockroachDB and YugabyteDB each refuse
// a few operations of their own besides.
func Services() []replay.Service {
	dialects := []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner}
	services := make([]replay.Service, 0, len(dialects))
	for _, dialect := range dialects {
		services = append(services, replay.Service{Dialect: dialect, Guard: guard{dialect: dialect}})
	}
	return services
}

type guard struct {
	dialect string
}

// Validate refuses a statement whose effects cannot be confined to scope.
func (g guard) Validate(stmt string, scope replay.Scope) error {
	tokens := replay.Tokens(stmt, lexerOptions())
	if scope.Server && postgresServerWideOperation(tokens, scope) != "" {
		return validatePostgresServerWideStatement(g.dialect, tokens)
	}
	return validatePostgresReplayStatement(g.dialect, tokens)
}

// lexerOptions reads the family's standard-conforming string literals.
func lexerOptions() lexer.Options {
	return lexer.Options{StandardStrings: true}
}
