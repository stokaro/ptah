// Package postgresreplay is the PostgreSQL family's migration replay guard:
// it refuses a statement on PostgreSQL, CockroachDB or YugabyteDB whose
// effects cannot be confined to the dev database, on a server the run owns
// it lifts the refusals whose only reason is the rest of the server, and on a
// run that trusts its SQL it lifts every refusal whose only reason is
// confinement.
// internal/devclean asks it through the internal/devclean/replay contract.
package postgresreplay

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Services returns the replay guard of each dialect of the PostgreSQL family.
// They share one reading of the SQL; CockroachDB and YugabyteDB each refuse
// a few operations of their own besides.
func Services() []replay.Service {
	dialects := []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB}
	services := make([]replay.Service, 0, len(dialects))
	for _, dialect := range dialects {
		services = append(services, replay.Service{Dialect: dialect, Guard: guard{dialect: dialect}})
	}
	return services
}

type guard struct {
	dialect string
	// session is what [guard.ValidateSession] read about how the dev session
	// resolves a name written without a schema. Nil is a guard that read no
	// session; see [fallbackCatalogNamespace] and [protection.creationNamespace].
	session *sessionNames
}

// Validate refuses a statement whose effects cannot be confined to scope, and
// under [replay.Scope.TrustedSQL] one that would mislead the run or its
// cleanup; see [postgresUnconfinedOperation].
func (g guard) Validate(stmt string, scope replay.Scope) error {
	tokens, ok := decodedUnicodeIdentifiers(stmt, replay.Tokens(stmt, lexerOptions()))
	if !ok {
		return replay.Refuse(g.dialect, `a U&"..." identifier whose escapes the guard cannot decode`)
	}
	var names sessionNames
	if g.session != nil {
		names = *g.session
	}
	if scope.Server && postgresServerWideOperation(stmt, tokens, scope) != "" {
		return validatePostgresServerWideStatement(g.dialect, names, tokens, scope)
	}
	if scope.TrustedSQL && postgresUnconfinedOperation(stmt, tokens) != "" {
		return validatePostgresServerWideStatement(g.dialect, names, tokens, scope)
	}
	return validatePostgresReplayStatement(g.dialect, names, tokens, scope)
}

// lexerOptions is how PostgreSQL reads its text: standard-conforming string
// literals, and # as an operator character. A # read as a comment would hide
// the rest of its line from the guard while the server runs it.
func lexerOptions() lexer.Options {
	return dialectlexer.Options(platform.Postgres)
}
