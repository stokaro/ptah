// Package pglint holds the migration lint rules the PostgreSQL owner checks:
// the TX family, which judges the transaction shape of a migration file.
//
// Rules publishes them as data and Service checks them, once per lint run.
// The bundled runtime ptah.run/engine/builtin registers both on the postgres
// target, and ptah.run/migration/lint applies its policy to the findings:
// suppression, severity overrides, excluded paths and the gate.
package pglint

import (
	"context"

	"ptah.run/core/lintext"
	"ptah.run/core/platform"
)

// Rules returns the rules the PostgreSQL owner publishes, in code order. Each
// call returns a fresh slice.
func Rules() []lintext.Rule {
	return checks().Rules()
}

// Service checks the PostgreSQL owner's rules. Its zero value is ready to
// use.
type Service struct{}

// CheckMigrations judges the request's files under the codes it enables. A
// code this owner does not publish wraps lintext.ErrInvalid, and cancellation
// returns the context's error; either returns no result.
func (Service) CheckMigrations(ctx context.Context, request lintext.CheckRequest) (lintext.CheckResult, error) {
	return checks().CheckMigrations(ctx, request)
}

// checks are the owner's rules and the Go functions behind them.
func checks() lintext.Checks {
	return lintext.Checks{
		{
			Rule: lintext.Rule{
				Code:     "TX101",
				Title:    "transactional and non-transactional statements mixed",
				Severity: lintext.Warning,
				Targets:  []string{platform.Postgres},
			},
			// Either direction: the migrator wraps a down file in the same
			// transaction it wraps an up file in, so a rollback that mixes
			// CREATE INDEX CONCURRENTLY with transactional DDL is refused by
			// PostgreSQL exactly the same way.
			File: checkTransactionMix,
		},
		{
			Rule: lintext.Rule{
				Code:     "TX201",
				Title:    "transaction block embedded in migration",
				Severity: lintext.Warning,
				Targets:  []string{platform.Postgres},
			},
			Statement: checkEmbeddedTransaction,
		},
	}
}
