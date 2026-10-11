// Package pglint holds the migration lint rules the PostgreSQL owner checks:
// the PG family of PostgreSQL hazards, the online-mode rules ON101 and ON103,
// and the TX family, which judges the transaction shape of a migration file.
//
// Rules publishes them as data and Service checks them, once per lint run.
// The bundled runtime ptah.run/engine/builtin registers both on the postgres
// target, and ptah.run/migration/lint applies its policy to the findings:
// suppression, severity overrides, excluded paths, the online mode and the
// gate. The rules that read the schema state a version starts from name the
// statements they want it for in the answer, as ptah.run/core/lintext
// describes.
package pglint

import (
	"cmp"
	"context"
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
)

// Rules returns the rules the PostgreSQL owner publishes, in code order. Each
// call returns a fresh slice.
func Rules() []lintext.Rule {
	return rules().Published()
}

// Service checks the PostgreSQL owner's rules. Its zero value is ready to
// use.
type Service struct{}

// CheckMigrations judges the request's files under the codes it enables. A
// code this owner does not publish wraps lintext.ErrInvalid, and cancellation
// returns the context's error; either returns no result.
func (Service) CheckMigrations(ctx context.Context, request lintext.CheckRequest) (lintext.CheckResult, error) {
	return rules().CheckMigrations(ctx, request)
}

// rules are the owner's rules and the Go functions behind them, in code
// order.
func rules() lintkit.Rules {
	all := lintkit.Rules{
		definerRoutineWithoutSearchPathRule(),
		setNotNullRule(),
		postgresInPlaceRule(),
		postgresOnlineRule(),
		postgresOnlineTransactionRule(),
	}
	all = append(all, postgresRules()...)
	all = append(all, postgresCostRules()...)
	all = append(all, atlasGapPostgresRules()...)
	all = append(all, transactionRules()...)
	slices.SortFunc(all, func(a, b lintkit.Rule) int { return cmp.Compare(a.Code, b.Code) })
	return all
}
