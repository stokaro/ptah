// Package mysqllint holds the migration lint rules the MySQL owner checks for
// MySQL and MariaDB: the MY family of MySQL-family hazards and the online-mode
// rule ON102.
//
// Rules publishes them as data and Service checks them, once per lint run.
// The bundled runtime ptah.run/engine/builtin registers both on the mysql
// target, and each rule names the targets it judges, mariadb among them.
// ptah.run/migration/lint applies its policy to the findings: suppression,
// severity overrides, excluded paths, the online mode and the gate. The rules
// that read the schema state a version starts from name the statements they
// want it for in the answer, as ptah.run/core/lintext describes.
package mysqllint

import (
	"cmp"
	"context"
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
)

// Rules returns the rules the MySQL owner publishes, in code order. Each call
// returns a fresh slice.
func Rules() []lintext.Rule {
	return rules().Published()
}

// Service checks the MySQL owner's rules. Its zero value is ready to use.
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
	all := lintkit.Rules{mysqlInPlaceRule(), mysqlOnlineRule()}
	all = append(all, mysqlRules()...)
	all = append(all, mysqlCostRules()...)
	all = append(all, mysqlMemberRules()...)
	all = append(all, atlasGapMySQLRules()...)
	slices.SortFunc(all, func(a, b lintkit.Rule) int { return cmp.Compare(a.Code, b.Code) })
	return all
}
