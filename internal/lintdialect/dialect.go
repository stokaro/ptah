// Package lintdialect defines the SQL dialects accepted by migration linting.
// It keeps lint-policy and command validation on one compatibility boundary.
package lintdialect

import (
	"go.5x5.cz/ptah/core/platform"
)

// Expected is the user-facing list of supported lint dialects.
//
// It names the canonical spelling of every supported engine. Each engine's
// documented aliases are accepted too -- see [Canonical] -- but listing all
// twenty-four spellings here would bury the nine names a reader is actually
// choosing between.
const Expected = "postgres, mysql, mariadb, sqlite, sqlserver, clickhouse, cockroachdb, yugabytedb, or spanner"

// Canonical resolves an accepted lint dialect spelling to the canonical name
// the lint engine compares against, and reports whether it is supported.
//
// Resolution is [platform.NormalizeDialect]'s, so every spelling ptah accepts
// anywhere else is accepted here: "pgx" and "postgresql" resolve to "postgres",
// "mssql" and "tsql" to "sqlserver", "crdb" to "cockroachdb". This closes a gap
// where a documented alias was refused by lint alone while `ptah generate`,
// `--dev-url` inference and the renderer all took it.
//
// Resolving rather than merely accepting is the load-bearing half. migration/lint
// selects Rule.Dialects membership and its lexer mode by exact string comparison
// and validates neither, so an alias that survived this boundary would not fail
// loudly -- lint.Options{Dialect: "pgx"} runs clean while silently matching no
// PostgreSQL rule and picking the hybrid lexer instead of the PostgreSQL one.
// Every caller must therefore store what this returns, not what it was given.
//
// The empty dialect is supported and resolves to itself: it means "run every
// dialect-independent rule".
func Canonical(dialect string) (string, bool) {
	if dialect == "" {
		return "", true
	}
	canonical := platform.NormalizeDialect(dialect)
	return canonical, canonical != ""
}

// Valid reports whether dialect is supported. The empty value means that the
// hybrid lint scanner should run every dialect-independent rule.
func Valid(dialect string) bool {
	_, ok := Canonical(dialect)
	return ok
}

// Compatible reports whether a lint policy declaring policyDialect may govern
// migrations that run against databaseDialect. An unsupported spelling on
// either side is never compatible; an empty side means "unknown", which
// constrains nothing.
//
// The comparison is by engine family, not by engine, and the two families are
// grounded differently:
//
//   - MySQL and MariaDB are interchangeable as a measured fact. Every built-in
//     MySQL-family rule lists both in Rule.Dialects and lint's scanner mode
//     treats them identically, so the two names select an identical analysis.
//     TestBuiltInRules_NoRuleSplitsTheMySQLFamily reddens if that stops holding.
//   - The PostgreSQL family is grouped as a decision, not as a measurement. Its
//     members do NOT select the same rule set: PG and TX rules name the literal
//     "postgres" and no rule names cockroachdb, yugabytedb or spanner, so those
//     databases run the dialect-independent families only. They are accepted
//     because they share PostgreSQL's wire protocol and planner family, so
//     "this directory targets PostgreSQL" is an honest description of the
//     target rather than a mistake worth blocking a deployment over.
//     TestBuiltInRules_PostgresFamilyRulesNameOnlyPostgres keeps that asymmetry
//     visible.
//
// Note what the declaration does NOT do: what gets linted is always the dialect
// the connection reports, never the policy's. Declaring "postgres" against
// CockroachDB does not turn the PG rules on, and it never did.
//
// A cross-family mismatch is a different claim. "This directory targets
// PostgreSQL" against a live MariaDB is a misconfigured --db-url or the wrong
// directory, and refusing it is cheap and high-signal. That refusal is what
// this predicate keeps.
func Compatible(policyDialect, databaseDialect string) bool {
	policy, policyOK := Canonical(policyDialect)
	if !policyOK {
		return false
	}
	// An empty policy dialect asserts nothing, so it constrains nothing. This
	// arm returns before databaseDialect is resolved on purpose: a caller that
	// knows no dialect, or knows one this package cannot resolve, must not be
	// refused for a claim its policy never made.
	if policy == "" {
		return true
	}
	database, databaseOK := Canonical(databaseDialect)
	if !databaseOK {
		return false
	}
	if database == "" {
		return true
	}
	return Family(policy) == Family(database)
}

// Family returns the lint family a canonical dialect belongs to: the group
// whose members select the same built-in rule set. Members of one family are
// interchangeable in a lint policy; members of different families are not.
//
// An unsupported or empty dialect is its own family, so it can only ever match
// itself.
func Family(canonical string) string {
	if platform.IsPostgresFamily(canonical) {
		return platform.Postgres
	}
	if canonical == platform.MariaDB {
		return platform.MySQL
	}
	return canonical
}
