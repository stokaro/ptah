// Package lintdialect defines the SQL dialects accepted by migration linting.
// It keeps lint-policy and command validation on one compatibility boundary.
package lintdialect

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
)

// Expected is the user-facing list of supported lint dialects.
//
// It names the canonical spelling of every supported engine. Each engine's
// documented aliases are accepted too -- see [Canonical] -- but listing every
// spelling here would bury the canonical names a reader is actually choosing
// between.
const Expected = "postgres, mysql, mariadb, sqlite, sqlserver, clickhouse, cockroachdb, yugabytedb, spanner, or ydb"

// Canonical resolves an accepted lint dialect spelling to the canonical name
// the lint engine compares against, and reports whether it is supported.
//
// Resolution is the process registry's, so every spelling ptah accepts
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
// A dialect is supported when its descriptor says lint analyzes it. "Whatever
// the registry resolves" is not enough, because a dialect Ptah knows need not
// be one lint can read: Oracle has no lint rule and no scanner mode, so a run
// that named it would exit 0 having matched no rule and scanned with the
// hybrid lexer, which is the failure described above for aliases. An engine
// becomes supported when its descriptor says so, once lint can analyze it
// (stokaro/ptah#1875).
//
// The empty dialect is supported and resolves to itself: it means "run every
// dialect-independent rule".
func Canonical(dialect string) (string, bool) {
	if dialect == "" {
		return "", true
	}
	described, found := platform.Lookup(dialect)
	if !found || !described.Lint.Analyzed {
		// The empty return is the safe half. A caller that ignores ok would
		// otherwise receive a name that selects no Rule.Dialects entry and no
		// lexer mode, and run clean against a grammar nothing parsed.
		return "", false
	}
	return described.Name, true
}

// CanonicalIn is [Canonical] for a run whose runtime may serve engines that are
// not built-in dialects, such as one an executable provider serves. A name
// Canonical refuses is supported when served registers it, and resolves to the
// target name it selects: that target's owner splits and judges the
// statements. A built-in dialect lint does not analyze, such as Oracle, stays
// refused whatever served holds, and a nil served adds nothing.
func CanonicalIn(dialect string, served schemaext.TargetResolver) (string, bool) {
	if canonical, ok := Canonical(dialect); ok {
		return canonical, true
	}
	if _, builtIn := platform.Lookup(dialect); builtIn || served == nil {
		return "", false
	}
	selected, err := served.ResolveTarget(strings.ToLower(strings.TrimSpace(dialect)))
	if err != nil {
		return "", false
	}
	return selected.Name(), true
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
//     "postgres", and a rule names cockroachdb, yugabytedb or spanner only
//     where it was measured on that engine -- PG303 is the one that has been
//     (stokaro/ptah#2958) -- so those databases run the dialect-independent
//     families plus that measured set. They are accepted because they share
//     PostgreSQL's wire protocol and planner family, so "this directory
//     targets PostgreSQL" is an honest description of the target rather than
//     a mistake worth blocking a deployment over.
//     TestBuiltInRules_PostgresFamilyMembersAreNamedOnlyWhereMeasured keeps
//     that asymmetry visible and the measured set explicit.
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
	return compatible(policyDialect, databaseDialect, Canonical)
}

// CompatibleIn is [Compatible] over the names [CanonicalIn] accepts. An
// engine that is not a built-in dialect is a family of its own, so a policy
// that names one governs migrations of that engine alone.
func CompatibleIn(policyDialect, databaseDialect string, served schemaext.TargetResolver) bool {
	return compatible(policyDialect, databaseDialect, func(dialect string) (string, bool) {
		return CanonicalIn(dialect, served)
	})
}

// compatible is the comparison of [Compatible], resolving each side with
// canonical.
func compatible(policyDialect, databaseDialect string, canonical func(string) (string, bool)) bool {
	policy, policyOK := canonical(policyDialect)
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
	database, databaseOK := canonical(databaseDialect)
	if !databaseOK {
		return false
	}
	if database == "" {
		return true
	}
	return Family(policy) == Family(database)
}

// Family returns the lint family a canonical dialect belongs to: the group
// whose members select the same built-in rule set, which is the protocol
// family its descriptor names. Members of one family are interchangeable in a
// lint policy; members of different families are not.
//
// An unsupported or empty dialect, and one whose descriptor names no family,
// is its own family, so it can only ever match itself.
func Family(canonical string) string {
	described, found := platform.Lookup(canonical)
	if found && described.Family != "" {
		return described.Family
	}
	return canonical
}
