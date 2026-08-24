// Package config provides configuration options for the Ptah schema migration system.
//
// This package provides a simple, programmatic API for configuring schema comparison
// and migration behavior when using Ptah as a library. It focuses on providing
// clean Go APIs rather than external configuration file management.
package config

import (
	"slices"

	"go.5x5.cz/ptah/core/platform/identifier"
)

// CompareOptions contains configuration options for schema comparison operations.
// These options control how schema differences are calculated and what elements
// should be ignored during comparison.
type CompareOptions struct {
	// IgnoredExtensions is a list of PostgreSQL extension names that should be
	// ignored during schema migrations. These extensions will:
	// - Never be deleted, even if missing from the target schema
	// - Be excluded from schema diff calculations
	// - Be treated as if they don't exist for comparison purposes
	//
	// Common extensions to ignore include:
	// - plpgsql: Default procedural language, usually pre-installed
	// - adminpack: Administrative functions, often pre-installed
	IgnoredExtensions []string

	// Dialect is the target database dialect ("postgres", "mysql", "mariadb",
	// "clickhouse"). It is optional; when empty the comparison uses
	// dialect-neutral rules. It is currently consulted only to fold
	// referential-action reporting quirks: MariaDB reports an unspecified
	// ON DELETE/ON UPDATE as RESTRICT (MySQL and PostgreSQL report NO ACTION),
	// and InnoDB treats RESTRICT and NO ACTION identically, so for MySQL/MariaDB
	// RESTRICT is folded to NO ACTION to avoid a perpetual drop+add loop on an
	// unchanged foreign key. PostgreSQL distinguishes the two at DDL, so the
	// fold is deliberately NOT applied there.
	Dialect string

	// IdentifierSemantics overrides the dialect's offline identifier rules.
	// Catalog-sensitive callers must provide a complete resolved snapshot;
	// first-party live operations obtain one through schemadiff.
	IdentifierSemantics *identifier.Semantics

	// SkipTableDrops reports that the caller removes every table drop from the
	// diff before it is planned: `diff.skip: [drop_table]` in ptah.yaml, and
	// `diff { skip { drop_table = true } }` in an Atlas project file.
	//
	// It changes NO comparison result. Nothing here reads it except the SQLite
	// virtual-table guard, which refuses a comparison for the statements it
	// predicts -- and a table drop the caller filters out again is a statement
	// nothing will run. Without this, a `schema apply` configured to skip table
	// drops was refused on an fts4 database whose plan the policy had already
	// emptied, and the operator was sent to two dangerous opt-ins to obtain
	// `Schema is synced, no changes to be made.` (stokaro/ptah#1028).
	//
	// It is deliberately narrow. `skip drop_table` does not filter a
	// modification, so the guard's post-comparison half keeps refusing a
	// rebuild of a table Ptah could not classify; only the removal input is
	// discounted.
	SkipTableDrops bool

	// SkipColumnDrops reports that the caller removes every column drop from
	// the diff before it is planned: `diff.skip: [drop_column]` in ptah.yaml.
	//
	// It changes NO comparison result, and only the SQLite virtual-table guard
	// reads it, for the same reason SkipTableDrops exists. SQLite has no ALTER
	// for a removed column, so the planner converges one by rebuilding the
	// table -- drop, recreate, copy -- and the guard refuses a rebuild it
	// cannot vouch for. A column drop the caller filters out again is a rebuild
	// nothing will perform. Without this, `ptah migrations generate` with
	// `diff.skip: [drop_table, drop_column]` was refused at exit 2 on an fts4
	// database whose plan the policy had already emptied to nothing.
	SkipColumnDrops bool

	// SkipIndexDrops reports that the caller removes every standalone index
	// drop from the diff before it is planned: `diff.skip: [drop_index]` in
	// ptah.yaml. Index replacements are not removed by that policy and are not
	// discounted by this field.
	//
	// It changes NO comparison result either. The SQLite virtual-table guard
	// counts the table an index removal is aimed at, because Ptah cannot tell a
	// module's own index from one an operator added any more than it can tell
	// the module's storage from an ordinary table; a drop the caller deletes
	// again is not one of those.
	SkipIndexDrops bool

	// DomainExpressions carries each declared domain's CHECK and DEFAULT as the
	// target server itself spells them, keyed by the domain's qualified name.
	//
	// PostgreSQL does not store the text of a CHECK. It parses the expression
	// and prints it back from the parse tree, so `VALUE IN ('x','y')` is read
	// back as `VALUE = ANY (ARRAY['x'::text, 'y'::text])` and `VALUE > 0` as
	// `(VALUE > 0)`. Comparing a declaration against that is comparing two
	// different languages, which is why the comparison used to decline: a
	// string difference did not mean a changed constraint, and acting on one
	// would have dropped and recreated a domain that had not changed.
	//
	// A resolved entry is the declaration after the same round trip -- the
	// server was asked to normalize it -- so the two sides compare as like with
	// like and a real change is neither invented nor lost (stokaro/ptah#1717).
	//
	// A nil map means nobody could ask a server, which is every comparison that
	// has no connection. CHECK and DEFAULT stay uncompared there, exactly as
	// before.
	DomainExpressions map[string]DomainExpression

	// GeneratedExpressions carries each declared generated column's expression
	// as the target server itself spells it, keyed by the column's qualified
	// name.
	//
	// It is [CompareOptions.DomainExpressions] for a different attribute and the
	// same reason: Oracle does not store the text of a generated column's
	// expression. It stores a rewrite -- every column reference quoted and
	// upper-cased, the spaces around operators gone, and parentheses added that
	// the declaration did not have. Measured on 23.26.2.0.0:
	//
	//	declared                                        stored
	//	n * 2                                        -> "N"*2
	//	CASE WHEN n > 0 AND n < 10 THEN 1 ELSE 0 END -> CASE  WHEN ("N">0 AND "N"<10) THEN 1 ELSE 0 END
	//
	// The last row is why no textual fold is sound: nothing that folds
	// whitespace and case invents those parentheses, and a rule that agrees on
	// the simple rows and disagrees on that one is worse than no rule.
	//
	// A resolved entry is the declaration after the same round trip, so the two
	// sides compare as like with like (stokaro/ptah#1915).
	//
	// A nil map means nobody could ask a server. On a dialect whose stored form
	// is a rewrite the expression then stays UNCOMPARED, because reporting a
	// difference between two spellings of one expression plans a modification
	// that changes nothing, on every run. Every other dialect stores the text it
	// was given and is compared as before.
	GeneratedExpressions map[string]GeneratedExpression

	// ContinuousAggregateBodies carries each declared TimescaleDB continuous
	// aggregate's SELECT as the target server itself spells it, keyed by the
	// aggregate's qualified name.
	//
	// It is [CompareOptions.DomainExpressions] for a third attribute and the
	// same reason. TimescaleDB rewrites the definition it is given before
	// storing it, and the rewrite is not a formatting difference: measured on
	// 2.29.2 / PostgreSQL 17.11,
	//
	//	declared                          stored
	//	time_bucket('1 hour', time)    -> time_bucket('01:00:00'::interval, "time")
	//	GROUP BY bucket, sensor        -> GROUP BY (time_bucket('01:00:00'::interval, "time")), sensor
	//
	// The second row is why no textual fold is sound: the GROUP BY key a
	// declaration writes by its output name comes back as the whole expression
	// it stood for, and nothing that folds whitespace and case does that.
	//
	// A resolved entry is the declaration after the same rewrite, so the two
	// sides compare as like with like (stokaro/ptah#1026).
	//
	// A nil map means nobody could ask a server. The body then stays
	// UNCOMPARED: reporting a difference between two spellings of one SELECT
	// would drop and recreate the aggregate on every run, which is worse than
	// missing a change -- and dropping a continuous aggregate discards its
	// materialized history.
	ContinuousAggregateBodies map[string]ContinuousAggregateBody

	// CheckExpressions carries each declared table CHECK as the target server
	// itself spells it, keyed by the constraint's qualified name.
	//
	// It is [CompareOptions.DomainExpressions] for the same attribute on a
	// different object, and the rewrite is the same one. PostgreSQL does not
	// store the text of a CHECK: it parses the expression and prints it back
	// from the parse tree, so a declaration and its own read-back differ.
	// Measured on 17.11:
	//
	//	declared                     stored
	//	price >= 0                -> (price >= (0)::numeric)
	//	score BETWEEN 1 AND 10    -> ((score >= 1) AND (score <= 10))
	//	score > 0 OR grade = 'x'  -> ((score > 0) OR (grade = 'x'::text))
	//
	// The second row is why no textual fold finishes the job: BETWEEN is
	// expanded by the parser and nothing reconstructs it, which is the same
	// wall the IN rewrite hit before the comparison learned to decline that
	// one shape.
	//
	// A resolved entry is the declaration after the same rewrite, so the two
	// sides compare as like with like (stokaro/ptah#2044).
	//
	// A nil map means nobody could ask a server. The comparison then falls back
	// to the textual normalizer it used before, which is right for the shapes
	// that survive it and declines the ones it cannot fold.
	CheckExpressions map[string]CheckExpression

	// PolicyExpressions carries each declared RLS policy's USING and WITH CHECK
	// as the target server itself spells them, keyed by the policy's table and
	// name.
	//
	// It is [CompareOptions.CheckExpressions] for a third object and the same
	// rewrite. Measured on PostgreSQL 17.11:
	//
	//	declared        column          stored
	//	level > 0       integer      -> (level > 0)
	//	price >= 0      numeric      -> (price >= (0)::numeric)
	//	owner = 'x'     varchar      -> ((owner)::text = 'x'::text)
	//
	// The cast depends on the column's type, so no rule over the declaration's
	// text can decide it: the three rows differ only in the type of the column
	// they name (stokaro/ptah#2049).
	//
	// A nil map means nobody could ask a server, and the comparison falls back
	// to the textual normalizer it used before.
	PolicyExpressions map[string]PolicyExpression

	// IndexExpressions carries each declared index's expression and predicate
	// as the target server itself spells them, keyed by the index's name.
	//
	// The same rewrite again, and the same reason it cannot be folded:
	// `lower(code)` is stored as `lower((code)::text)` over a varchar column
	// and as `lower(code)` over text (stokaro/ptah#2047).
	IndexExpressions map[string]IndexExpression
}

// PolicyExpression is one RLS policy's expressions in the target server's own
// spelling. See [CompareOptions.PolicyExpressions].
//
// The zero value is not an empty policy: it is what a resolver returns for a
// declaration it could not put through the server, and a comparison must fall
// back rather than read it as changed. Resolved reports which it is.
type PolicyExpression struct {
	// Using and WithCheck are the normalized clauses, empty where the policy
	// declares none.
	Using     string
	WithCheck string
	// Resolved reports that a server answered for this policy.
	Resolved bool
}

// IndexExpression is one index's expression and predicate in the target
// server's own spelling. See [CompareOptions.IndexExpressions].
//
// The zero value carries the same meaning [PolicyExpression]'s does.
type IndexExpression struct {
	// Expression is the normalized index expression, empty for an index over
	// plain columns.
	Expression string
	// Predicate is the normalized WHERE clause, empty for a full index.
	Predicate string
	// Resolved reports that a server answered for this index.
	Resolved bool
}

// CheckExpression is one CHECK constraint's expression in the target server's
// own spelling. See [CompareOptions.CheckExpressions].
//
// The zero value is not an empty check: it is what a resolver returns for a
// declaration it could not put through the server, and a comparison must fall
// back rather than read it as removed. Resolved reports which it is.
type CheckExpression struct {
	// Expression is the normalized CHECK, as pg_get_expr prints it.
	Expression string
	// Resolved reports that a server answered for this constraint. A false
	// value on a present key is a declaration the server refused.
	Resolved bool
}

// ContinuousAggregateBody is one continuous aggregate's SELECT in the target
// server's own spelling. See [CompareOptions.ContinuousAggregateBodies].
//
// The zero value is not an empty body: it is what a resolver returns for a
// declaration it could not put through the server, and a comparison must skip
// the body rather than read it as changed. Resolved reports which it is.
type ContinuousAggregateBody struct {
	// Body is the normalized SELECT, as the catalog spells it.
	Body string
	// Resolved reports that a server answered for this aggregate. A false value
	// on a present key is a declaration the server refused, and the body may
	// not be compared.
	Resolved bool
}

// GeneratedExpression is one generated column's expression in the target
// server's own spelling. See [CompareOptions.GeneratedExpressions].
//
// The zero value is not "no expression": it is what a resolver returns for a
// declaration it could not put through the server, and a comparison must skip
// the attribute rather than read it as removed. Resolved reports which it is.
type GeneratedExpression struct {
	// Expression is the normalized expression, empty when the column declares
	// none.
	Expression string
	// Resolved reports that a server answered for this column. A false value on
	// a present key is a declaration the server refused, and the expression may
	// not be compared.
	Resolved bool
}

// DomainExpression is one domain's CHECK and DEFAULT in the target server's own
// spelling. See [CompareOptions.DomainExpressions].
//
// The zero value is not "no constraint": it is what a resolver returns for a
// declaration it could not put through the server, and a comparison must skip
// the attribute rather than read it as removed. Resolved reports which it is.
type DomainExpression struct {
	// Check is the normalized CHECK, empty when the domain declares none.
	Check string
	// Default is the normalized DEFAULT, empty when the domain declares none.
	Default string
	// Resolved reports that a server answered for this domain. A false value
	// on a present key is a domain whose declaration the server refused, and
	// nothing about it may be compared.
	Resolved bool
}

// DefaultCompareOptions returns the default comparison options with sensible defaults.
// The default configuration includes commonly pre-installed PostgreSQL
// extensions that should typically be ignored during migrations.
func DefaultCompareOptions() *CompareOptions {
	return &CompareOptions{
		IgnoredExtensions: []string{
			"plpgsql", // PostgreSQL procedural language - usually pre-installed
		},
	}
}

// WithIgnoredExtensions returns a new CompareOptions with the specified ignored extensions.
// This completely replaces the default ignored extensions list.
//
// Example:
//
//	opts := config.WithIgnoredExtensions("plpgsql", "adminpack", "pg_stat_statements")
func WithIgnoredExtensions(extensions ...string) *CompareOptions {
	return &CompareOptions{
		IgnoredExtensions: extensions,
	}
}

// WithAdditionalIgnoredExtensions returns a new CompareOptions that includes the default
// ignored extensions plus the additional ones specified.
//
// Example:
//
//	opts := config.WithAdditionalIgnoredExtensions("adminpack", "pg_stat_statements")
//	// Result: ["plpgsql", "adminpack", "pg_stat_statements"]
func WithAdditionalIgnoredExtensions(extensions ...string) *CompareOptions {
	defaults := DefaultCompareOptions()
	allExtensions := make([]string, len(defaults.IgnoredExtensions)+len(extensions))
	copy(allExtensions, defaults.IgnoredExtensions)
	copy(allExtensions[len(defaults.IgnoredExtensions):], extensions)

	return &CompareOptions{
		IgnoredExtensions: allExtensions,
	}
}

// IsExtensionIgnored checks if the given extension name should be ignored
// during schema migrations based on the current configuration.
func (c *CompareOptions) IsExtensionIgnored(extensionName string) bool {
	return slices.Contains(c.IgnoredExtensions, extensionName)
}

// FilterIgnoredExtensions removes ignored extensions from the provided slice
// and returns a new slice containing only non-ignored extensions.
// This is useful for filtering extension lists before comparison.
func (c *CompareOptions) FilterIgnoredExtensions(extensions []string) []string {
	filtered := make([]string, 0)
	for _, ext := range extensions {
		if !c.IsExtensionIgnored(ext) {
			filtered = append(filtered, ext)
		}
	}
	return filtered
}
