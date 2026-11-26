// Package clickhousecomparison holds the rules ClickHouse gives the shared
// schema comparison ([schemacomparison.Rules]): how a data-skipping index's
// definition and a computed column's expression read on ClickHouse, and the
// server that decides whether two CHECKs are one. View queries compare in
// the connection's database scope. The built-in runtime
// registers them on its ClickHouse target, so the shared comparison holds no
// ClickHouse knowledge of its own.
package clickhousecomparison

import (
	"regexp"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chprobe"
	"ptah.run/internal/chindexkey"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/precedence"
	"ptah.run/internal/schemaprep"
)

// Rules returns ClickHouse's comparison rules. A CHECK is decided by the
// server, which formats both conditions ([chprobe.CheckService]).
func Rules() schemacomparison.Rules {
	return schemacomparison.Rules{
		IndexDefinitions: IndexDefinitions{}, GeneratedExpressions: GeneratedExpressions{},
		Checks: chprobe.CheckService{}, ViewQueries: ViewQueries{},
	}
}

// GeneratedExpressions reads the expression of a MATERIALIZED, ALIAS or
// EPHEMERAL column.
type GeneratedExpressions struct{}

// typeDefault is the expression ClickHouse reports for an EPHEMERAL column
// declared without one: defaultValueOfTypeName of the column's type.
var typeDefault = regexp.MustCompile(`^defaultValueOfTypeName\('(?:[^'\\]|\\.|'')*'\)$`)

// Spelling returns expression as the operations it groups under ClickHouse's
// precedence, the reading a default gets: ClickHouse stores `a + b * c` as
// `a + (b * c)` and `a = 1 OR b = 3` as `(a = 1) OR (b = 3)`. An expression
// the precedence does not read is returned as it stands.
//
// The expression ClickHouse reports for an EPHEMERAL column declared without
// one, `defaultValueOfTypeName('Int64')`, reads as no expression, the
// spelling of that declaration. The type it names is not compared: the
// column's type is compared on its own, and a lone call to the function names
// the column's type wherever ClickHouse wrote it.
func (GeneratedExpressions) Spelling(expression string) string {
	if typeDefault.MatchString(expression) {
		return ""
	}
	if grouped, read := precedence.Canonical(expression, precedence.ClickHouseOperators, dialectlexer.Options(platform.ClickHouse)); read {
		return grouped
	}
	return expression
}

// IndexDefinitions compares ClickHouse data-skipping index definitions.
//
// The key is the one part of the definition this comparison owns. The type
// and the granularity are the ClickHouse owner's index facet, compared there.
// An index has no uniqueness and no key direction on ClickHouse, which the
// renderer reports as lost rather than writes, so neither is compared.
type IndexDefinitions struct{}

// DefinitionChanged reports whether the key of current differs from the key
// desired declares.
//
// A skipping index's key is one expression, and system.data_skipping_indices
// reports it whole: `INDEX i (a, b)` reads back as the single key `a, b`. So
// the keys are compared as the expression each side writes, in the form
// [chindexkey.Same] reads ClickHouse's own formatting into.
func (IndexDefinitions) DefinitionChanged(desired schemamodel.Index, current catalog.Index, _ identifier.Semantics) bool {
	return !chindexkey.Same(chindexkey.Written(schemaprep.IndexKey(desired)), chindexkey.Reported(current))
}
