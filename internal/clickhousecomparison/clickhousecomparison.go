// Package clickhousecomparison holds the rules ClickHouse gives the shared
// schema comparison ([schemacomparison.Rules]): how a data-skipping index's
// definition reads on ClickHouse. The built-in runtime registers them on its
// ClickHouse target, so the shared comparison holds no ClickHouse knowledge of
// its own.
package clickhousecomparison

import (
	"cmp"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/chindexkey"
)

// Rules returns ClickHouse's comparison rules.
func Rules() schemacomparison.Rules {
	return schemacomparison.Rules{IndexDefinitions: IndexDefinitions{}}
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
	desiredKeys := make([]string, 0, len(desired.Parts))
	for _, part := range declaredParts(desired) {
		desiredKeys = append(desiredKeys, cmp.Or(strings.TrimSpace(part.Expr), part.Name))
	}
	currentKeys := make([]string, 0, len(current.Parts))
	for _, part := range reportedParts(current) {
		currentKeys = append(currentKeys, cmp.Or(strings.TrimSpace(part.Expr), part.Name))
	}
	return !chindexkey.Same(desiredKeys, currentKeys)
}

// declaredParts are the key parts of a declared index, or its fields as parts
// where it declares no parts.
func declaredParts(index schemamodel.Index) []schemamodel.IndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]schemamodel.IndexPart, len(index.Fields))
	for position, field := range index.Fields {
		parts[position] = schemamodel.IndexPart{Name: field}
	}
	return parts
}

// reportedParts are the key parts of a reported index, or its columns as parts
// where the reader reported no parts.
func reportedParts(index catalog.Index) []catalog.IndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]catalog.IndexPart, len(index.Columns))
	for position, column := range index.Columns {
		parts[position] = catalog.IndexPart{Name: column}
	}
	return parts
}
