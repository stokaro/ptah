package providerhost

import (
	"ptah.run/catalog"
	"ptah.run/core/schemaprojection"
)

// indexKindReadBack is what a read of an engine reports for a declaration,
// for an engine that spells a declared index's kind as it reports a held one
// (provider.IndexMethodsFeature, ADR 0057): an index declared with a kind
// reads back with that kind as its method, so a declaration converted to
// stand in for the current side, as a Go source does, compares with the read
// rather than as an index of no method. Without it an asynchronous YDB index
// read as "async" compares with a declaration of none, and every plan drops
// and creates it again.
//
// A column reads back with its declared type, and a unique index is described
// as a unique index only by what the comparison reads of it.
type indexKindReadBack struct{}

var _ schemaprojection.ReadBack = indexKindReadBack{}

func (indexKindReadBack) ColumnType(string) (string, bool) { return "", false }

func (indexKindReadBack) IndexMethod(indexType string) (string, bool) {
	return indexType, indexType != ""
}

func (indexKindReadBack) UniqueIndexDetail(catalog.Index) bool { return false }
