// Package ydbcomparison holds the rules YDB gives the shared schema
// comparison ([schemacomparison.Rules]): how a column's type, default and
// sequence, an index definition and a view's query read on YDB, and how a
// default a YDB catalog reports reads back as a value. The built-in runtime registers them on its YDB target,
// so the shared comparison holds no YDB knowledge of its own. The comparison
// of YDB's own feature objects, such as changefeeds, is the YDB owner's
// object comparison and lives elsewhere.
package ydbcomparison

import (
	"ptah.run/catalog"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbview"
)

// Rules returns YDB's comparison rules.
func Rules() schemacomparison.Rules {
	return schemacomparison.Rules{Columns: Columns{}, Indexes: Indexes{}, Views: Views{}}
}

// Indexes compares YDB index definitions.
type Indexes struct{}

// OptionsEqual reports whether a YDB index holds the kind and the settings
// declared for it. YDB changes none of an index's definition in place -- its
// columns, its covered columns, its uniqueness and whether it is maintained
// synchronously are fixed when it is built -- so the shared comparison
// rebuilds an index whose definition differs, these options included.
//
// The kind is read through [ydbindex.KindOf] on both sides, which is the same
// reading the renderer writes the clause from, so a declared BTREE and a
// catalog GLOBAL SYNC are one kind. A catalog therefore reports the kind alone
// in Method -- `GLOBAL SYNC` or `GLOBAL ASYNC` -- and uniqueness in IsUnique,
// not the `GLOBAL UNIQUE SYNC` clause the renderer writes. An access method
// either side cannot read is a difference, because the index cannot be the
// one that was declared. A full-text and a local index compare their
// settings too.
//
// The partitioning is never a rebuild: it is the YDB owner's index facet,
// changed in place by ALTER INDEX. The one change YDB cannot make in place,
// removing a maximum partition count, is one no declaration asks for, because
// a maximum it leaves out keeps the held one.
//
// A vector index's settings are the YDB owner's facet, which its own
// comparison reads; a difference in them is planned there.
func (Indexes) OptionsEqual(desired schemamodel.Index, current catalog.Index) bool {
	desiredKind, desiredErr := ydbindex.KindOf(desired.Type)
	currentKind, currentErr := ydbindex.KindOf(current.Method)
	return desiredErr == nil && currentErr == nil && desiredKind == currentKind &&
		(!desiredKind.IsFullText() || ydbindex.FullTextEqual(desired.StorageParams, current.StorageParams)) &&
		(!desiredKind.IsLocal() || ydbindex.LocalEqual(desiredKind, desired.StorageParams, current.StorageParams))
}

// RenameCarries reports whether renaming a YDB index in place keeps a facet
// of kind. The index's partitioning is kept, since the YDB owner compares it
// under the new name and changes it in place. Any other facet, such as a
// vector index's settings, which are fixed when the index is built, is not,
// and a drop and a create of an index with such a facet are not a rename.
func (Indexes) RenameCarries(kind schemaext.Kind) bool {
	return kind == ydbschema.IndexPartitioningKind
}

// Views compares YDB view queries.
type Views struct{}

// QueryEqual compares two YDB view queries exactly, in the form the server
// stores a query in ([ydbview.QueryText]). The server keeps a query's tokens
// and nothing else, so both sides are read into that form and compared as
// they stand: YDB names are case-sensitive, so the case folding other targets
// share would call two different views one, and the server keeps a `*` and
// the qualifiers it was given, so there is nothing for later folds to undo.
func (Views) QueryEqual(desired, current string) bool {
	return ydbview.QueryText(desired) == ydbview.QueryText(current)
}
