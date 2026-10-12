// Package sqlitecomparison holds the rules SQLite gives the shared schema
// comparison ([schemacomparison.Rules]): how an index definition reads on
// SQLite. The built-in runtime registers them on its SQLite target, so the
// shared comparison holds no SQLite knowledge of its own.
package sqlitecomparison

import (
	"ptah.run/catalog"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Rules returns SQLite's comparison rules.
func Rules() schemacomparison.Rules {
	return schemacomparison.Rules{Indexes: Indexes{}}
}

// Indexes compares SQLite index definitions.
//
// SQLite changes nothing about an index in place: a changed definition is a
// DROP INDEX and a CREATE INDEX. Registering the rules has the shared
// comparison compare the whole definition the reader reports: the uniqueness
// from PRAGMA index_list, and each key's column, direction and expression from
// PRAGMA index_xinfo and the stored CREATE INDEX text, beside the partial
// predicate it compares on every target.
type Indexes struct{}

// OptionsEqual reports that a SQLite index holds the options declared for it.
// SQLite has no access method, operator class, storage parameter or other
// build setting for an index, so there are no options beyond what the shared
// comparison reads.
func (Indexes) OptionsEqual(schemamodel.Index, catalog.Index) bool {
	return true
}

// RenameCarries reports that a SQLite index carries no facet through a
// rename. SQLite has no statement that renames an index.
func (Indexes) RenameCarries(schemaext.Kind) bool {
	return false
}
