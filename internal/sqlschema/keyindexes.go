package sqlschema

import (
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
)

// Document is a schema read one file at a time: the model the earlier files
// built, and what the reader knows about it that the model has no place for.
//
// A schema directory, and a file with its imports, is one script run in order;
// see [ReadOnto]. The model holds every object the script declared. It does not
// hold which indexes a MySQL or MariaDB server built for a foreign key rather
// than for a declaration, and a later statement needs that to name an index
// and to know what `DROP FOREIGN KEY` leaves behind; see [keyIndex].
type Document struct {
	base *schemamodel.Database
	keys keyIndexes
}

// NewDocument starts a document whose earlier files built base. base is the
// caller's own accumulated model, which the reader changes in place; see
// [ReadOnto]. A nil base starts an empty document.
func NewDocument(base *schemamodel.Database) *Document {
	return &Document{base: base}
}

// keyIndex is one index a MySQL-family server built for a foreign key rather
// than for a declaration of the document.
//
// The server builds such an index for a key nothing else covers, and keeps it
// only while no other index begins with its columns. Measured on MySQL 8.4.11
// and 26.7.0 and MariaDB 11.8.9 and 12.3.3, with `FK (a)` standing for
// `FOREIGN KEY (a) REFERENCES p(id)`:
//
//	statements                                              indexes of c
//	CONSTRAINT fk FK (a); DROP FOREIGN KEY fk               fk (a)
//	CONSTRAINT fk FK (a); DROP FOREIGN KEY fk; ADD KEY (b)  fk (a), b (b)
//	CONSTRAINT fk FK (a); DROP FOREIGN KEY fk; ADD KEY k (a, b)   k (a, b)
//	CONSTRAINT fk FK (a); ADD CONSTRAINT fk2 FK (a)         fk2 (a)
//	CONSTRAINT fk FK (a, b); ADD CONSTRAINT fk2 FK (a)      fk (a, b)
//	FK (a), FK (a, b) in one CREATE TABLE                   a (a, b)
//	KEY k (a), FK (a); ADD UNIQUE (a)                       k (a), a (a)
//
// So the index outlives its key, and it gives way to any later index that
// begins with its columns -- one the author declares, or one the server builds
// for another key -- and to an identical one built for a later key. An index
// the author declared never gives way. MySQL does not let a leading part in
// descending order stand in for an ascending one, and MariaDB does.
type keyIndex struct {
	// table is the table, qualified as the model qualifies it.
	table   string
	name    string
	columns []string
	// owner is the foreign key the server built the index for, and empty once
	// that key is dropped.
	owner string
	// declared is whether the model declares the index as an index of the
	// table: the index a MySQL `FOREIGN KEY name (columns)` clause names, which
	// the comparison cannot tell is the key's by its name, and an index whose
	// key was dropped.
	declared bool
}

// keyIndexes are the indexes a document's server built for its foreign keys,
// in the order it built them.
type keyIndexes struct {
	entries []keyIndex
}

// onTable answers the indexes built for keys of table.
func (k *keyIndexes) onTable(table string) []keyIndex {
	var entries []keyIndex
	for _, entry := range k.entries {
		if entry.table == table {
			entries = append(entries, entry)
		}
	}
	return entries
}

// add records an index the server built.
func (k *keyIndexes) add(entry keyIndex) {
	k.entries = append(k.entries, entry)
}

// releaseCoveredBy removes the indexes of table that an index over covering
// begins with, which the server drops when that index is built, and answers
// the ones the model declares, so the caller can take them out of the model.
func (k *keyIndexes) releaseCoveredBy(table string, covering candidateKey, naming engineIndexNaming) []keyIndex {
	var released []keyIndex
	k.entries = slices.DeleteFunc(k.entries, func(entry keyIndex) bool {
		if entry.table != table || !(coverage{covering}).covers(entry.columns, naming) {
			return false
		}
		if entry.declared {
			released = append(released, entry)
		}
		return true
	})
	return released
}

// reusable reports whether table holds a built index over more columns than
// columns that begins with them, which a key over columns reuses rather than
// building its own.
func (k *keyIndexes) reusable(table string, columns []string, naming engineIndexNaming) bool {
	for _, entry := range k.onTable(table) {
		if len(entry.columns) > len(columns) && (coverage{ascending(entry.columns)}).covers(columns, naming) {
			return true
		}
	}
	return false
}

// disown answers the indexes the server built for key on table, which outlive
// it, and records that no key owns them. Each is declared from then on, since
// the model has no other way to say the table still holds it.
func (k *keyIndexes) disown(table, key string) []keyIndex {
	var orphans []keyIndex
	for i := range k.entries {
		entry := &k.entries[i]
		if entry.table != table || entry.owner == "" || !strings.EqualFold(entry.owner, key) {
			continue
		}
		entry.owner = ""
		if !entry.declared {
			entry.declared = true
			orphans = append(orphans, *entry)
		}
	}
	return orphans
}

// forget removes the index of table called name, which a statement dropped.
func (k *keyIndexes) forget(table, name string) {
	k.entries = slices.DeleteFunc(k.entries, func(entry keyIndex) bool {
		return entry.table == table && strings.EqualFold(entry.name, name)
	})
}

// forgetTable removes every index of table, which a statement dropped with
// the table.
func (k *keyIndexes) forgetTable(table string) {
	k.entries = slices.DeleteFunc(k.entries, func(entry keyIndex) bool {
		return entry.table == table
	})
}

// isDeclaredKeyIndex reports whether table's index called name is one the
// server built for a key and the model declares.
func (k *keyIndexes) isDeclaredKeyIndex(table, name string) bool {
	return slices.ContainsFunc(k.entries, func(entry keyIndex) bool {
		return entry.table == table && entry.declared && strings.EqualFold(entry.name, name)
	})
}
