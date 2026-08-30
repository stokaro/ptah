package schemacensus

import (
	"slices"
	"strings"
)

// Disposition is what a desired-schema field is for.
//
// Every field reachable from the model carries exactly one, and every value
// other than [DDL] carries the reason it is not rendered. The point of the
// closed set is that there is no implicit eighth value meaning "populated, and
// nobody downstream reads it": a field that would need one is a defect, and
// [Entry.Gap] is where it is recorded until it is repaired.
type Disposition string

const (
	// DDL: the field reaches rendered SQL on at least one target. [Measure]
	// proves it by removing the field and watching the output change.
	DDL Disposition = "ddl"

	// Comparison: read when a desired schema is compared with a live one, and
	// never written into DDL. What a description does not claim to describe is
	// the example: it changes what a diff may conclude and nothing else.
	Comparison Disposition = "comparison"

	// Planning: read while a change set is assembled or ordered, and written
	// into no statement a render produces. A declared extension dependency
	// decides what has to exist first, and the CREATE statement it orders does
	// not mention it.
	//
	// One entry carries the other half of that sentence and is worth reading
	// before the category is reused: Index.Concurrently asks that an index be
	// BUILT without locking, which is a fact about adding it to a live table.
	// A plan does write it into DDL. A render creates the whole schema from
	// nothing, has no table to lock, and has neither of the two facts the
	// decision needs.
	Planning Disposition = "planning"

	// Derived: computed from other fields by [schemamodel.Finalize] or by a
	// reader, rather than authored. Removing it changes nothing because the
	// derivation puts it back, which is the intended behavior rather than a
	// loss.
	Derived Disposition = "derived"

	// SourceOrigin: where the declaration was read from -- the Go struct a
	// table was parsed out of, the helper a column came from. It identifies the
	// source text, not a database object.
	SourceOrigin Disposition = "source"

	// Export: names and shapes an exported API document carries. It is a
	// contract about a generated document, and it reaches no database.
	Export Disposition = "export"

	// Data: reference or seed rows, and where to find them. Rows are not DDL,
	// and the verb that writes them is not the one that renders a schema.
	Data Disposition = "data"
)

// Dispositions is every value, in the order a reader meets them.
func Dispositions() []Disposition {
	return []Disposition{DDL, Comparison, Planning, Derived, SourceOrigin, Export, Data}
}

// Entry is one field's disposition.
//
// Reason is required for every disposition other than [DDL]: the classification
// alone says what a field is not, and the next reader needs to know why. Gap is
// the opposite direction and belongs only on a [DDL] field the census cannot
// observe -- a fact that should reach SQL and does not. It names the issue that
// tracks the repair, and the gate refuses a blank one.
type Entry struct {
	Field       string
	Disposition Disposition
	Reason      string
	Gap         string
}

// Registry returns one entry per field, sorted by field name.
//
// It is hand-written, and it is the only hand-written part of this package.
// Nothing here is trusted: [Fields] decides which entries must exist, and
// [Measure] decides whether a [DDL] entry is telling the truth.
func Registry() []Entry {
	entries := slices.Clone(registry)
	slices.SortFunc(entries, func(a, b Entry) int { return strings.Compare(a.Field, b.Field) })
	return entries
}

// Lookup returns the entry for a field path.
func Lookup(field string) (Entry, bool) {
	for _, entry := range registry {
		if entry.Field == field {
			return entry, true
		}
	}
	return Entry{}, false
}
