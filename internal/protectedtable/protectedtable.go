// Package protectedtable decides whether a declared entry fences a table off
// from change.
//
// The rule is one predicate because it is asked from both ways to reconcile
// declared rows: `ptah migrations data` writes a migration body, and the
// declarative planner writes statements into a plan. Each asks about a table
// its own change touches, and an answer that differed between them would fence
// a table on one path and leave it open on the other -- which is what two lists
// do the moment the first is extended.
//
// `ptah seed` asks a different question with the same word. Its protected
// entries say the target is production, matched against the tables that EXIST
// there rather than the tables a change touches, so a run refuses before it
// writes anything. It shares the entry grammar below and nothing else.
package protectedtable

import (
	"strings"

	"ptah.run/core/schemamodel"
)

// Set is the protected entries a caller declared, ready to be asked about a
// table.
//
// The zero value is a set that protects nothing, so a caller that declared no
// entries needs no branch of its own.
type Set struct {
	// entries maps the lower-cased entry to the spelling the caller wrote,
	// because a refusal reads better in the words the author used.
	entries map[string]string
}

// New reads the declared entries.
//
// Surrounding space is dropped and an empty entry is ignored: a repeated flag
// and a list read out of a file both produce them, and an entry of "" would
// otherwise fence the table whose name is empty, which is every table with no
// schema.
func New(entries []string) Set {
	if len(entries) == 0 {
		return Set{}
	}
	set := Set{entries: make(map[string]string, len(entries))}
	for _, entry := range entries {
		if entry = strings.TrimSpace(entry); entry != "" {
			set.entries[strings.ToLower(entry)] = entry
		}
	}
	if len(set.entries) == 0 {
		return Set{}
	}
	return set
}

// Empty reports whether the set fences nothing, which lets a caller skip the
// work of deciding what a change touches.
func (s Set) Empty() bool {
	return len(s.entries) == 0
}

// Entry reports the declared entry that fences the table, and whether one does.
//
// An entry matches case-insensitively, by the bare table name or by the
// schema-qualified `schema.table` form. Both spellings match on purpose: a
// schema-qualified table can be fenced by either, and a bare entry fences the
// table in whatever schema it lives, which is the answer a caller wants when
// the same table exists in a tenant schema per customer.
func (s Set) Entry(schema, table string) (string, bool) {
	if s.Empty() {
		return "", false
	}
	if entry, ok := s.entries[strings.ToLower(table)]; ok {
		return entry, true
	}
	entry, ok := s.entries[strings.ToLower(schemamodel.QualifyTableName(schema, table))]
	return entry, ok
}
