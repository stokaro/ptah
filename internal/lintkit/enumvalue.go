package lintkit

import "strings"

// EnumValueChange is what an ALTER TYPE statement does to one label of an
// enum type, read off the statement's words.
type EnumValueChange struct {
	// Type is the type the statement alters.
	Type TableReference
	// Label is the label the statement adds, or the one it renames, with its
	// quotes removed.
	Label string
	// Adds reports an ADD VALUE, and false a RENAME VALUE.
	Adds bool
	// IfNotExists reports an ADD VALUE IF NOT EXISTS, which the engine skips
	// when the type already has the label.
	IfNotExists bool
}

// AlterEnumValue reads `ALTER TYPE name ADD VALUE [IF NOT EXISTS] 'label'`,
// with or without a BEFORE or AFTER clause, and `ALTER TYPE name RENAME VALUE
// 'old' TO 'new'`, and reports whether the statement is one of them. Only a
// plain quoted label is read: an escape string such as E'x' spells its label
// in a form the words do not decode, so the statement is not recognized.
func AlterEnumValue(w, sourceWords []string) (EnumValueChange, bool) {
	if !HasWordPrefix(w, "ALTER", "TYPE") {
		return EnumValueChange{}, false
	}
	ref, j := TableRefAt(w, sourceWords, 2)
	if ref.Normalized == "" {
		return EnumValueChange{}, false
	}
	change := EnumValueChange{Type: ref}
	switch {
	case HasWordPrefix(w[j:], "ADD", "VALUE"):
		change.Adds = true
		j += 2
		if next := SkipIfNotExists(w, j); next != j {
			change.IfNotExists = true
			j = next
		}
	case HasWordPrefix(w[j:], "RENAME", "VALUE"):
		j += 2
	default:
		return EnumValueChange{}, false
	}
	if j >= len(w) || !plainStringLiteral(w[j]) {
		return EnumValueChange{}, false
	}
	change.Label = strings.ReplaceAll(w[j][1:len(w[j])-1], "''", "'")
	return change, true
}

// plainStringLiteral reports a string literal written 'text', which is the
// text between its quotes with each quote doubled.
func plainStringLiteral(word string) bool {
	return len(word) >= 2 && word[0] == '\'' && word[len(word)-1] == '\''
}
