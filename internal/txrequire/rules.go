package txrequire

import "strings"

// concurrentIndex reports CREATE INDEX CONCURRENTLY or DROP INDEX
// CONCURRENTLY, which PostgreSQL refuses inside a transaction block
// unconditionally -- measured `ERROR: CREATE INDEX CONCURRENTLY cannot run
// inside a transaction block (25001)`.
func concurrentIndex(words []string) bool {
	if hasPrefixWords(words, "CREATE", "INDEX") ||
		hasPrefixWords(words, "CREATE", "UNIQUE", "INDEX") ||
		hasPrefixWords(words, "DROP", "INDEX") {
		return containsWord(words, "CONCURRENTLY")
	}
	return false
}

// createdType names the type a CREATE TYPE statement creates, or "".
//
// It is the exception that keeps a valid workflow working: a value added to a
// type created in the SAME transaction is usable immediately, so a file that
// creates its enum and then uses a new value needs no autocommit.
//
// It does not check that the type is an enum, because a non-enum registration
// cannot mask a finding: a value can only be added to an enum, so an
// `ALTER TYPE ... ADD VALUE` against a composite fails on its own before any
// of this matters. Requiring the ENUM keyword would be a branch no valid SQL
// can reach.
func createdType(words []string) string {
	if !hasPrefixWords(words, "CREATE", "TYPE") || len(words) < 3 {
		return ""
	}
	return normalizeName(words[2])
}

// recordAddedEnumValue records the values an ALTER TYPE ... ADD VALUE
// statement adds to a type this file did not create.
//
// A value added to a type the file created is not recorded, because using it
// in the same transaction is allowed.
func recordAddedEnumValue(words, sourceWords []string, created map[string]bool, pending map[string]string) {
	if !hasPrefixWords(words, "ALTER", "TYPE") || len(words) < 3 {
		return
	}
	typeName := normalizeName(words[2])
	if created[typeName] {
		return
	}
	displayName := typeName
	if len(sourceWords) > 2 {
		displayName = sourceWords[2]
	}
	for index := 3; index+1 < len(words); index++ {
		if words[index] != "VALUE" {
			continue
		}
		value := words[index+1]
		if value == "IF" {
			// ADD VALUE IF NOT EXISTS 'x'
			if index+4 < len(words) {
				value = words[index+4]
			}
		}
		if isStringLiteral(value) {
			pending[value] = displayName
		}
	}
}

// usesPendingValue reports whether a statement mentions a value this file has
// already added to a pre-existing enum type.
//
// The match is on the quoted LITERAL rather than on the type name, because
// that is what PostgreSQL refuses: the value, not the type. The literal keeps
// its quotes through tokenization, so a bare identifier of the same spelling
// cannot match it.
//
// Another ALTER TYPE is not a use, and that is measured rather than assumed:
// `BEGIN; ALTER TYPE af ADD VALUE 'great'; ALTER TYPE af ADD VALUE 'grand'
// AFTER 'great'; COMMIT;` is accepted on PostgreSQL 18.4, and the ordering
// takes effect. Positioning a new value against one added moments earlier is
// exactly the shape that would otherwise be read as a use.
func usesPendingValue(words []string, pending map[string]string) bool {
	if len(pending) == 0 {
		return false
	}
	// Adding another value to the same type is not a use of the first.
	if hasPrefixWords(words, "ALTER", "TYPE") {
		return false
	}
	for _, word := range words {
		if _, ok := pending[word]; ok {
			return true
		}
	}
	return false
}

func isStringLiteral(word string) bool {
	return len(word) >= 2 && word[0] == '\''
}

// normalizeName folds an identifier the way the tokenizer left it: a bare name
// arrives upper-cased, a quoted one keeps its case and its quotes.
func normalizeName(word string) string {
	return strings.TrimSuffix(strings.TrimPrefix(word, `"`), `"`)
}
