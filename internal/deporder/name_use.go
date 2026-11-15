package deporder

import (
	"slices"
	"strings"
	"unicode/utf8"
)

// NameUse is one name in the code of a body, with what the code beside it says
// the name can be.
type NameUse struct {
	// Parts are the name's parts in order, folded to lower case, a quoted part
	// without its quotes. A part that is not a name, such as the star of
	// `t.*`, is empty.
	Parts []string
	// Call reports that the name stands where SQL names a function: an
	// opening parenthesis follows it, as in `uid()` and `EXECUTE FUNCTION
	// touch()`, or FUNCTION, PROCEDURE or ROUTINE precedes it, as in `COMMENT
	// ON FUNCTION uid IS ...`, `GRANT EXECUTE ON FUNCTION uid TO ...` and,
	// past IF EXISTS, `DROP FUNCTION IF EXISTS uid`.
	Call bool
	// Relation reports that the name stands where SQL names a table, a view or
	// a sequence: after FROM, JOIN, INTO, UPDATE, TABLE, ONLY, ON, USING,
	// REFERENCES, SEQUENCE, VIEW, SETOF, RETURNS, the EXISTS of IF [NOT]
	// EXISTS, PARTITION OF or ATTACH and DETACH PARTITION, after the parentheses a
	// FROM, a JOIN or INHERITS opens, further along the list a FROM, USING,
	// TRUNCATE or LOCK opens, or in front of `%ROWTYPE`.
	Relation bool
}

// CodeNameUses returns the names in the code of body, in order, read with
// dialect's lexical rules, as [ReferencesRelation] reads them: parts fold to
// lower case, and a name inside a string literal, a dollar-quoted string or a
// comment names nothing and is not returned.
//
// Where a name stands is read off the code beside it, not from a parse of the
// statement. Call and Relation each err toward true: a column that follows ON
// in a join condition reads as a relation, and a type with a length, such as
// `varchar(10)`, as a call. A name either one reports false for is not, in any
// statement SQL writes, a function or a relation named in that place.
func CodeNameUses(body, dialect string) []NameUse {
	body = strings.ToLower(body)
	if strings.TrimSpace(body) == "" {
		return nil
	}
	masks := sqlMasksForDialect(body, dialect)
	spans := dottedNameSpans(body, masks)
	uses := make([]NameUse, 0, len(spans))
	for _, span := range spans {
		next := skipCodeSpace(body, masks, span.end)
		uses = append(uses, NameUse{
			Parts:    slices.Clone([]string(span.name)),
			Call:     codeByteAt(body, masks, next, '(') || namesRoutine(body, masks, span.start),
			Relation: codeByteAt(body, masks, next, '%') || standsForRelation(body, masks, span.start),
		})
	}
	return uses
}

// codeByteAt reports whether the code of body holds b at offset i, outside a
// quoted name.
func codeByteAt(body string, masks sqlMasks, i int, b byte) bool {
	return i < len(body) && masks.code[i] && !masks.quotedIdentifier[i] && body[i] == b
}

// standsForRelation reports whether the name that starts at offset start of
// body stands where SQL names a relation; see [NameUse.Relation].
func standsForRelation(body string, masks sqlMasks, start int) bool {
	before := previousCode(body, masks, start)
	switch {
	case relationKeyword(before.word):
		return true
	case before.mark == '(':
		// `FROM (a JOIN b ON ...)`, as PostgreSQL prints a join, and
		// `INHERITS (a)`.
		opener := before
		for opener.mark == '(' {
			opener = previousCode(body, masks, opener.at)
		}
		switch opener.word {
		case "from", "join", "inherits":
			return true
		}
		return false
	case before.mark == ',':
		return continuesRelationList(body, masks, before.at)
	default:
		return false
	}
}

// relationKeyword reports whether a name that follows word stands where SQL
// names a relation. ON names the table of a policy, a trigger, an index or a
// grant, and the first operand of a join condition; RETURNS and SETOF name a
// table's row type; EXISTS ends the IF [NOT] EXISTS of a statement on a table;
// OF names the parent of PARTITION OF, and PARTITION the child ATTACH and
// DETACH name.
func relationKeyword(word string) bool {
	switch word {
	case "from", "join", "into", "update", "table", "only", "on", "using", "references",
		"inherits", "like", "lock", "truncate", "sequence", "view", "setof", "returns",
		"exists", "of", "partition":
		return true
	default:
		return false
	}
}

// namesRoutine reports whether the name that starts at offset start of body
// follows FUNCTION, PROCEDURE or ROUTINE, past the IF [NOT] EXISTS of a
// statement on a routine, as in `DROP FUNCTION IF EXISTS uid`.
func namesRoutine(body string, masks sqlMasks, start int) bool {
	before := previousCode(body, masks, start)
	if before.word == "exists" {
		before = previousCode(body, masks, before.at)
		if before.word == "not" {
			before = previousCode(body, masks, before.at)
		}
		if before.word != "if" {
			return false
		}
		before = previousCode(body, masks, before.at)
	}
	return routineKeyword(before.word)
}

// routineKeyword reports whether a name that follows word stands where SQL
// names a routine without its argument list.
func routineKeyword(word string) bool {
	switch word {
	case "function", "procedure", "routine":
		return true
	default:
		return false
	}
}

// continuesRelationList reports whether the comma at offset comma of body
// continues a list of relations: `FROM a x, b`, `TRUNCATE a, b`, `INHERITS
// (a, b)`. It reads back to what opened the list, past every name, alias and
// parenthesized group, and stops at the first word that opens anything else.
func continuesRelationList(body string, masks sqlMasks, comma int) bool {
	depth := 0
	for at := comma; ; {
		before := previousCode(body, masks, at)
		at = before.at
		switch {
		case before.at < 0 || before.mark == ';':
			return false
		case before.mark == ')':
			depth++
		case before.mark == '(' && depth > 0:
			depth--
		case before.mark == '(':
			return previousCode(body, masks, before.at).word == "inherits"
		case depth > 0 || before.word == "":
			continue
		case listOpener(before.word):
			return true
		case clauseWord(before.word):
			return false
		}
	}
}

// listOpener reports whether word opens a comma-separated list of relations.
func listOpener(word string) bool {
	switch word {
	case "from", "using", "truncate", "lock", "table":
		return true
	default:
		return false
	}
}

// clauseWord reports whether word opens something other than a list of
// relations, so a comma after it separates columns, values or conditions.
func clauseWord(word string) bool {
	switch word {
	case "select", "where", "group", "order", "by", "having", "limit", "offset", "set", "values",
		"returning", "on", "window", "into", "union", "except", "intersect", "with", "when", "then",
		"else", "case", "and", "or", "not", "create", "alter", "drop", "grant", "revoke", "insert",
		"update", "delete", "policy", "to", "begin", "end", "return", "perform", "check", "default":
		return true
	default:
		return false
	}
}

// codeToken is the piece of code in front of an offset of a body: a bare word,
// folded as the body is, or a punctuation mark. A quoted name has neither.
type codeToken struct {
	word string
	mark byte
	// at is where the piece starts, or -1 when nothing precedes the offset.
	at int
}

// previousCode returns the piece of code that ends before offset i of body,
// past white space, comments and literals.
func previousCode(body string, masks sqlMasks, i int) codeToken {
	j := i - 1
	for j >= 0 && (!masks.code[j] || (!masks.quotedIdentifier[j] && isSpace(body[j]))) {
		j--
	}
	if j < 0 {
		return codeToken{at: -1}
	}
	if masks.quotedIdentifier[j] {
		for j > 0 && masks.quotedIdentifier[j-1] && !masks.quoteOpen[j] {
			j--
		}
		return codeToken{at: j}
	}
	end := j + 1
	start := end
	for start > 0 && masks.code[start-1] && !masks.quotedIdentifier[start-1] {
		r, size := utf8.DecodeLastRuneInString(body[:start])
		if !isNameRune(r) {
			break
		}
		start -= size
	}
	if start == end {
		return codeToken{mark: body[j], at: j}
	}
	return codeToken{word: body[start:end], at: start}
}
