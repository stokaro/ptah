package deporder

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/internal/tableref"
)

// ReferencesIdentifier reports whether the SQL body references name as a
// standalone or qualified identifier. It is [ReferencesRelation] with the
// default schema unknown, and it backs the schema-scope cross-reference
// diagnostics and the PostgreSQL planner's questions about what a drop takes
// with it.
//
// Only the code of the body counts. A name that appears inside a string
// literal, a dollar-quoted string, a line comment or a block comment names
// nothing, and treating it as a reference is not a harmless over-approximation:
// the PostgreSQL planner asks this question to work out what
// DROP VIEW ... CASCADE takes with it, and an answer of "yes" there puts a
// DROP MATERIALIZED VIEW ... CASCADE into the plan. Measured on PostgreSQL
// 17.10, a materialized view whose body is
// "SELECT 'base_view' AS label, count(*) AS total FROM accounts" -- which reads
// no view at all -- was dropped and recreated when base_view was modified,
// taking a hand-made dependent view, a unique index on the materialized view
// and the privileges granted on it, none of which the plan rebuilds.
//
// Quoted identifiers are code and still match, because "base_view" is a
// reference to base_view. Their contents are opaque, so a quotation mark or a
// comment marker inside one cannot open a literal or a comment.
func ReferencesIdentifier(body, name string) bool {
	return ReferencesRelation(body, name, "")
}

// ReferencesRelation reports whether a name in the code of the SQL body can
// name relation, a bare or schema-qualified name in [tableref] form, when an
// unqualified name resolves in defaultSchema.
//
// A name in the body is a run of parts joined by dots, each part bare or
// quoted, with spaces allowed around a dot. It is matched from its end: its
// last part against the relation's own name, and the part before that, where
// there is one, against the relation's schema. A part in front of those, such
// as a database, is not compared, and a name that ends in a column, such as
// `order_totals.total`, does not name the relation `order_totals`. The body
// `FROM "reporting".order_totals` reads reporting.order_totals.
//
// Parts compare without regard to case, quoted or not. PostgreSQL folds an
// unquoted part to lower case and keeps a quoted one as written, but a
// relation name a caller passes does not record whether its declaration quoted
// it, and the planners that ask also serve engines whose quoted names ignore
// case. Folding both costs a rebuild or a refusal where two relations differ
// only in case; telling them apart would cost a reader the drop takes without
// a word.
//
// Two schemas written out are one schema only when they are equal:
// `reporting.item_skus` never reads public.item_skus. An unqualified name, in
// relation or in the body, is in defaultSchema, as the planner resolves one: a
// bare `item_skus` reads public.item_skus when defaultSchema is public, and
// reads no reporting.item_skus. An empty defaultSchema means the schema is not
// known, and an unqualified name then matches a qualified one in any schema,
// because the answer has to err toward "reads it": everything that reads a
// dropped relation is either rebuilt or refused, and what is missed is dropped
// with CASCADE.
//
// Only the code of the body counts; see [ReferencesIdentifier]. A relation that
// [tableref.Parse] does not accept names nothing.
func ReferencesRelation(body, relation, defaultSchema string) bool {
	ref, ok := tableref.Parse(relation)
	if !ok {
		return false
	}
	resolves := func(schema string) bool {
		return defaultSchema == "" || strings.EqualFold(schema, defaultSchema)
	}
	return namesRead(codeNames(body, ""), ref, resolves)
}

// namesRead reports whether one of names, read from a body, names ref.
//
// inDefault tells whether an unqualified name may be in schema. The question is
// the same for both sides of a pairing: a bare name in the body against a
// qualified relation, and a qualified name in the body against a bare one.
// [ReferencesRelation] answers it with the default schema it was given, or yes
// when it was given none; the creation order answers it with the dialect's
// default schema, or no; see [referencesViewLikeIdentifier].
func namesRead(names []dottedName, ref tableref.Ref, inDefault func(schema string) bool) bool {
	for _, name := range names {
		last := len(name) - 1
		if !strings.EqualFold(name[last], ref.Name) {
			continue
		}
		if namesSchema(name, ref, inDefault) {
			return true
		}
	}
	return false
}

// namesSchema reports whether name, whose last part already equals ref's own
// name, is in ref's schema.
func namesSchema(name dottedName, ref tableref.Ref, inDefault func(schema string) bool) bool {
	qualified := len(name) > 1
	switch {
	case !qualified && !ref.Qualified:
		return true
	case !qualified:
		return inDefault(ref.Schema)
	case ref.Qualified:
		return strings.EqualFold(name[len(name)-2], ref.Schema)
	default:
		return inDefault(name[len(name)-2])
	}
}

// dottedName is one name in the code of a body: its parts in order, each as
// written once the quotes are taken off. A part that is not a name, such as
// the star of `t.*`, is empty and matches nothing.
type dottedName []string

// codeNames returns the names in the code of body, read with dialect's
// lexical rules; see [sqlMasksForDialect].
//
// The body is folded to lower case before it is read, as the scanner expects
// (an E-prefixed literal is recognized by a lower-case e), and the names come
// back folded.
func codeNames(body, dialect string) []dottedName {
	body = strings.ToLower(body)
	if strings.TrimSpace(body) == "" {
		return nil
	}
	return dottedNames(body, sqlMasksForDialect(body, dialect))
}

// dottedNames reads every dotted name in the code of body.
func dottedNames(body string, masks sqlMasks) []dottedName {
	var names []dottedName
	for i := 0; i < len(body); {
		part, end, ok := namePart(body, masks, i)
		if !ok {
			i = skipNonName(body, masks, i)
			continue
		}
		name := dottedName{part}
		for {
			dot := skipCodeSpace(body, masks, end)
			if dot >= len(body) || !isCodeDot(body, masks, dot) {
				break
			}
			next, nextEnd, ok := namePart(body, masks, skipCodeSpace(body, masks, dot+1))
			if !ok {
				name = append(name, "")
				end = dot + 1
				break
			}
			name = append(name, next)
			end = nextEnd
		}
		names = append(names, name)
		i = end
	}
	return names
}

// namePart reads the name part that starts at offset i of body: a quoted
// identifier, without its quotes and with each doubled quote halved, or a bare
// word. It reports false when no part starts there.
func namePart(body string, masks sqlMasks, i int) (part string, end int, ok bool) {
	if i >= len(body) || !masks.code[i] {
		return "", i, false
	}
	if masks.quoteOpen[i] {
		end = i + 1
		for end < len(body) && masks.quotedIdentifier[end] && !masks.quoteOpen[end] {
			end++
		}
		inner := body[i+1 : end]
		if end-1 > i && masks.quoteClose[end-1] {
			inner = body[i+1 : end-1]
		}
		quote := body[i : i+1]
		return strings.ReplaceAll(inner, quote+quote, quote), end, true
	}
	if masks.quotedIdentifier[i] {
		return "", i, false
	}
	if masks.bracketNames && body[i] == '[' {
		return bracketPart(body, i)
	}
	r, size := utf8.DecodeRuneInString(body[i:])
	if r != '_' && !unicode.IsLetter(r) {
		return "", i, false
	}
	end = i + size
	for end < len(body) && masks.code[end] && !masks.quotedIdentifier[end] {
		r, size := utf8.DecodeRuneInString(body[end:])
		if !isNameRune(r) {
			break
		}
		end += size
	}
	return body[i:end], end, true
}

// bracketPart reads the bracket-quoted name part that opens at offset i of
// body, with each doubled closing bracket halved. An unclosed one runs to the
// end of the body.
func bracketPart(body string, i int) (part string, end int, ok bool) {
	var inner strings.Builder
	for end = i + 1; end < len(body); end++ {
		if body[end] != ']' {
			inner.WriteByte(body[end])
			continue
		}
		if end+1 < len(body) && body[end+1] == ']' {
			inner.WriteByte(']')
			end++
			continue
		}
		return inner.String(), end + 1, true
	}
	return inner.String(), end, true
}

// skipNonName moves past the byte or rune at offset i, where no name part
// starts. A word that does not start a name, such as the 1 of `1e5` or the $1
// of a positional parameter, is skipped whole, so its tail is not read as a
// name.
func skipNonName(body string, masks sqlMasks, i int) int {
	if !masks.code[i] || masks.quotedIdentifier[i] {
		return i + 1
	}
	r, size := utf8.DecodeRuneInString(body[i:])
	if !isNameRune(r) {
		return i + size
	}
	for i < len(body) && masks.code[i] && !masks.quotedIdentifier[i] {
		r, size := utf8.DecodeRuneInString(body[i:])
		if !isNameRune(r) {
			break
		}
		i += size
	}
	return i
}

// skipCodeSpace moves past the white space of code that starts at offset i.
func skipCodeSpace(body string, masks sqlMasks, i int) int {
	for i < len(body) && masks.code[i] && !masks.quotedIdentifier[i] && isSpace(body[i]) {
		i++
	}
	return i
}

func isCodeDot(body string, masks sqlMasks, i int) bool {
	return masks.code[i] && !masks.quotedIdentifier[i] && body[i] == '.'
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f' || b == '\v'
}

// isNameRune reports whether r continues a bare name part.
func isNameRune(r rune) bool {
	return r == '_' || r == '$' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
