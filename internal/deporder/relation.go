package deporder

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/tableref"
)

// ReferencesIdentifier reports whether the SQL body references name as a
// standalone or qualified identifier. It is [ReferencesRelation] with nothing
// known about the session, so an unqualified name may be in any schema, and
// it backs the schema-scope cross-reference diagnostics and the PostgreSQL
// planner's questions about what a drop takes with it where no comparison
// says where names resolve.
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
	return ReferencesRelation(body, name, identifier.Semantics{})
}

// ReferencesRelation reports whether a name in the code of the SQL body can
// name relation, a bare or schema-qualified name in [tableref] form, where
// names resolve as semantics says.
//
// A name in the body is a run of parts joined by dots, each part bare or
// quoted, with spaces allowed around a dot. It is matched from its end: its
// last part against the relation's own name, and the part before that, where
// there is one, against the relation's schema. A part in front of those, such
// as a database, is not compared, and a name that ends in a column, such as
// `order_totals.total`, does not name the relation `order_totals`. The body
// `FROM "reporting".order_totals` reads reporting.order_totals.
//
// Parts compare as [strings.ToLower] folds them, quoted or not. PostgreSQL
// folds an unquoted part to lower case and keeps a quoted one as written, but
// a relation name a caller passes does not record whether its declaration
// quoted it, and the planners that ask also serve engines whose quoted names
// ignore case. Folding both costs a rebuild or a refusal where two relations
// differ only in case; telling them apart would cost a reader the drop takes
// without a word.
//
// Two schemas written out are one schema only when they are equal:
// `reporting.item_skus` never reads public.item_skus. An unqualified name means
// one thing in relation and another in the body:
//
//   - A relation named without a schema is in semantics.DefaultSchema, where
//     the planner creates an object declared without one. With public the
//     default, `public.item_skus` reads item_skus and `reporting.item_skus`
//     does not.
//   - A name written without a schema in the body may be in any schema of
//     semantics.SearchPath. PostgreSQL resolves it to the first schema of the
//     path that has it, and prints back without a schema every relation the
//     path reaches: under `search_path=reporting,public` a policy that reads
//     public.order_totals is read back as `order_totals`. A bare
//     `order_totals` then reads public.order_totals and
//     reporting.order_totals, and no order_totals in a schema off the path.
//
// What semantics leaves out errs toward "reads it": without a default schema,
// an unqualified relation may be in any schema, and without a search path, so
// may an unqualified name in the body. Everything that reads a dropped
// relation is either rebuilt or refused, and what is missed is dropped with
// CASCADE.
//
// Only the code of the body counts; see [ReferencesIdentifier]. A relation that
// [tableref.Parse] does not accept names nothing.
func ReferencesRelation(body, relation string, semantics identifier.Semantics) bool {
	ref, ok := tableref.Parse(relation)
	if !ok {
		return false
	}
	return namesRead(codeNames(body, ""), ref, sessionResolution(semantics))
}

// resolution says where an unqualified name may be, for each side of a
// pairing. The two differ: an object is created in one schema, while a name
// is looked up along a path.
type resolution struct {
	// relationIn reports whether a relation named without a schema may be in
	// schema, which a qualified name in the body names.
	relationIn func(schema string) bool
	// nameReaches reports whether a name written without a schema in the body
	// may name a relation in schema.
	nameReaches func(schema string) bool
}

// sessionResolution is where names resolve in a session described by
// semantics; see [ReferencesRelation].
func sessionResolution(semantics identifier.Semantics) resolution {
	defaultSchema := foldName(semantics.DefaultSchema)
	path := make([]string, 0, len(semantics.SearchPath))
	for _, schema := range semantics.SearchPath {
		path = append(path, foldName(schema))
	}
	return resolution{
		relationIn: func(schema string) bool {
			return defaultSchema == "" || foldName(schema) == defaultSchema
		},
		nameReaches: func(schema string) bool {
			return len(path) == 0 || slices.Contains(path, foldName(schema))
		},
	}
}

// namesRead reports whether one of names, read from a body and folded, names
// ref, with unqualified names resolved by where.
func namesRead(names []dottedName, ref tableref.Ref, where resolution) bool {
	own := foldName(ref.Name)
	for _, name := range names {
		if name[len(name)-1] == own && namesSchema(name, ref, where) {
			return true
		}
	}
	return false
}

// namesSchema reports whether name, whose last part already equals ref's own
// name, is in ref's schema.
func namesSchema(name dottedName, ref tableref.Ref, where resolution) bool {
	qualified := len(name) > 1
	switch {
	case !qualified && !ref.Qualified:
		return true
	case !qualified:
		return where.nameReaches(ref.Schema)
	case ref.Qualified:
		return name[len(name)-2] == foldName(ref.Schema)
	default:
		return where.relationIn(name[len(name)-2])
	}
}

// foldName is how a name compares here: as [codeNames] folds the body, so a
// name and its spelling in a body fold alike. [strings.EqualFold] does not
// agree with it: `İ` lower-cases to two runes and folds to none of them.
func foldName(name string) string {
	return strings.ToLower(name)
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
