package defaultlit

import (
	"slices"
	"strings"
)

// RelativeTimeLiteral reports whether value, a default written as SQL, is a
// string that literals names: a date and time input the engine reads as a
// moment relative to when it reads it, such as PostgreSQL's 'now'. literals is
// in upper case, as the dialect descriptor states it, and the string is
// matched without case and surrounding spaces.
//
// The string may carry casts and parentheses, in either order: `'now'`,
// `'now'::timestamptz`, `('now')` and `('now')::timestamptz` are each the
// literal, because the engine folds each of them when it creates the table.
// A string cast to a string type first, as in `('now'::text)::date`, is not:
// the engine keeps that cast in the default and converts the text each time
// it evaluates it, so every row gets its own moment. For any value that is
// not one quoted string the answer is false. Only a column that holds a
// moment reads the string as one, so a caller asks [DateTimeType] about the
// column first.
func RelativeTimeLiteral(value string, literals []string) bool {
	if len(literals) == 0 {
		return false
	}
	text, cast := unwrap(value)
	return !stringType(cast) && relativeTimeString(text, literals)
}

// relativeTimeString reports whether text is one quoted string whose contents
// literals names ([relativeTimeWord]).
func relativeTimeString(text string, literals []string) bool {
	if len(text) < 2 || text[0] != '\'' || text[len(text)-1] != '\'' || strings.Contains(text[1:len(text)-1], "'") {
		return false
	}
	return relativeTimeWord(text[1:len(text)-1], literals)
}

// RelativeTimeArrayLiteral reports whether value, a default written as SQL for
// an array column, holds an element that literals names ([RelativeTimeLiteral]).
// The array may be a quoted array literal, such as '{now}' or
// '{"epoch", today}', or an ARRAY constructor whose elements the engine reads
// as dates or times, such as ARRAY['today']::date[] or ARRAY['today'::date],
// with casts and parentheses around it, at any depth of nesting. Measured on
// PostgreSQL 18, each of these stores the moment the table was created.
//
// A bare ARRAY['today'] is not one: the engine reads an uncast string element
// as text, and refuses a text array on a date or time array column with its
// own error. Neither is an array cast to a string type first, as in
// '{now}'::text[]::date[], which the engine converts each time it evaluates
// the default ([RelativeTimeLiteral]).
func RelativeTimeArrayLiteral(value string, literals []string) bool {
	if len(literals) == 0 {
		return false
	}
	text, cast := unwrap(value)
	if stringType(cast) {
		return false
	}
	if inner, ok := arrayConstructor(text); ok {
		return constructorHolds(inner, cast != "", literals)
	}
	if len(text) < 2 || text[0] != '\'' || text[len(text)-1] != '\'' {
		return false
	}
	body := text[1 : len(text)-1]
	if strings.Contains(strings.ReplaceAll(body, "''", ""), "'") {
		return false
	}
	return slices.ContainsFunc(arrayLiteralElements(strings.ReplaceAll(body, "''", "'")), func(element string) bool {
		return relativeTimeWord(element, literals)
	})
}

// constructorHolds reports whether an element of an ARRAY constructor, whose
// elements inner holds, is a string literals names that the engine reads as a
// date or time. typed says whether a cast around the constructor types its
// uncast elements: without one an uncast string element is text, which the
// engine never reads as a moment. A nested constructor is typed by its own
// cast, or else by the one around it.
func constructorHolds(inner string, typed bool, literals []string) bool {
	return slices.ContainsFunc(splitTopLevel(inner), func(element string) bool {
		text, cast := unwrap(element)
		if stringType(cast) {
			return false
		}
		if nested, ok := arrayConstructor(text); ok {
			return constructorHolds(nested, typed || cast != "", literals)
		}
		return (typed || cast != "") && relativeTimeString(text, literals)
	})
}

// RelativeTimeDefault reports whether value, a default on a column of the
// built-in type columnType, is one the engine reads once, when it creates the
// table: a string literals names on a date or time type ([DateTimeType],
// [RelativeTimeLiteral]), or an array holding one on an array of such a type
// ([ArrayElementType], [RelativeTimeArrayLiteral]). Resolving a created type,
// such as a domain, to its base is the caller's.
func RelativeTimeDefault(columnType, value string, literals []string) bool {
	if element, ok := ArrayElementType(columnType); ok {
		return DateTimeType(element) && RelativeTimeArrayLiteral(value, literals)
	}
	return DateTimeType(columnType) && RelativeTimeLiteral(value, literals)
}

// ArrayElementType returns the element type of an array type spelling, such
// as timestamptz for `timestamptz[]`, `timestamptz[3][]` or
// `timestamptz ARRAY`, and reports whether spelling names an array. A bracket
// inside a quoted name, as in "a[]", does not make an array.
func ArrayElementType(spelling string) (string, bool) {
	text := strings.TrimSpace(spelling)
	array := false
	for strings.HasSuffix(text, "]") {
		open := strings.LastIndexByte(text, '[')
		if open <= 0 || strings.Trim(text[open+1:len(text)-1], "0123456789 ") != "" {
			return "", false
		}
		text, array = strings.TrimSpace(text[:open]), true
	}
	if lower := strings.ToLower(text); strings.HasSuffix(lower, "array") && len(text) > len("array") {
		if before := text[len(text)-len("array")-1]; before == ' ' || before == '"' || before == ')' {
			text, array = strings.TrimSpace(text[:len(text)-len("array")]), true
		}
	}
	if !array || text == "" {
		return "", false
	}
	return text, true
}

// dateTimeTypes are the built-in date and time types of the PostgreSQL
// family, the engines whose descriptors name relative time literals, as a
// column spells them without a precision.
var dateTimeTypes = map[string]bool{
	"date": true, "time": true, "timetz": true, "time without time zone": true, "time with time zone": true,
	"timestamp": true, "timestamptz": true, "timestamp without time zone": true, "timestamp with time zone": true,
}

// catalogDateTimeTypes are the names the catalog gives the built-in date and
// time types, the only ones a quoted name can spell: quoted, "timestamp with
// time zone" names no type.
var catalogDateTimeTypes = map[string]bool{"date": true, "time": true, "timetz": true, "timestamp": true, "timestamptz": true}

// DateTimeType reports whether spelling names a built-in date or time type of
// the PostgreSQL family, without regard to case, a pg_catalog qualifier or a
// precision: `TIMESTAMPTZ`, `timestamp(3) with time zone` and
// `pg_catalog.date` each do. A quoted part keeps its case, as the server
// reads it: `"pg_catalog"."timestamptz"` and `"timestamp"` name the built-in
// types, and `"Timestamp"` names a type somebody created. Any other name
// answers false, an array and a created type included: resolving a created
// type, such as a domain, is the caller's. An enum named update_kind holds no
// moment, whatever its name says.
func DateTimeType(spelling string) bool {
	return builtinType(spelling, dateTimeTypes, catalogDateTimeTypes)
}

// stringTypes are the built-in string types of the PostgreSQL family, as a
// cast spells them without a length.
var stringTypes = map[string]bool{
	"text": true, "varchar": true, "character varying": true, "char": true, "character": true, "bpchar": true, "name": true,
}

// catalogStringTypes are the names the catalog gives the built-in string
// types, the only ones a quoted name can spell. Quoted, "char" names the
// one-byte internal type, so it is not among them.
var catalogStringTypes = map[string]bool{"text": true, "varchar": true, "bpchar": true, "name": true}

// stringType reports whether spelling, the type a cast names, is a built-in
// string type or an array of one ([DateTimeType] reads the spelling the same
// way). A string cast to one stays a string the engine converts each time it
// evaluates the default. Any other cast, a created type included, answers
// false, so a default cast to a type Ptah cannot resolve is judged by the
// string it holds.
func stringType(spelling string) bool {
	if element, ok := ArrayElementType(spelling); ok {
		spelling = element
	}
	return spelling != "" && builtinType(spelling, stringTypes, catalogStringTypes)
}

// builtinType reports whether spelling names one of the built-in types names
// holds, or catalog holds where a part of the spelling is quoted, without
// regard to case, a pg_catalog qualifier or a precision.
func builtinType(spelling string, names, catalog map[string]bool) bool {
	if strings.Contains(spelling, `"`) {
		return quotedBuiltinType(spelling, catalog)
	}
	name := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(spelling)), "pg_catalog.")
	if open := strings.IndexByte(name, '('); open >= 0 {
		end := strings.IndexByte(name[open:], ')')
		if end < 0 {
			return false
		}
		name = name[:open] + " " + name[open+end+1:]
	}
	return names[strings.Join(strings.Fields(name), " ")]
}

// quotedBuiltinType answers [builtinType] for a spelling with a quoted part:
// a name of one or two parts, the first pg_catalog, and an optional precision.
// A bare part folds to lower case and a quoted part keeps its own.
func quotedBuiltinType(spelling string, catalog map[string]bool) bool {
	parts, rest, ok := identifierParts(spelling)
	if !ok || len(parts) == 0 || len(parts) > 2 || len(parts) == 2 && parts[0] != "pg_catalog" {
		return false
	}
	if rest = strings.TrimSpace(rest); rest != "" &&
		(rest[0] != '(' || rest[len(rest)-1] != ')' || strings.Trim(rest[1:len(rest)-1], "0123456789 ") != "") {
		return false
	}
	return catalog[parts[len(parts)-1]]
}

// identifierParts reads a dotted name from the start of text, each part bare
// or in double quotes, and returns its parts and the text after it. A bare
// part folds to lower case; a quoted part keeps its case, with "" read as one
// quote.
func identifierParts(text string) ([]string, string, bool) {
	var parts []string
	rest := strings.TrimLeft(text, " ")
	for {
		part, after, ok := identifierPart(rest)
		if !ok {
			return nil, "", false
		}
		parts = append(parts, part)
		after = strings.TrimLeft(after, " ")
		if !strings.HasPrefix(after, ".") {
			return parts, after, true
		}
		rest = strings.TrimLeft(after[1:], " ")
	}
}

// identifierPart reads one bare or double-quoted identifier from the start of
// text.
func identifierPart(text string) (part, rest string, ok bool) {
	if strings.HasPrefix(text, `"`) {
		var quoted strings.Builder
		for i := 1; i < len(text); i++ {
			if text[i] != '"' {
				quoted.WriteByte(text[i])
				continue
			}
			if i+1 < len(text) && text[i+1] == '"' {
				quoted.WriteByte('"')
				i++
				continue
			}
			return quoted.String(), text[i+1:], quoted.Len() > 0
		}
		return "", "", false
	}
	end := 0
	for end < len(text) && (text[end] == '_' || text[end] == '$' || text[end] >= 'a' && text[end] <= 'z' ||
		text[end] >= 'A' && text[end] <= 'Z' || end > 0 && text[end] >= '0' && text[end] <= '9') {
		end++
	}
	return strings.ToLower(text[:end]), text[end:], end > 0
}

// unwrap removes the parentheses and casts around value, in either order and
// at any depth, and returns what is left and the type of the innermost cast,
// the one the engine applies first, or "" where there is no cast:
// `('now'::text)::date` becomes `'now'` and text, and
// `ARRAY['today']::date[]` becomes `ARRAY['today']` and date[].
func unwrap(value string) (text, innermost string) {
	text = strings.TrimSpace(value)
	for {
		if enclosed(text) {
			text = strings.TrimSpace(text[1 : len(text)-1])
			continue
		}
		if cast := outerCast(text); cast > 0 {
			text, innermost = strings.TrimSpace(text[:cast]), strings.TrimSpace(text[cast+2:])
			continue
		}
		return text, innermost
	}
}

// outerCast returns where the outermost cast of text begins, or -1 where text
// does not end in one: a :: and a type name after a string, a parenthesis, a
// bracket or another cast, so the cast in 'now'::timestamp::date that names
// date is found, and the :: in a longer expression is not.
func outerCast(text string) int {
	cast := strings.LastIndex(text, "::")
	if cast <= 0 || !isTypeName(text[cast+2:]) {
		return -1
	}
	before := strings.TrimSpace(text[:cast])
	if before != "" && (strings.IndexByte("')]", before[len(before)-1]) >= 0 || outerCast(before) > 0) {
		return cast
	}
	return -1
}

// relativeTimeWord reports whether text, the contents of a string, is a word
// literals names, matched without case and surrounding spaces.
func relativeTimeWord(text string, literals []string) bool {
	return slices.Contains(literals, strings.ToUpper(strings.TrimSpace(text)))
}

// arrayConstructor returns what an ARRAY[...] constructor holds between its
// brackets, and reports whether text is one.
func arrayConstructor(text string) (string, bool) {
	if len(text) < len("ARRAY[]") || !strings.EqualFold(text[:len("ARRAY")], "ARRAY") {
		return "", false
	}
	rest := strings.TrimSpace(text[len("ARRAY"):])
	if len(rest) < 2 || rest[0] != '[' || rest[len(rest)-1] != ']' {
		return "", false
	}
	return rest[1 : len(rest)-1], true
}

// splitTopLevel splits SQL at the commas outside strings, parentheses and
// brackets.
func splitTopLevel(text string) []string {
	var parts []string
	depth, quoted, start := 0, false, 0
	for i := range len(text) {
		switch {
		case text[i] == '\'':
			quoted = !quoted
		case quoted:
		case text[i] == '(' || text[i] == '[':
			depth++
		case text[i] == ')' || text[i] == ']':
			depth--
		case text[i] == ',' && depth == 0:
			parts = append(parts, text[start:i])
			start = i + 1
		}
	}
	return append(parts, text[start:])
}

// arrayLiteralElements returns the elements of the array literal text, such as
// now and epoch for {now,"epoch"} or {{now}}, with double quotes and backslash
// escapes removed. A dimension decoration, as in [1:1]={now}, is skipped, and
// text that is no array literal has no elements.
func arrayLiteralElements(text string) []string {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "[") {
		if _, after, found := strings.Cut(text, "="); found {
			text = strings.TrimSpace(after)
		}
	}
	if !strings.HasPrefix(text, "{") {
		return nil
	}
	var elements []string
	var element strings.Builder
	quoted := false
	for i := 0; i < len(text); i++ {
		switch ch := text[i]; {
		case ch == '\\' && i+1 < len(text):
			i++
			element.WriteByte(text[i])
		case ch == '"':
			quoted = !quoted
		case quoted:
			element.WriteByte(ch)
		case ch == '{' || ch == '}' || ch == ',':
			elements = append(elements, element.String())
			element.Reset()
		default:
			element.WriteByte(ch)
		}
	}
	return elements
}

// enclosed reports whether text is one pair of parentheses around the rest,
// reading a quoted string as one token.
func enclosed(text string) bool {
	if len(text) < 2 || text[0] != '(' || text[len(text)-1] != ')' {
		return false
	}
	depth, quoted := 0, false
	for i := range len(text) {
		switch {
		case text[i] == '\'':
			quoted = !quoted
		case quoted:
		case text[i] == '(':
			depth++
		case text[i] == ')':
			depth--
			if depth == 0 && i < len(text)-1 {
				return false
			}
		}
	}
	return depth == 0 && !quoted
}

// isTypeName reports whether text can be the type a cast names, such as
// timestamptz, timestamp with time zone, numeric(10, 2) or date[], rather
// than more SQL after one. Its parentheses and brackets balance, so the cast
// in ARRAY['today'::date] is not read as one around the constructor.
func isTypeName(text string) bool {
	if strings.TrimSpace(text) == "" {
		return false
	}
	depth := 0
	for _, ch := range []byte(text) {
		letter := ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9'
		if !letter && !strings.ContainsRune(" _(),[]\".", rune(ch)) {
			return false
		}
		depth += strings.Count("([", string(ch)) - strings.Count(")]", string(ch))
		if depth < 0 {
			return false
		}
	}
	return depth == 0
}
