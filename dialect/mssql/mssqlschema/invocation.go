package mssqlschema

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/core/schemaext"
)

// ParseInvocation reads a predicate written as a call of an inline
// table-valued function: a two-part function name and its argument list. It
// reads the spelling a declaration writes, `dbo.fn_tenant(tenant_id)`, and the
// one sys.security_predicates reports, `([dbo].[fn_tenant]([tenant_id]))`.
//
// Parentheses around the whole call are dropped. A name part may be plain,
// bracketed or double-quoted, and is returned without its quoting. Each
// argument is returned as written, without the white space around it, in the
// order the call lists them; a call with no arguments returns none.
//
// SQL Server takes nothing else as a predicate: an inline expression is
// `Incorrect syntax near '('`, and a one-part function name is refused because
// a schema-bound policy names its function in two parts, both measured on SQL
// Server 2025. A text of another shape, a one-part or three-part name, or text
// after the argument list is refused with [schemaext.ErrInvalidValue].
func ParseInvocation(text string) (ObjectName, []string, error) {
	call := strings.TrimSpace(text)
	for enclosed(call) {
		call = strings.TrimSpace(call[1 : len(call)-1])
	}
	var parts []string
	rest := call
	for {
		part, width, ok := namePart(rest)
		if !ok {
			return ObjectName{}, nil, invocationError(text)
		}
		parts = append(parts, part)
		rest = strings.TrimLeftFunc(rest[width:], unicode.IsSpace)
		if !strings.HasPrefix(rest, ".") {
			break
		}
		rest = strings.TrimLeftFunc(rest[1:], unicode.IsSpace)
	}
	if len(parts) != 2 || !strings.HasPrefix(rest, "(") {
		return ObjectName{}, nil, invocationError(text)
	}
	arguments, ok := argumentList(rest)
	if !ok {
		return ObjectName{}, nil, invocationError(text)
	}
	return ObjectName{Schema: parts[0], Name: parts[1]}, arguments, nil
}

// CatalogPredicate reads one row of sys.security_predicates: the schema and
// name of the table the predicate binds, its predicate_definition,
// predicate_type_desc and operation_desc, the last empty where the catalog
// reports NULL. The arguments keep the catalog's spelling (see
// [CompareArgument]). A definition that is not a call of a two-part function
// (see [ParseInvocation]), a type other than FILTER or BLOCK, and a filter
// predicate with an operation are refused with [schemaext.ErrInvalidValue].
func CatalogPredicate(tableSchema, table, definition, kind, operation string) (Predicate, error) {
	function, arguments, err := ParseInvocation(definition)
	if err != nil {
		return Predicate{}, err
	}
	predicate := Predicate{
		Type: PredicateType(strings.ToUpper(kind)), Function: function, Arguments: arguments,
		Table: ObjectName{Schema: tableSchema, Name: table}, Operation: BlockOperation(strings.ToUpper(operation)),
	}
	if err := validatePredicate(predicate); err != nil {
		return Predicate{}, err
	}
	return predicate, nil
}

// Invocation writes a predicate's function call as T-SQL takes it: the
// two-part name with each part bracketed, and the arguments as the predicate
// holds them. [ParseInvocation] reads it back to the same function and
// arguments.
func (p Predicate) Invocation() string {
	return p.Function.String() + "(" + strings.Join(p.Arguments, ", ") + ")"
}

func invocationError(text string) error {
	return fmt.Errorf("%w: %q is not a call of a two-part inline table-valued function, the only predicate a "+
		"security policy takes", schemaext.ErrInvalidValue, text)
}

// enclosed reports whether text is wrapped in one pair of parentheses that
// close only at its end.
func enclosed(text string) bool {
	if len(text) < 2 || text[0] != '(' || text[len(text)-1] != ')' {
		return false
	}
	end, ok := closingParenthesis(text)
	return ok && end == len(text)-1
}

// namePart reads one part of a name at the start of text: a bracketed or
// double-quoted identifier without its quoting, or a plain one.
func namePart(text string) (string, int, bool) {
	if text == "" {
		return "", 0, false
	}
	switch text[0] {
	case '[':
		part, width, ok := delimited(text, ']')
		return part.value, width, ok && strings.TrimSpace(part.value) != ""
	case '"':
		part, width, ok := delimited(text, '"')
		return part.value, width, ok && strings.TrimSpace(part.value) != ""
	}
	first, size := utf8.DecodeRuneInString(text)
	if !unicode.IsLetter(first) && first != '_' && first != '@' && first != '#' {
		return "", 0, false
	}
	end := size
	for end < len(text) {
		r, width := utf8.DecodeRuneInString(text[end:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !strings.ContainsRune("_@#$", r) {
			break
		}
		end += width
	}
	return text[:end], end, true
}

// argumentList splits an argument list that starts text and ends it at the
// commas outside parentheses, quotes and brackets.
func argumentList(text string) ([]string, bool) {
	end, ok := closingParenthesis(text)
	if !ok || strings.TrimSpace(text[end+1:]) != "" {
		return nil, false
	}
	body := text[1:end]
	if strings.TrimSpace(body) == "" {
		return nil, true
	}
	var arguments []string
	depth, start := 0, 0
	for i := 0; i < len(body); i++ {
		switch body[i] {
		case '[', '"', '\'':
			width, ok := quotedWidth(body[i:])
			if !ok {
				return nil, false
			}
			i += width - 1
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				arguments = append(arguments, strings.TrimSpace(body[start:i]))
				start = i + 1
			}
		}
	}
	arguments = append(arguments, strings.TrimSpace(body[start:]))
	if slices.Contains(arguments, "") {
		return nil, false
	}
	return arguments, true
}

// closingParenthesis returns the index of the parenthesis that closes the one
// text starts with, skipping quoted text.
func closingParenthesis(text string) (int, bool) {
	depth := 0
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '[', '"', '\'':
			width, ok := quotedWidth(text[i:])
			if !ok {
				return 0, false
			}
			i += width - 1
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return 0, false
}

// quotedWidth is the width of the bracketed identifier, double-quoted
// identifier or string literal text starts with.
func quotedWidth(text string) (int, bool) {
	closer := text[0]
	if closer == '[' {
		closer = ']'
	}
	_, width, ok := delimited(text, closer)
	return width, ok
}
