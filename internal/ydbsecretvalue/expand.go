// Package ydbsecretvalue defines a YDB secret's value in the query the YDB
// connection sends, reading it from the environment variable the statement
// names, and keeps that value out of whatever the connection reports. The
// declaration grammar and the statements live in
// [ptah.run/dialect/ydb/ydbsecret]; this package is execution machinery and
// never part of a schema model.
package ydbsecretvalue

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbsyntax"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Expansion is a query whose secret values are defined in it, ready for the
// server, together with the values it defined, so a caller can keep them out
// of whatever it reports about the query.
type Expansion struct {
	// Text is the query as the server receives it.
	Text string
	// values are the values Text defines, longest first.
	values []string
}

// Redact returns text with every value the expansion defined written as
// `[secret]`. An error the server answers the query with goes through here
// before anything prints it.
func (e Expansion) Redact(text string) string {
	for _, value := range e.values {
		if value != "" {
			text = strings.ReplaceAll(text, value, "[secret]")
		}
	}
	return text
}

// Defines reports whether the expansion defined any value, which is when the
// text the server receives differs from the text the caller passed.
func (e Expansion) Defines() bool {
	return len(e.values) > 0
}

// ErrReference is the error [Expand] wraps when a query refers to a secret
// value where Ptah does not define it.
var ErrReference = errors.New("a secret value is referred to outside the value of CREATE SECRET or ALTER SECRET")

// Expand defines, in query, every secret value it refers to, reading each from
// the environment variable that names it through lookup, and returns the query
// unchanged when it refers to none.
//
// A reference is a named expression `$<variable>` whose name starts with
// [ydbsecret.ValuePrefix]. Expand defines one only where the query uses it as the value
// of `CREATE SECRET <path> WITH (value = ...)` or `ALTER SECRET <path> WITH
// (value = ...)`, the statements [ydbsecret.CreateStatement] and [ydbsecret.AlterStatement] write,
// and refuses the whole query when a reference stands anywhere else -- in
// another statement, in a definition, in a block -- so a value only ever
// reaches a secret, which nothing reads back. A query that defines such a
// name itself is refused the same way. A variable that is not set refuses the
// query, naming the variable; one that is set and empty is the empty value.
//
// The definitions are written in front of the first statement that uses
// them, after any translation setting and PRAGMA ahead of it, and each value
// is a String literal: YDB takes a secret's value as a String and refuses a
// Utf8 one (`String (or named expression with type String) was expected`).
func Expand(query string, lookup func(string) (string, bool)) (Expansion, error) {
	if !mentionsPrefix(query) {
		return Expansion{Text: query}, nil
	}
	tokens := significantTokens(query)
	accepted, firstStatement := valuePositions(tokens)
	var variables []string
	for i, token := range tokens {
		variable, isReference := referenceName(token)
		if !isReference {
			continue
		}
		if !accepted[i] {
			return Expansion{}, fmt.Errorf("%w: %s at offset %d; Ptah defines it only there", ErrReference,
				token.Value, token.Start)
		}
		if err := ydbsecret.CheckValueEnv(variable); err != nil {
			return Expansion{}, fmt.Errorf("secret value %s: %w", token.Value, err)
		}
		if !slices.Contains(variables, variable) {
			variables = append(variables, variable)
		}
	}
	if len(variables) == 0 {
		return Expansion{Text: query}, nil
	}
	var definitions strings.Builder
	values := make([]string, 0, len(variables))
	for _, variable := range variables {
		value, set := lookup(variable)
		if !set {
			return Expansion{}, fmt.Errorf("a secret's value comes from environment variable %s, which is not set",
				variable)
		}
		literal := ydbsyntax.StringLiteral(value)
		values = append(values, redactionForms(value, literal)...)
		fmt.Fprintf(&definitions, "%s = %s;\n", ydbsecret.Reference(variable), literal)
	}
	slices.SortFunc(values, func(a, b string) int { return len(b) - len(a) })
	insertAt := tokens[firstStatement].Start
	return Expansion{
		Text:   query[:insertAt] + definitions.String() + query[insertAt:],
		values: values,
	}, nil
}

// redactionForms includes the bytes sent in YQL and their quoted diagnostic
// form. An SDK error can print the expanded query with %q; looking only for
// the original value would expose a value containing escapes in that error.
func redactionForms(value, literal string) []string {
	escaped := literal[1 : len(literal)-1]
	quotedValue := strconv.Quote(value)
	quotedEscaped := strconv.Quote(escaped)
	return []string{value, escaped, quotedValue[1 : len(quotedValue)-1], quotedEscaped[1 : len(quotedEscaped)-1]}
}

// References returns the variables query refers to as secret values, each once
// and in the order the query first names them, wherever the references stand.
// A reader that reports which variables a migration needs asks this; whether
// the references are where [Expand] defines them is Expand's question.
func References(query string) []string {
	if !mentionsPrefix(query) {
		return nil
	}
	var variables []string
	for _, token := range significantTokens(query) {
		if variable, ok := referenceName(token); ok && !slices.Contains(variables, variable) {
			variables = append(variables, variable)
		}
	}
	return variables
}

// referenceName returns the variable a token refers to as a secret value: a
// named expression whose name starts with [ydbsecret.ValuePrefix] in any case. YQL
// resolves a name by its case (measured on 26.2.1.14: `$PTAH_SECRET_X = 'v';
// SELECT $ptah_secret_x` answers `Unknown name: $ptah_secret_x`), so folding
// only widens what the rules hold, and [ydbsecret.CheckValueEnv] then refuses any
// spelling but the exact one.
func referenceName(token lexer.Token) (string, bool) {
	if token.Type != lexer.TokenIdentifier || len(token.Value) <= len(ydbsecret.ValuePrefix) ||
		!strings.EqualFold(token.Value[:len(ydbsecret.ValuePrefix)+1], "$"+ydbsecret.ValuePrefix) {
		return "", false
	}
	return token.Value[1:], true
}

// mentionsPrefix reports whether query may hold a reference at all, a cheap
// test that spares the lexer every query that names no secret value.
func mentionsPrefix(query string) bool {
	return strings.Contains(strings.ToUpper(query), "$"+ydbsecret.ValuePrefix)
}

// significantTokens reads query as YQL, leaving out whitespace and comments.
func significantTokens(query string) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(query, dialectlexer.Options(platform.YDB))
	var tokens []lexer.Token
	for {
		token := lexr.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		}
		tokens = append(tokens, token)
	}
}

// valuePositions finds, among tokens, the ones that stand as the value of a
// CREATE SECRET or ALTER SECRET statement, and the index of the first token
// of the first statement holding such a value, which is where the definitions
// go. A statement is the tokens between two semicolons outside parentheses.
func valuePositions(tokens []lexer.Token) (map[int]bool, int) {
	accepted := make(map[int]bool)
	first := -1
	start := 0
	depth := 0
	for i := 0; i <= len(tokens); i++ {
		end := i == len(tokens)
		if !end {
			switch {
			case tokens[i].MatchOperatorValue("("):
				depth++
			case tokens[i].MatchOperatorValue(")"):
				depth--
			}
		}
		if !end && (tokens[i].Type != lexer.TokenSemicolon || depth != 0) {
			continue
		}
		if positions := secretValues(tokens[start:i]); len(positions) > 0 {
			for _, position := range positions {
				accepted[start+position] = true
			}
			if first < 0 {
				first = start
			}
		}
		start = i + 1
	}
	if first < 0 {
		first = 0
	}
	return accepted, first
}

// secretValues returns the positions, in one statement's tokens, of the value
// of a CREATE SECRET or ALTER SECRET statement that a reference may stand in:
// the token after `value =` directly inside the WITH list, when it is the
// whole value. Any other statement has none.
func secretValues(statement []lexer.Token) []int {
	var positions []int
	for _, j := range secretValueOptions(statement) {
		separator := statement[j-3]
		whole := j+1 < len(statement) &&
			(statement[j+1].MatchOperatorValue(",") || statement[j+1].MatchOperatorValue(")"))
		if (separator.MatchOperatorValue("(") || separator.MatchOperatorValue(",")) && whole {
			positions = append(positions, j)
		}
	}
	return positions
}

// secretHeader reads `CREATE SECRET`, `CREATE OR REPLACE SECRET` (with an
// optional IF NOT EXISTS) or `ALTER SECRET` (with an optional IF EXISTS) at
// the start of statement, and returns the index of the path after it.
func secretHeader(statement []lexer.Token) (int, bool) {
	words := func(i int, want ...string) bool {
		if i+len(want) > len(statement) {
			return false
		}
		for k, word := range want {
			if !keyword(statement[i+k], word) {
				return false
			}
		}
		return true
	}
	var i int
	switch {
	case words(0, "CREATE", "OR", "REPLACE", "SECRET"):
		i = 4
	case words(0, "CREATE", "SECRET"), words(0, "ALTER", "SECRET"):
		i = 2
	default:
		return 0, false
	}
	switch {
	case keyword(statement[0], "CREATE") && words(i, "IF", "NOT", "EXISTS"):
		i += 3
	case keyword(statement[0], "ALTER") && words(i, "IF", "EXISTS"):
		i += 2
	}
	return i, i < len(statement)
}

// keyword reports whether token is the bare word word, in any case.
func keyword(token lexer.Token, word string) bool {
	return token.Type == lexer.TokenIdentifier && strings.EqualFold(token.Value, word)
}

// ClearValue reports whether statement, one YQL statement, puts a secret's
// value into its own text: a CREATE SECRET or ALTER SECRET whose value is
// anything but a reference [Expand] defines, or any statement that creates or
// changes the deprecated `OBJECT ... (TYPE SECRET)`, which Ptah defines no
// value for and whose value the server keeps readable in .metadata. It
// returns the statement's form and the secret's path as written, and never
// the value, so a caller can report the statement without repeating it.
//
// It reads the statement with the same rules [Expand] reads a reference
// with, so a statement Expand defines a value for is never reported here.
func ClearValue(statement string) (form, path string, writes bool) {
	tokens := significantTokens(statement)
	if form, path, legacy := legacySecretObject(tokens); legacy {
		return form, path, true
	}
	header, ok := secretHeader(tokens)
	if !ok {
		return "", "", false
	}
	form = strings.ToUpper(tokens[0].Value) + " SECRET"
	path = identifierText(tokens[header])
	for _, position := range secretValueOptions(tokens) {
		variable, isReference := referenceName(tokens[position])
		if !isReference || ydbsecret.CheckValueEnv(variable) != nil {
			return form, path, true
		}
	}
	return form, path, false
}

// secretValueOptions returns the positions, in one statement's tokens, of
// every `value` option's value directly inside the WITH list of a CREATE
// SECRET or ALTER SECRET, whatever the value is.
func secretValueOptions(statement []lexer.Token) []int {
	i, ok := secretHeader(statement)
	if !ok {
		return nil
	}
	i++
	if i+1 >= len(statement) || !keyword(statement[i], "WITH") || !statement[i+1].MatchOperatorValue("(") {
		return nil
	}
	var positions []int
	depth := 0
	for j := i + 1; j < len(statement); j++ {
		switch {
		case statement[j].MatchOperatorValue("("):
			depth++
			continue
		case statement[j].MatchOperatorValue(")"):
			depth--
			continue
		}
		if depth == 1 && j >= i+4 && statement[j-1].MatchOperatorValue("=") && keyword(statement[j-2], "value") {
			positions = append(positions, j)
		}
	}
	return positions
}

// legacySecretObject reads `CREATE|ALTER|UPSERT OBJECT <name> (TYPE SECRET)`
// at the start of statement and returns its form and the name as written.
// DROP OBJECT carries no value and is not one.
func legacySecretObject(statement []lexer.Token) (form, name string, ok bool) {
	if len(statement) < 7 || !keyword(statement[1], "OBJECT") {
		return "", "", false
	}
	verb := strings.ToUpper(statement[0].Value)
	if verb != "CREATE" && verb != "ALTER" && verb != "UPSERT" {
		return "", "", false
	}
	i := 2
	if verb == "CREATE" && len(statement) > 5 && keyword(statement[2], "IF") {
		i = 5
	}
	if i+4 >= len(statement) || !statement[i+1].MatchOperatorValue("(") || !keyword(statement[i+2], "TYPE") ||
		!keyword(statement[i+3], "SECRET") || !statement[i+4].MatchOperatorValue(")") {
		return "", "", false
	}
	return verb + " OBJECT ... (TYPE SECRET)", identifierText(statement[i]), true
}

// identifierText is a name token as a reader would write it: a backticked
// path without its quotes.
func identifierText(token lexer.Token) string {
	if name, ok := lexer.YQLIdentifierValue(token.Value); ok {
		return name
	}
	return token.Value
}
