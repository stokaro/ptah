package schemalineage

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/lexer"
)

// deriveView resolves one view body into edges, or records why it could not.
//
// The shape it resolves is the one that carries most real lineage: a select
// list over a single FROM source, with columns named plainly or aliased. Every
// other shape becomes an [Undecided] naming what stopped it, because a view
// whose dependencies were not resolved must not be indistinguishable from a
// view with none.
func deriveView(name, body string, materialized bool, columns map[string][]string) Result {
	undecided := func(reason string) Result {
		return Result{Undecided: []Undecided{{View: name, Reason: reason, Materialized: materialized}}}
	}
	tokens := tokenize(body)
	if len(tokens) == 0 {
		return undecided("the body is empty")
	}
	selectStart, fromStart, err := selectAndFrom(tokens)
	if err != nil {
		return undecided(err.Error())
	}
	source, err := singleSource(tokens, fromStart)
	if err != nil {
		return undecided(err.Error())
	}
	items := splitSelectList(tokens[selectStart:fromStart])
	if len(items) == 0 {
		return undecided("the select list is empty")
	}

	var edges []Edge
	for _, item := range items {
		itemEdges, err := resolveSelectItem(item, name, source, materialized, columns)
		if err != nil {
			return undecided(err.Error())
		}
		edges = append(edges, itemEdges...)
	}
	return Result{Edges: edges}
}

// tokenize drops whitespace and comments, which carry no lineage and would
// otherwise have to be skipped at every step below.
func tokenize(body string) []lexer.Token {
	lex := lexer.NewLexer(body)
	var tokens []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenSemicolon:
			continue
		}
		tokens = append(tokens, token)
	}
}

// selectAndFrom locates the select list's bounds: the token after SELECT, and
// the FROM that ends it.
//
// Depth tracking is what keeps a subquery's own FROM from being mistaken for
// this statement's. Without it `SELECT (SELECT x FROM inner) AS c FROM outer`
// would read `inner` as the source.
func selectAndFrom(tokens []lexer.Token) (selectStart, fromStart int, err error) {
	if !tokens[0].MatchIdentifierValue("select") {
		return 0, 0, fmt.Errorf("the body does not begin with SELECT")
	}
	if len(tokens) > 1 && (tokens[1].MatchIdentifierValue("distinct") || tokens[1].MatchIdentifierValue("all")) {
		selectStart = 2
	} else {
		selectStart = 1
	}
	depth := 0
	for i := selectStart; i < len(tokens); i++ {
		switch {
		case tokens[i].MatchOperatorValue("("):
			depth++
		case tokens[i].MatchOperatorValue(")"):
			depth--
		case depth == 0 && tokens[i].MatchIdentifierValue("from"):
			if i == selectStart {
				return 0, 0, fmt.Errorf("the select list is empty")
			}
			return selectStart, i, nil
		}
	}
	return 0, 0, fmt.Errorf("the body has no top-level FROM")
}

// singleSource reads the one table the select list draws from.
//
// A join, a subquery source or a set operation is refused rather than guessed
// at: with more than one source in scope, an unqualified column cannot be
// attributed to a table, and attributing it to the wrong one is worse than
// saying so.
func singleSource(tokens []lexer.Token, fromStart int) (sourceRef, error) {
	rest := tokens[fromStart+1:]
	if len(rest) == 0 {
		return sourceRef{}, fmt.Errorf("the FROM clause names no source")
	}
	if rest[0].MatchOperatorValue("(") {
		return sourceRef{}, fmt.Errorf("the FROM clause is a subquery, which this analysis does not resolve")
	}
	table, consumed, err := qualifiedName(rest)
	if err != nil {
		return sourceRef{}, err
	}
	source := sourceRef{table: table, alias: table}
	rest = rest[consumed:]
	if len(rest) > 0 && rest[0].MatchIdentifierValue("as") {
		rest = rest[1:]
		if len(rest) == 0 {
			return sourceRef{}, fmt.Errorf("the FROM clause ends after AS")
		}
	}
	if len(rest) > 0 && rest[0].Type == lexer.TokenIdentifier && !isClauseKeyword(rest[0]) {
		source.alias = unquote(rest[0].Value)
		rest = rest[1:]
	}
	for _, token := range rest {
		if token.MatchOperatorValue(",") || isJoinKeyword(token) {
			return sourceRef{}, fmt.Errorf(
				"the FROM clause names more than one source, so an unqualified column cannot be attributed")
		}
		if token.MatchIdentifierValue("union") || token.MatchIdentifierValue("intersect") ||
			token.MatchIdentifierValue("except") {
			return sourceRef{}, fmt.Errorf("the body combines queries, which this analysis does not resolve")
		}
	}
	return source, nil
}

// sourceRef is the single table a select list reads, under the name the body
// refers to it by.
type sourceRef struct {
	table string
	alias string
}

func isJoinKeyword(token lexer.Token) bool {
	return slices.ContainsFunc([]string{"join", "inner", "left", "right", "full", "cross", "natural"}, token.MatchIdentifierValue)
}

func isClauseKeyword(token lexer.Token) bool {
	return slices.ContainsFunc([]string{
		"where", "group", "having", "window", "order", "limit", "offset", "fetch", "for",
		"union", "intersect", "except", "join", "inner", "left", "right", "full", "cross", "natural",
	}, token.MatchIdentifierValue)
}

// qualifiedName reads a possibly schema-qualified name and returns its last
// part, which is the table.
func qualifiedName(tokens []lexer.Token) (string, int, error) {
	if len(tokens) == 0 || tokens[0].Type != lexer.TokenIdentifier {
		return "", 0, fmt.Errorf("the FROM clause names no source")
	}
	name := unquote(tokens[0].Value)
	consumed := 1
	for consumed+1 < len(tokens) && tokens[consumed].MatchOperatorValue(".") &&
		tokens[consumed+1].Type == lexer.TokenIdentifier {
		name = unquote(tokens[consumed+1].Value)
		consumed += 2
	}
	return name, consumed, nil
}

func unquote(value string) string {
	return strings.Trim(value, `"`+"`"+`[]`)
}
