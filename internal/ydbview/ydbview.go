// Package ydbview holds what YDB does with a view that the renderer, the
// reader and the schema comparison have to agree on: the clause every CREATE
// VIEW carries, and the form in which the server keeps the view's query.
//
// It is one package rather than a constant in the renderer and a fold in the
// comparison because the two answer the same question from opposite ends. The
// renderer writes the query as the declaration spells it, the server stores
// its own form of it, and the comparison has to read both sides into that
// form, or a view applied once is planned again on every run.
package ydbview

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// SecurityClause is the WITH clause every YDB view is created with.
//
// YDB runs a view's query with the rights of the user who reads the view, and
// requires the view to say so. Measured on 25.1.4.7 and 26.2.1.14, a CREATE
// VIEW without the clause and one with `security_invoker = FALSE` are both
// refused with `security_invoker option must be explicitly enabled`, and the
// option name is case-sensitive: `SECURITY_INVOKER = true` is refused the same
// way. The server keeps no record of the clause -- DescribeView returns the
// query alone -- so there is nothing to read back and compare.
const SecurityClause = "WITH (security_invoker = TRUE)"

// QueryText returns the text YDB stores for a view whose query is body.
//
// The server keeps the query's tokens and nothing else: comments are dropped,
// every token is separated from the next by one space, and the semicolons
// that end the statement are gone. Case and the contents of quoted names and
// literals are kept. Measured on 25.1.4.7 and 26.2.1.14 through DescribeView,
// both lines alike:
//
//	select   t.id, `name` -- c
//	  FROM `t` as t where amount>=10
//	-> select t . id , `name` FROM `t` as t where amount >= 10
//
//	SELECT COUNT(*) AS c FROM t GROUP BY name;
//	-> SELECT COUNT ( * ) AS c FROM t GROUP BY name
//
// A pragma the CREATE VIEW ran under is part of the view. The server stores
// each pragma that precedes the statement in its query, as written, ahead of
// the tokens of the view's own query, because a pragma such as TablePathPrefix
// decides which tables the query's names mean (measured on both lines: after
// `PRAGMA TablePathPrefix('/local/app');` the stored text begins with that
// pragma, and the view reads `/local/app/sub/items` for `sub/items`). The form
// keeps those pragmas, so a view created under one never reads as the same
// query without it.
//
// The tokens here are Ptah's YQL lexer's, which splits operators into single
// characters where the server keeps `>=` whole. That is safe because both
// sides of a comparison go through this function: the server's spacing falls
// on token boundaries, and re-reading its text splits it the same way the
// declaration is split. What would not be safe is a token of this lexer that
// spans two of the server's, and the lexer reads identifiers, numbers with
// their suffixes and quoted literals by the grammar's own rules.
func QueryText(body string) string {
	tokens := significant(body)
	for len(tokens) > 0 && tokens[len(tokens)-1].Type == lexer.TokenSemicolon {
		tokens = tokens[:len(tokens)-1]
	}
	words := make([]string, 0, len(tokens))
	for _, token := range tokens {
		words = append(words, token.Value)
	}
	return strings.Join(words, " ")
}

// significant returns the tokens of text that are neither whitespace nor
// comments, read by the YQL lexer.
func significant(text string) []lexer.Token {
	scanner := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.YDB))
	var tokens []lexer.Token
	for {
		token := scanner.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenUnknown:
			continue
		default:
			tokens = append(tokens, token)
		}
	}
}
