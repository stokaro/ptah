package toschema

import (
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/lexer"
)

// A CREATE INDEX key list element is more than a column name. PostgreSQL's
// grammar is
//
//	{ column | ( expression ) } [ opclass [ ( param = value [, ...] ) ] ]
//	                            [ ASC | DESC ] [ NULLS { FIRST | LAST } ]
//
// and Ptah's own PostgreSQL renderer writes every one of those suffixes. The
// SQL frontend keeps each element as one opaque string, and until #1242 this
// converter classified anything that was not a bare identifier as an
// EXPRESSION, so a file Ptah had just written came back with the suffix glued
// to the key. Measured on live PostgreSQL 17.10, a database holding
//
//	CREATE INDEX i ON t USING gist (tsv tsvector_ops (siglen = 64))
//
// inspected to `USING gist ("tsv" tsvector_ops(siglen=64))`, and diffing the
// database against that very file planned
//
//	DROP INDEX IF EXISTS "i";
//	CREATE INDEX IF NOT EXISTS "i" ON "t" USING gist (("tsv" tsvector_ops(siglen=64)));
//
// -- a rebuild of an identical index whose CREATE psql refuses with `syntax
// error at or near "tsvector_ops"`, so applying the plan dropped the index and
// put nothing back. The same shape hit `("code" text_pattern_ops)`,
// `("created_at" DESC NULLS LAST)` and `("score" NULLS FIRST)`, which is the
// class: every index key suffix the renderer can write, the SQL surface could
// not read.
//
// The decomposition is deliberately all-or-nothing per key list. An element it
// does not fully understand leaves the whole list on the legacy path exactly as
// before, and a list where nothing carries a suffix is left there too, so
// nothing that used to convert one way starts converting another.

// decomposeIndexKeyList converts raw key list elements into structured parts.
//
// It reports nil unless every element decomposes AND at least one of them
// carries a suffix the element text alone cannot express. Both halves matter: a
// partial conversion would silently re-quote or reorder keys nothing asked it
// to touch, and converting a plain key list would change how existing documents
// render for no gain.
func decomposeIndexKeyList(columns []string) []goschema.IndexPart {
	parts := make([]goschema.IndexPart, 0, len(columns))
	suffixed := false
	for _, column := range columns {
		part, hasSuffix, ok := decomposeIndexKeyElement(column)
		if !ok {
			return nil
		}
		suffixed = suffixed || hasSuffix
		parts = append(parts, part)
	}
	if !suffixed {
		return nil
	}
	return parts
}

// decomposeIndexKeyElement splits one key list element into its reference and
// its suffixes. It reports whether the element carried a suffix at all, and
// whether it was understood well enough to replace the raw text.
func decomposeIndexKeyElement(column string) (part goschema.IndexPart, suffixed, ok bool) {
	tokens := indexKeyTokens(column)
	if len(tokens) == 0 {
		return goschema.IndexPart{}, false, false
	}
	// A per-key COLLATE is a suffix this model has no slot for. Leaving the
	// element opaque keeps it in the text where a reader can still see it,
	// which is strictly better than dropping it on the way through.
	for _, token := range tokens {
		if token.MatchIdentifierValue("COLLATE") {
			return goschema.IndexPart{}, false, false
		}
	}

	end := len(tokens)
	if order, consumed := trailingNullsOrder(tokens[:end]); consumed > 0 {
		part.NullsOrder = order
		end -= consumed
		suffixed = true
	}
	if direction, consumed := trailingSortDirection(tokens[:end]); consumed > 0 {
		part.Desc = direction
		end -= consumed
		suffixed = true
	}
	if class, consumed := trailingOperatorClass(tokens[:end]); consumed > 0 {
		part.Operator = class
		end -= consumed
		suffixed = true
	}

	head := tokens[:end]
	switch {
	case len(head) == 1 && isIndexKeyNameToken(head[0]):
		part.Name = normalizeSQLIdentifier(head[0].Value)
		return part, suffixed, true
	case isBalancedGroup(head):
		part.Expr = strings.TrimSpace(column[head[0].End:head[len(head)-1].Start])
		return part, suffixed, part.Expr != ""
	default:
		return goschema.IndexPart{}, false, false
	}
}

// indexKeyTokens lexes one key list element, dropping whitespace. Offsets are
// relative to the element text, which is what the expression span below cuts
// from.
func indexKeyTokens(column string) []lexer.Token {
	scanner := lexer.NewLexer(column)
	var tokens []lexer.Token
	for {
		token := scanner.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
		default:
			tokens = append(tokens, token)
		}
	}
}

// isIndexKeyNameToken reports whether a token can be a column name in a key
// list. The lexer hands a double-quoted identifier back as a string token, so a
// key list written by Ptah's PostgreSQL renderer, which quotes every column, is
// made of string tokens rather than identifier tokens.
func isIndexKeyNameToken(token lexer.Token) bool {
	return token.Type == lexer.TokenIdentifier || isDoubleQuotedIdentifier(token)
}

// isDoubleQuotedIdentifier reports the PostgreSQL-family quoting of a name. A
// backtick or bracket spelling is MySQL's or SQL Server's, and neither dialect
// has operator classes; see trailingOperatorClass.
func isDoubleQuotedIdentifier(token lexer.Token) bool {
	return token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`)
}

// trailingNullsOrder reads a NULLS FIRST or NULLS LAST suffix and reports how
// many tokens it spans.
func trailingNullsOrder(tokens []lexer.Token) (string, int) {
	if len(tokens) < 3 || !tokens[len(tokens)-2].MatchIdentifierValue("NULLS") {
		return "", 0
	}
	switch {
	case tokens[len(tokens)-1].MatchIdentifierValue("FIRST"):
		return goschema.NullsOrderFirst, 2
	case tokens[len(tokens)-1].MatchIdentifierValue("LAST"):
		return goschema.NullsOrderLast, 2
	default:
		return "", 0
	}
}

// trailingSortDirection reads an ASC or DESC suffix. ASC is consumed and
// reported as "not descending" rather than left in the text: it is the default
// on every dialect Ptah renders, and PostgreSQL's own catalog reports an
// ascending key by saying nothing.
func trailingSortDirection(tokens []lexer.Token) (desc bool, consumed int) {
	if len(tokens) < 2 {
		return false, 0
	}
	switch {
	case tokens[len(tokens)-1].MatchIdentifierValue("DESC"):
		return true, 1
	case tokens[len(tokens)-1].MatchIdentifierValue("ASC"):
		return false, 1
	default:
		return false, 0
	}
}

// trailingOperatorClass reads an operator class, with or without a parameter
// list, off the end of a key element.
//
// This converter is dialect-neutral, so the discrimination is by spelling
// rather than by a dialect name: the key the class follows has to be spelled
// the PostgreSQL-family way, bare or double-quoted. An operator class exists
// only in that family, a backtick- or bracket-quoted key belongs to a dialect
// that has none, and MySQL's `name(10)` prefix length is not a class because
// its head is neither one identifier nor a parenthesized group.
//
// The parameter list is re-spelled `name(a=1, b=2)` rather than copied out of
// the input, because that is exactly how the live-database reader spells the
// same class -- `pg_opclass.opcname` joined to `pg_attribute.attoptions` with
// ", " -- and the two spellings are compared to each other. Copying the input
// would make `tsvector_ops (siglen = 64)` differ from the catalog's
// `tsvector_ops(siglen=64)` and rebuild the index forever.
func trailingOperatorClass(tokens []lexer.Token) (string, int) {
	if len(tokens) < 2 || !isPostgresKeySpelling(tokens[0]) {
		return "", 0
	}
	last := len(tokens) - 1
	if tokens[last].Type == lexer.TokenIdentifier {
		return tokens[last].Value, 1
	}
	if !tokens[last].MatchOperatorValue(")") {
		return "", 0
	}
	open := matchingOpenParen(tokens, last)
	// open == 1 would make the whole element a function call -- `lower(name)`
	// -- which is the key itself and not a class applied to one.
	if open < 2 || tokens[open-1].Type != lexer.TokenIdentifier {
		return "", 0
	}
	params, ok := operatorClassParams(tokens[open+1 : last])
	if !ok {
		return "", 0
	}
	return tokens[open-1].Value + "(" + params + ")", len(tokens) - (open - 1)
}

// isPostgresKeySpelling reports whether an element begins the way a
// PostgreSQL-family key does: a bare identifier, a double-quoted one, or an
// opening parenthesis for an expression.
//
// A backtick or a bracket is MySQL's or SQL Server's quoting, and this lexer
// hands `name` back as one identifier token, so the quote character has to be
// looked at rather than the token kind.
func isPostgresKeySpelling(token lexer.Token) bool {
	if strings.HasPrefix(token.Value, "`") || strings.HasPrefix(token.Value, "[") {
		return false
	}
	return isIndexKeyNameToken(token) || token.MatchOperatorValue("(")
}

// operatorClassParams re-spells the inside of an operator class parameter list
// as the catalog reports it. It refuses anything that is not a comma-separated
// list of `name = value`, because a shape it cannot re-spell is a shape it
// cannot compare.
func operatorClassParams(tokens []lexer.Token) (string, bool) {
	if len(tokens) == 0 {
		return "", false
	}
	entries := make([]string, 0, (len(tokens)+3)/4)
	for position := 0; position < len(tokens); position += 4 {
		if position+2 >= len(tokens) {
			return "", false
		}
		name := tokens[position]
		if name.Type != lexer.TokenIdentifier || !tokens[position+1].MatchOperatorValue("=") {
			return "", false
		}
		value := tokens[position+2]
		if value.Type == lexer.TokenOperator {
			return "", false
		}
		entries = append(entries, name.Value+"="+unquoteIndexOptionValue(value.Value))
		if position+3 < len(tokens) && !tokens[position+3].MatchOperatorValue(",") {
			return "", false
		}
	}
	return strings.Join(entries, ", "), true
}

// unquoteIndexOptionValue strips the quoting PostgreSQL puts around an index
// option value. `siglen='64'` and `siglen=64` are the same parameter, and
// `pg_attribute.attoptions` reports the unquoted one; recording the quotes
// would rebuild the index for the difference between two spellings of a number.
func unquoteIndexOptionValue(value string) string {
	if unquoted, err := strconv.Unquote(value); err == nil {
		return unquoted
	}
	if len(value) >= 2 && strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		value = strings.TrimSuffix(strings.TrimPrefix(value, "'"), "'")
		return strings.ReplaceAll(value, "''", "'")
	}
	return value
}

// isBalancedGroup reports whether the tokens are one parenthesized group and
// nothing else, so that `(lower(name))` is a group and `(a) || (b)` is not.
func isBalancedGroup(tokens []lexer.Token) bool {
	if len(tokens) < 3 || !tokens[0].MatchOperatorValue("(") || !tokens[len(tokens)-1].MatchOperatorValue(")") {
		return false
	}
	return matchingOpenParen(tokens, len(tokens)-1) == 0
}

// matchingOpenParen reports the index of the "(" that the ")" at closeIndex
// closes, or -1 when the tokens are unbalanced.
func matchingOpenParen(tokens []lexer.Token, closeIndex int) int {
	depth := 0
	for position := closeIndex; position >= 0; position-- {
		switch {
		case tokens[position].MatchOperatorValue(")"):
			depth++
		case tokens[position].MatchOperatorValue("("):
			depth--
			if depth == 0 {
				return position
			}
		}
	}
	return -1
}
