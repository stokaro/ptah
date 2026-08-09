package parser

import (
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/lexer"
)

// A CREATE INDEX key list element is more than a column name. PostgreSQL's
// grammar is
//
//	{ column | ( expression ) } [ opclass [ ( param = value [, ...] ) ] ]
//	                            [ ASC | DESC ] [ NULLS { FIRST | LAST } ]
//
// and Ptah's own PostgreSQL renderer writes every one of those suffixes. Until
// #1242 the parser kept each element as one opaque string and
// [go.5x5.cz/ptah/internal/convert/toschema] then classified anything that was
// not a bare identifier as an EXPRESSION, so a file Ptah had just written came
// back with the suffix glued to the key. Measured on live PostgreSQL 17.10, a
// database holding
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
// The decomposition below is deliberately all-or-nothing per statement. An
// element it does not fully understand leaves the whole key list on the legacy
// [go.5x5.cz/ptah/core/ast.IndexNode.Columns] path exactly as before, and a key
// list where nothing carries a suffix is left there too, so nothing that used
// to parse one way starts parsing another.

// indexKeyGrammar returns the key grammar this parser reads a key list under.
//
// The operator class suffix only exists in the PostgreSQL family. An unset
// dialect is the compatibility parser's best-effort mode, which is what
// [go.5x5.cz/ptah/internal/schemafile] falls back to when a caller names no
// target, and PostgreSQL DDL is what reaches it; MySQL's key grammar has no
// two-identifier element to confuse with one. A dialect that is set and is not
// PostgreSQL keeps such an element opaque.
func (p *Parser) indexKeyGrammar() indexKeyGrammar {
	return indexKeyGrammar{
		input:           p.input,
		operatorClasses: p.dialect == "" || platform.IsPostgresFamily(p.dialect),
	}
}

// indexKeyElement is one element of a CREATE INDEX key list: the text the
// legacy Columns field keeps, and the significant tokens that spell it.
type indexKeyElement struct {
	text   string
	tokens []lexer.Token
}

// indexKeyGrammar is the slice of the CREATE INDEX key grammar one document is
// read under: the document itself, and whether this dialect has operator
// classes at all.
type indexKeyGrammar struct {
	// input is the whole statement text, which expression spans are cut from.
	input string
	// operatorClasses enables the PostgreSQL-family operator class suffix.
	operatorClasses bool
}

// decomposeList converts the elements of a parsed key list into structured
// parts.
//
// It reports nil unless every element decomposes AND at least one of them
// carries a suffix that [go.5x5.cz/ptah/core/ast.IndexNode.Columns] cannot
// spell. Both halves matter: a partial conversion would silently reorder or
// re-quote keys nothing asked it to touch, and converting a plain key list
// would change how existing documents render for no gain.
func (g indexKeyGrammar) decomposeList(elements []indexKeyElement) []ast.IndexPart {
	parts := make([]ast.IndexPart, 0, len(elements))
	suffixed := false
	for _, element := range elements {
		part, hasSuffix, ok := g.decomposeElement(element.tokens)
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

// decomposeElement splits one key list element into its reference and its
// suffixes. It reports whether the element carried a suffix at all, and whether
// it was understood well enough to replace the raw text.
func (g indexKeyGrammar) decomposeElement(tokens []lexer.Token) (part ast.IndexPart, suffixed, ok bool) {
	if len(tokens) == 0 {
		return ast.IndexPart{}, false, false
	}
	// A per-key COLLATE is a suffix this model has no slot for. Leaving the
	// element opaque keeps it in the text where a reader can still see it,
	// which is strictly better than dropping it on the way through.
	for _, token := range tokens {
		if token.MatchIdentifierValue("COLLATE") {
			return ast.IndexPart{}, false, false
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
	if g.operatorClasses {
		if class, consumed := trailingOperatorClass(tokens[:end]); consumed > 0 {
			part.Operator = class
			end -= consumed
			suffixed = true
		}
	}

	head := tokens[:end]
	switch {
	case len(head) == 1 && isIndexKeyNameToken(head[0]):
		part.Name = head[0].Value
		return part, suffixed, true
	case isBalancedGroup(head):
		part.Expr = strings.TrimSpace(g.input[head[0].End:head[len(head)-1].Start])
		return part, suffixed, part.Expr != ""
	default:
		return ast.IndexPart{}, false, false
	}
}

// isIndexKeyNameToken reports whether a token can be a column name in a key
// list. The lexer hands a double-quoted identifier back as a string token --
// the DDL parser's own expectIdentifier accepts both spellings the same way --
// so a key list written by Ptah's PostgreSQL renderer, which quotes every
// column, is made of string tokens rather than identifier tokens.
func isIndexKeyNameToken(token lexer.Token) bool {
	return token.Type == lexer.TokenIdentifier || isDoubleQuotedIdentifierToken(token)
}

// trailingNullsOrder reads a NULLS FIRST or NULLS LAST suffix and reports how
// many tokens it spans.
func trailingNullsOrder(tokens []lexer.Token) (string, int) {
	if len(tokens) < 3 || !tokens[len(tokens)-2].MatchIdentifierValue("NULLS") {
		return "", 0
	}
	switch {
	case tokens[len(tokens)-1].MatchIdentifierValue("FIRST"):
		return ast.NullsOrderFirst, 2
	case tokens[len(tokens)-1].MatchIdentifierValue("LAST"):
		return ast.NullsOrderLast, 2
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
// The parameter list is re-spelled `name(a=1, b=2)` rather than copied out of
// the input, because that is exactly how the live-database reader spells the
// same class -- `pg_opclass.opcname` joined to `pg_attribute.attoptions` with
// ", " -- and the two spellings are compared to each other. Copying the input
// would make `tsvector_ops (siglen = 64)` differ from the catalog's
// `tsvector_ops(siglen=64)` and rebuild the index forever.
func trailingOperatorClass(tokens []lexer.Token) (string, int) {
	if len(tokens) < 2 {
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
