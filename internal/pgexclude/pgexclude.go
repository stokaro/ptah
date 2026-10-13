// Package pgexclude reads represented EXCLUDE catalog definitions for the
// PostgreSQL reader and normalization probe. It is a leaf so orchestration
// can use the same grammar without linking provider models or registration.
package pgexclude

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Definition is the part of an EXCLUDE definition that the table
// owner represents. Elements and WhereCondition retain SQL source text without
// their enclosing parentheses. Deferral is recorded independently of the
// predicate. The zero value describes no valid constraint.
type Definition struct {
	UsingMethod    string
	Elements       string
	WhereCondition string
	Deferrable     bool
	Initially      string
}

// Parse reads one pg_get_constraintdef EXCLUDE definition. Quoted
// strings, identifiers, and comments do not delimit expression groups. It
// accepts the represented element, predicate, and deferral clauses; an
// unrepresented clause or malformed group returns nil and an error wrapping
// schemaext.ErrInvalidValue. It neither evaluates expressions nor changes text
// inside them.
func Parse(text string) (*Definition, error) {
	tokens, err := exclusionTokens(text)
	if err != nil {
		return nil, err
	}
	p := exclusionParser{text: text, tokens: tokens}
	if !p.word("EXCLUDE") || !p.word("USING") {
		return nil, exclusionSyntax("expected EXCLUDE USING")
	}
	if len(p.tokens) == 0 || !exclusionIdentifier(p.tokens[0]) {
		return nil, exclusionSyntax("expected an index method after USING")
	}
	result := &Definition{UsingMethod: p.tokens[0].Value}
	p.tokens = p.tokens[1:]
	result.Elements, err = p.group("elements")
	if err != nil {
		return nil, err
	}
	if p.word("WHERE") {
		result.WhereCondition, err = p.group("WHERE predicate")
		if err != nil {
			return nil, err
		}
	}
	switch {
	case p.word("DEFERRABLE"):
		result.Deferrable = true
	case p.word("NOT"):
		if !p.word("DEFERRABLE") {
			return nil, exclusionSyntax("expected DEFERRABLE after NOT")
		}
	}
	if p.word("INITIALLY") {
		switch {
		case p.word("DEFERRED"):
			if !result.Deferrable {
				return nil, exclusionSyntax("INITIALLY DEFERRED requires DEFERRABLE")
			}
			result.Initially = "deferred"
		case p.word("IMMEDIATE"):
			result.Initially = "immediate"
		default:
			return nil, exclusionSyntax("expected DEFERRED or IMMEDIATE after INITIALLY")
		}
	}
	if len(p.tokens) != 0 {
		return nil, exclusionSyntax("unrepresented or unexpected clause " + p.tokens[0].Value)
	}
	return result, nil
}

func exclusionSyntax(reason string) error {
	return fmt.Errorf("%w: PostgreSQL EXCLUDE: %s", schemaext.ErrInvalidValue, reason)
}

func exclusionTokens(text string) ([]lexer.Token, error) {
	scan := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.Postgres))
	var tokens []lexer.Token
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		case lexer.TokenUnknown, lexer.TokenSemicolon:
			return nil, exclusionSyntax("unexpected token " + token.Value)
		}
		tokens = append(tokens, token)
	}
	return tokens, nil
}

func exclusionIdentifier(token lexer.Token) bool {
	return token.Type == lexer.TokenIdentifier ||
		len(token.Value) > 2 && strings.HasPrefix(token.Value, `"`) && strings.HasSuffix(token.Value, `"`)
}

type exclusionParser struct {
	text   string
	tokens []lexer.Token
}

func (p *exclusionParser) word(word string) bool {
	if len(p.tokens) == 0 || !p.tokens[0].MatchIdentifierValue(word) {
		return false
	}
	p.tokens = p.tokens[1:]
	return true
}

func (p *exclusionParser) group(name string) (string, error) {
	if len(p.tokens) == 0 || !p.tokens[0].MatchOperatorValue("(") {
		return "", exclusionSyntax("expected '(' before " + name)
	}
	start, depth := p.tokens[0].End, 0
	for i, token := range p.tokens {
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				if i == 1 {
					return "", exclusionSyntax("empty " + name)
				}
				value := strings.TrimSpace(p.text[start:token.Start])
				p.tokens = p.tokens[i+1:]
				return value, nil
			}
		}
	}
	return "", exclusionSyntax("unclosed " + name)
}
