// Package pgexclude reads represented EXCLUDE definitions for PostgreSQL
// source documents, catalog reads, and normalization probes. It is a leaf so orchestration
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

// Parse reads one EXCLUDE declaration or pg_get_constraintdef definition. Quoted
// strings, identifiers, and comments do not delimit expression groups. It
// accepts the represented element, predicate, and deferral clauses; an
// unrepresented clause or malformed group returns nil and an error wrapping
// schemaext.ErrInvalidValue. It neither evaluates expressions nor changes text
// inside them. An omitted index method selects btree. Deferral clauses may
// appear in either order; INITIALLY DEFERRED implies DEFERRABLE.
func Parse(text string) (*Definition, error) {
	tokens, err := exclusionTokens(text)
	if err != nil {
		return nil, err
	}
	p := exclusionParser{text: text, tokens: tokens}
	if !p.word("EXCLUDE") {
		return nil, exclusionSyntax("expected EXCLUDE")
	}
	result := &Definition{UsingMethod: "btree"}
	if p.word("USING") {
		if len(p.tokens) == 0 || !exclusionIdentifier(p.tokens[0]) {
			return nil, exclusionSyntax("expected an index method after USING")
		}
		result.UsingMethod = p.tokens[0].Value
		p.tokens = p.tokens[1:]
	}
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
	if err := p.deferral(result); err != nil {
		return nil, err
	}
	return result, nil
}

// deferral accepts repeated compatible clauses and refuses contradictory ones.
func (p *exclusionParser) deferral(result *Definition) error {
	notDeferrable := false
	for len(p.tokens) > 0 {
		switch {
		case p.word("DEFERRABLE"):
			result.Deferrable = true
		case p.word("NOT"):
			if !p.word("DEFERRABLE") {
				return exclusionSyntax("expected DEFERRABLE after NOT")
			}
			notDeferrable = true
		case p.word("INITIALLY"):
			timing, err := p.initialTiming()
			if err != nil {
				return err
			}
			if result.Initially != "" && result.Initially != timing {
				return exclusionSyntax("conflicting initial timing")
			}
			result.Initially = timing
		default:
			return exclusionSyntax("unrepresented or unexpected clause " + p.tokens[0].Value)
		}
	}
	if result.Deferrable && notDeferrable {
		return exclusionSyntax("conflicting deferral clauses")
	}
	if result.Initially == "deferred" {
		if notDeferrable {
			return exclusionSyntax("INITIALLY DEFERRED requires DEFERRABLE")
		}
		result.Deferrable = true
	}
	return nil
}

func (p *exclusionParser) initialTiming() (string, error) {
	switch {
	case p.word("DEFERRED"):
		return "deferred", nil
	case p.word("IMMEDIATE"):
		return "immediate", nil
	default:
		return "", exclusionSyntax("expected DEFERRED or IMMEDIATE after INITIALLY")
	}
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
