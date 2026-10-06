// Package yqlparse reads declarative YQL into the shared schema AST. Statements
// and clauses without a model are refused before any description is returned.
package yqlparse

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

type parser struct {
	text   string
	tokens []lexer.Token
	pos    int
	err    error
}

// Parse reads a YQL schema document. It never executes the document or connects
// to a database. Unknown syntax fails the whole read rather than losing objects.
func Parse(text string) (*ast.StatementList, error) {
	p := newParser(text)
	result := &ast.StatementList{}
	for !p.done() {
		if p.accept(";") {
			continue
		}
		node := p.declaration()
		if list, ok := node.(*ast.StatementList); ok {
			result.Statements = append(result.Statements, list.Statements...)
		} else {
			result.Statements = append(result.Statements, node)
		}
		if !p.done() && !p.accept(";") {
			p.failf("expected ';' after the declaration")
		}
	}
	if p.err != nil {
		return nil, p.err
	}
	return result, nil
}

func (p *parser) declaration() ast.Node {
	if p.word("ALTER") {
		return p.alterDeclaration()
	}
	if p.word("COMMENT") {
		return p.comment()
	}
	p.wantWord("CREATE")
	switch {
	case p.word("STREAMING"):
		p.pos++
		return p.streamingQuery(false)
	case p.word("USER"):
		p.pos++
		return p.createUser()
	case p.word("GROUP"):
		p.pos++
		return p.createGroup()
	case p.word("TABLE"):
		p.pos++
		return p.table()
	case p.word("VIEW"):
		p.pos++
		return p.view()
	case p.word("TOPIC"):
		p.pos++
		return p.topic()
	case p.word("COORDINATION"):
		p.pos++
		return p.coordination()
	case p.word("SECRET"):
		p.pos++
		return p.secret()
	case p.word("EXTERNAL"):
		p.pos++
		return p.external(false)
	case p.word("OR"):
		p.pos++
		return p.replacementDeclaration()
	case p.word("RESOURCE"):
		p.pos++
		return p.resourcePool()
	default:
		p.failf("this CREATE object kind is not supported in a desired YQL schema")
		return nil
	}
}

func (p *parser) replacementDeclaration() ast.Node {
	p.wantWord("REPLACE")
	if p.word("STREAMING") {
		p.pos++
		return p.streamingQuery(true)
	}
	p.wantWord("EXTERNAL")
	return p.external(true)
}

func (p *parser) done() bool { return p.err != nil || p.pos >= len(p.tokens) }
func (p *parser) peek() lexer.Token {
	if p.pos >= len(p.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: len(p.text), End: len(p.text)}
	}
	return p.tokens[p.pos]
}
func (p *parser) word(value string) bool { token := p.peek(); return token.MatchIdentifierValue(value) }
func (p *parser) accept(value string) bool {
	if p.done() || p.peek().Value != value {
		return false
	}
	p.pos++
	return true
}
func (p *parser) want(value string) {
	if !p.accept(value) {
		p.failf("expected %q", value)
	}
}
func (p *parser) wantWord(value string) {
	if p.word(value) {
		p.pos++
		return
	}
	p.failf("expected %s", value)
}
func (p *parser) failf(format string, args ...any) {
	if p.err == nil {
		p.err = fmt.Errorf("YQL schema at position %d: %s (got %q)", p.peek().Start, fmt.Sprintf(format, args...), p.peek().Value)
	}
}
func (p *parser) identifier() string {
	token := p.peek()
	name, ok := lexer.YQLIdentifierValue(token.Value)
	if token.Type != lexer.TokenIdentifier || !ok || name == "" || strings.HasPrefix(name, "$") {
		p.failf("expected an identifier")
		return ""
	}
	p.pos++
	return token.Value
}
func (p *parser) names() []string {
	p.want("(")
	var names []string
	for !p.done() {
		names = append(names, p.identifier())
		if !p.accept(",") {
			break
		}
	}
	p.want(")")
	return names
}

// expression takes a balanced value without consuming the delimiter. The stop
// words apply only at the outer level, never inside a literal or constructor.
func (p *parser) expression(stopWords ...string) string {
	start, end, depth := p.peek().Start, p.peek().Start, 0
	for !p.done() {
		token := p.peek()
		if depth == 0 && (token.Value == "," || token.Value == ")" || token.Type == lexer.TokenSemicolon || p.anyWord(stopWords)) {
			break
		}
		if token.Value == "(" {
			depth++
		}
		if token.Value == ")" {
			depth--
		}
		if token.Type == lexer.TokenUnknown {
			p.failf("unrecognized token in a value")
			break
		}
		end = token.End
		p.pos++
	}
	if depth != 0 || end == start {
		p.failf("expected a complete value")
	}
	return strings.TrimSpace(p.text[start:end])
}
func (p *parser) anyWord(words []string) bool {
	return slices.ContainsFunc(words, p.word)
}
func scalar(value string) string {
	if decoded, ok := lexer.StringValue(value, dialectlexer.Options(platform.YDB)); ok {
		return decoded
	}
	return value
}
func decodedName(value string) string { name, _ := lexer.YQLIdentifierValue(value); return name }

func (p *parser) options() map[string]string {
	return p.optionsUsing(func(string) string { return p.expression() })
}

func (p *parser) optionsUsing(valueOf func(string) string) map[string]string {
	p.want("(")
	values := make(map[string]string)
	for !p.done() {
		name := strings.ToLower(decodedName(p.identifier()))
		if _, exists := values[name]; exists {
			p.failf("setting %q is declared twice", name)
		}
		p.want("=")
		values[name] = valueOf(name)
		if !p.accept(",") {
			break
		}
	}
	p.want(")")
	return values
}

func newParser(text string) *parser {
	p := &parser{text: text}
	lex := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.YDB))
	for token := lex.NextToken(); token.Type != lexer.TokenEOF; token = lex.NextToken() {
		if token.Type == lexer.TokenUnknown && strings.TrimSpace(token.Value) == "--!syntax_v1" {
			continue
		}
		if token.Type == lexer.TokenComment && strings.HasPrefix(token.Value, "/*") && !strings.HasSuffix(token.Value, "*/") {
			p.err = fmt.Errorf("YQL schema at position %d: unterminated comment", token.Start)
			break
		}
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			p.tokens = append(p.tokens, token)
		}
	}
	return p
}
