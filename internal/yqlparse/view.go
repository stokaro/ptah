package yqlparse

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlcompound"
)

func (p *parser) path() string {
	name := p.identifier()
	if strings.HasPrefix(decodedName(name), "/") {
		p.failf("desired YQL schema paths must be database-relative")
	}
	return name
}

func (p *parser) view() *ast.CreateViewNode {
	node := ast.NewCreateView(p.path())
	p.wantWord("WITH")
	p.want("(")
	// Unlike ordinary option names, YDB requires this exact case.
	if decodedName(p.identifier()) != "security_invoker" {
		p.failf("a YDB view requires security_invoker = TRUE")
	}
	p.want("=")
	p.wantWord("TRUE")
	p.want(")")
	p.wantWord("AS")
	if !p.word("SELECT") && !p.word("DO") {
		p.failf("a YDB view needs a SELECT or DO query")
	}
	node.Body = p.queryBody()
	return node
}

// queryBody retains the query text. Semicolons inside lambdas and inline
// actions belong to the view, as they do to the shared YQL statement splitter.
func (p *parser) queryBody() string {
	start, end := p.peek().Start, p.peek().Start
	state := sqlcompound.New(platform.YDB)
	var closers []string
	for !p.done() {
		token := p.peek()
		if token.Type == lexer.TokenSemicolon && !state.KeepSemicolonInsideStatement() {
			break
		}
		if token.Type == lexer.TokenUnknown {
			p.failf("unrecognized token in view query")
			break
		}
		if token.Type == lexer.TokenIdentifier {
			state.Word(token.Value)
		} else {
			state.Symbol(token.Value)
		}
		switch token.Value {
		case "(", "[", "{":
			closers = append(closers, map[string]string{"(": ")", "[": "]", "{": "}"}[token.Value])
		case ")", "]", "}":
			if len(closers) == 0 || closers[len(closers)-1] != token.Value {
				p.failf("unbalanced view query")
			} else {
				closers = closers[:len(closers)-1]
			}
		}
		end = token.End
		p.pos++
	}
	if start == end || len(closers) != 0 || state.KeepSemicolonInsideStatement() {
		p.failf("expected a complete view query")
	}
	return strings.TrimSpace(p.text[start:end])
}
