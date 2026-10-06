package yqlparse

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
	"ptah.run/internal/ydbreplication"
)

// A connection clause may contain an accidentally embedded credential. Hide
// every token on these error paths, including errors at the statement boundary.
func (p *parser) replicationError() {
	if !p.done() && p.peek().Type != lexer.TokenSemicolon {
		p.failf("expected ';'")
	}
	if p.err != nil {
		p.err = fmt.Errorf("YQL schema at position %d: invalid replication or transfer declaration; use modeled settings and secret references, with an inline lambda for a transfer", p.peek().Start)
	}
}

func (p *parser) createReplication() *ast.CreateAsyncReplicationNode {
	defer p.replicationError()
	p.wantWord("REPLICATION")
	name := p.path()
	p.wantWord("FOR")
	var items []ast.AsyncReplicationItem
	for !p.done() {
		source := p.replicationPath(p.identifier())
		p.wantWord("AS")
		target := p.replicationPath(p.path())
		item, err := ydbreplication.ParseItem(map[string]string{"source": source, "target": target})
		if err != nil {
			p.failf("invalid replication item")
		}
		items = append(items, item)
		if !p.accept(",") {
			break
		}
	}
	p.wantWord("WITH")
	spec, err := ydbreplication.ParseReplicationSource(p.replicationSettings(ydbreplication.ReplicationSource))
	if err != nil {
		p.failf("invalid replication settings")
	}
	spec.Items = items
	if err := ydbreplication.ValidateReplicationItems(items); err != nil {
		p.failf("invalid replication items")
	}
	return ast.NewCreateAsyncReplication(name, spec)
}

func (p *parser) createTransfer() *ast.CreateTransferNode {
	defer p.replicationError()
	name := p.path()
	p.wantWord("FROM")
	source := p.replicationPath(p.identifier())
	p.wantWord("TO")
	target := p.replicationPath(p.path())
	p.wantWord("USING")
	lambda := p.transferLambda()
	settings := make(map[string]string)
	if p.word("WITH") {
		p.pos++
		settings = p.replicationSettings(ydbreplication.TransferSource)
	}
	settings["source"], settings["target"], settings["using"] = source, target, lambda
	spec, err := ydbreplication.ParseTransferSource(settings)
	if err != nil {
		p.failf("invalid transfer settings")
	}
	return ast.NewCreateTransfer(name, spec)
}

func (p *parser) alterReplication() *ast.AlterAsyncReplicationNode {
	defer p.replicationError()
	p.wantWord("REPLICATION")
	name := p.path()
	p.wantWord("SET")
	return &ast.AlterAsyncReplicationNode{Name: name, SourceSettings: p.replicationSettings(ydbreplication.ReplicationSource)}
}

func (p *parser) alterTransfer() *ast.AlterTransferNode {
	defer p.replicationError()
	node := &ast.AlterTransferNode{Name: p.path(), SourceSettings: make(map[string]string)}
	for !p.done() {
		p.wantWord("SET")
		settings := make(map[string]string)
		if p.word("USING") {
			p.pos++
			settings["using"] = p.transferLambda()
		} else {
			settings = p.replicationSettings(ydbreplication.TransferSource)
		}
		for name, value := range settings {
			if _, exists := node.SourceSettings[name]; exists {
				p.failf("duplicate transfer setting")
			}
			node.SourceSettings[name] = value
		}
		if !p.accept(",") {
			break
		}
	}
	return node
}

func (p *parser) replicationSettings(kind ydbreplication.SourceKind) map[string]string {
	return p.optionsUsing(func(key string) string {
		raw := p.expression()
		switch ydbreplication.SourceSettingType(key, kind) {
		case "String":
			value, ok := stringLiteral(raw, "String")
			if !ok || value == "" || value != strings.TrimSpace(value) {
				p.failf("expected a nonempty string literal")
			}
			return value
		case "Uint":
			value, err := strconv.ParseUint(raw, 10, 64)
			if err != nil || value == 0 {
				p.failf("expected a positive integer")
			}
			return raw
		case "Interval":
			literal := newParser(raw)
			literal.wantWord("Interval")
			literal.want("(")
			value, ok := stringLiteral(literal.expression(), "String")
			literal.want(")")
			if !ok || value != strings.TrimSpace(value) || literal.err != nil || !literal.done() {
				p.failf("expected an interval literal")
			}
			return value
		default:
			p.failf("unsupported replication or transfer setting")
			return ""
		}
	})
}

// The lambda is kept byte-for-byte, including nested blocks and semicolons.
// Only its grammar boundary is parsed; the server validates query expressions.
func (p *parser) transferLambda() string {
	start := p.peek().Start
	p.want("(")
	for !p.done() && p.peek().Value != ")" {
		token := p.peek()
		if token.Type != lexer.TokenIdentifier || !strings.HasPrefix(token.Value, "$") {
			p.failf("expected a lambda parameter")
		}
		p.pos++
		if !p.accept(",") {
			break
		}
	}
	p.want(")")
	if !p.accept("->") {
		p.want("-")
		p.want(">")
	}
	p.want("{")
	closers := []string{"}"}
	end := p.peek().Start
	for !p.done() && len(closers) > 0 {
		token := p.peek()
		if token.Type == lexer.TokenUnknown {
			p.failf("unrecognized lambda token")
			break
		}
		switch token.Value {
		case "(", "[", "{":
			closers = append(closers, map[string]string{"(": ")", "[": "]", "{": "}"}[token.Value])
		case ")", "]", "}":
			if token.Value != closers[len(closers)-1] {
				p.failf("unbalanced lambda")
			} else {
				closers = closers[:len(closers)-1]
			}
		}
		end = token.End
		p.pos++
	}
	if len(closers) != 0 {
		p.failf("incomplete lambda")
	}
	return strings.TrimSpace(p.text[start:end])
}

// The shared declaration parser trims annotation values. Quoted YQL paths
// cannot inherit that normalization, because whitespace changes their names.
func (p *parser) replicationPath(name string) string {
	value := decodedName(name)
	if value != strings.TrimSpace(value) {
		p.failf("path has surrounding whitespace")
	}
	return value
}
