package yqlparse

import (
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
)

func (p *parser) alterSerialSequence() *ast.AlterSequenceNode {
	node := ast.NewAlterSequence(decodedName(p.identifier()))
	if !strings.HasPrefix(node.Name, "/") || node.Name != strings.TrimSpace(node.Name) {
		p.failf("a Serial sequence needs its absolute database path")
	}
	for !p.done() && p.peek().Type != lexer.TokenSemicolon {
		switch {
		case p.word("START"):
			if node.Start != nil {
				p.failf("duplicate sequence START")
			}
			p.pos++
			if p.word("WITH") {
				p.pos++
			}
			value := p.sequenceNumber()
			node.Start = &value
		case p.word("INCREMENT"):
			if node.Increment != nil {
				p.failf("duplicate sequence INCREMENT")
			}
			p.pos++
			if p.word("BY") {
				p.pos++
			}
			value := p.sequenceNumber()
			node.Increment = &value
		default:
			p.failf("desired Serial sequences accept START and INCREMENT; RESTART is an operator action")
		}
	}
	if node.Start == nil && node.Increment == nil {
		p.failf("ALTER SEQUENCE needs START or INCREMENT")
	}
	return node
}

func (p *parser) sequenceNumber() int64 {
	value, err := strconv.ParseInt(p.peek().Value, 10, 64)
	if err != nil || value < 1 {
		p.failf("expected a positive sequence integer")
	}
	if !p.done() {
		p.pos++
	}
	return value
}
