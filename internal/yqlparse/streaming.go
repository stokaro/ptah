package yqlparse

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/internal/lexer"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbstream"
)

func (p *parser) streamingQuery(replace bool) *ast.ExtensionStatement {
	p.wantWord("QUERY")
	options := ydbast.StreamingCreation{OrReplace: replace}
	if p.word("IF") {
		p.pos++
		p.wantWord("NOT")
		p.wantWord("EXISTS")
		options.IfNotExists = true
	}
	ref, ok := tableref.Parse(p.canonicalPath())
	if !ok {
		p.failf("invalid streaming query path")
	}
	node := &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Schema: ref.Schema, Name: ref.Name, Creation: options}
	if p.word("WITH") {
		p.pos++
		p.optionsUsing(func(key string) string {
			value := p.expression()
			switch key {
			case "run":
				switch {
				case strings.EqualFold(value, "TRUE"):
					node.Spec.Run = new(true)
				case strings.EqualFold(value, "FALSE"):
					node.Spec.Run = new(false)
				default:
					p.failf("streaming query run requires TRUE or FALSE")
				}
			case "resource_pool":
				pool, ok := stringLiteral(value, "String")
				literal := newParser(value)
				if len(literal.tokens) == 1 && literal.tokens[0].Type == lexer.TokenIdentifier {
					pool, ok = lexer.YQLIdentifierValue(value)
				}
				if !ok || pool == "" || strings.HasPrefix(pool, "$") {
					p.failf("streaming query resource_pool requires a literal name")
				}
				node.Spec.ResourcePool = pool
			default:
				p.failf("unsupported streaming query setting %q", key)
			}
			return value
		})
	}
	p.wantWord("AS")
	node.Spec.Text = p.streamingBody()
	if err := ydbstream.Validate(node.Spec); err != nil {
		p.failf("%v", err)
	}
	return &ast.ExtensionStatement{Payload: node}
}

func (p *parser) streamingBody() string {
	block := newParser(p.queryBody())
	tokens := block.tokens
	if block.err != nil || len(tokens) < 4 ||
		!tokens[0].MatchIdentifierValue("DO") || !tokens[1].MatchIdentifierValue("BEGIN") ||
		!tokens[len(tokens)-2].MatchIdentifierValue("END") || !tokens[len(tokens)-1].MatchIdentifierValue("DO") {
		p.failf("a streaming query requires AS DO BEGIN ... END DO")
		return ""
	}
	return strings.TrimSpace(block.text[tokens[1].End:tokens[len(tokens)-2].Start])
}

func (p *parser) canonicalPath() string {
	path := decodedName(p.path())
	directory, name := "", path
	if slash := strings.LastIndexByte(path, '/'); slash >= 0 {
		directory, name = path[:slash], path[slash+1:]
	}
	return tableref.Canonical(directory, name)
}
