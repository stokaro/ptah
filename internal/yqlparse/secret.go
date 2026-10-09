package yqlparse

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/internal/lexer"
)

// secret accepts only the environment reference emitted by the renderer.
// A malformed declaration may contain a literal credential anywhere, including
// an option name or trailing token. Never expose the ordinary parser's token
// diagnostics for this statement.
func (p *parser) secret() *ast.ExtensionStatement {
	defer func() {
		if p.err != nil {
			p.err = fmt.Errorf("YQL schema at position %d: invalid CREATE SECRET declaration; use a database-relative path and WITH (value = $PTAH_SECRET_<name>); literal values and other options are not supported", p.peek().Start)
		}
	}()
	ref, err := ydbsecret.ParsePath(decodedName(p.path()))
	if err != nil {
		p.failf("%v", err)
	}
	schema, name := ref.Schema.Source, ref.Name.Source
	p.wantWord("WITH")
	p.want("(")
	p.wantWord("value")
	p.want("=")
	token := p.peek()
	variable, prefixed := strings.CutPrefix(token.Value, "$")
	if token.Type != lexer.TokenIdentifier || !prefixed {
		p.failf("a secret value must name its environment variable")
	}
	if err := ydbsecret.CheckValueEnv(variable); err != nil {
		p.failf("%v", err)
	}
	if !p.done() {
		p.pos++
	}
	p.want(")")
	if !p.done() && p.peek().Type != lexer.TokenSemicolon {
		p.failf("expected ';' after the declaration")
	}
	return &ast.ExtensionStatement{Payload: &ydbast.Secret{Operation: ydbast.SecretCreate, Schema: schema, Name: name, ValueEnv: variable}}
}
