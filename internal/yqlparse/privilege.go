package yqlparse

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
	"ptah.run/internal/ydbacl"
)

func (p *parser) privilegeStatement() *ast.StatementList {
	verb := strings.ToUpper(p.peek().Value)
	p.pos++
	var grantOption bool
	if verb == "REVOKE" && p.word("GRANT") {
		p.pos++
		p.wantWord("OPTION")
		p.wantWord("FOR")
		grantOption = true
	}
	permissions := p.permissions()
	p.wantWord("ON")
	paths := p.grantNames()
	if verb == "GRANT" {
		p.wantWord("TO")
	} else {
		p.wantWord("FROM")
	}
	roles := p.grantNames()
	if verb == "GRANT" && p.word("WITH") {
		p.pos++
		p.wantWord("GRANT")
		p.wantWord("OPTION")
		grantOption = true
	}
	// YDB stores a separate permission, not PostgreSQL's per-entry flag. Its
	// REVOKE GRANT OPTION FOR removes both that entry and the named permission.
	if grantOption {
		permissions = append(permissions, ydbacl.GrantPermission)
	}
	result := &ast.StatementList{}
	for _, path := range paths {
		for _, role := range roles {
			if verb == "GRANT" {
				result.Statements = append(result.Statements, ast.NewGrantPrivilege(role, ydbacl.ObjectPath, path, permissions))
			} else {
				result.Statements = append(result.Statements, ast.NewRevokePrivilege(role, ydbacl.ObjectPath, path, permissions))
			}
		}
	}
	return result
}

func (p *parser) grantNames() []string {
	var names []string
	for {
		names = append(names, decodedName(p.identifier()))
		if !p.accept(",") {
			return names
		}
	}
}

func (p *parser) permissions() []string {
	if p.word("ALL") {
		p.pos++
		if p.word("PRIVILEGES") {
			p.pos++
		}
		return []string{"ydb.generic.full"}
	}
	var names []string
	for {
		names = append(names, p.permission())
		if !p.accept(",") {
			return names
		}
	}
}

func (p *parser) permission() string {
	if p.peek().Type == lexer.TokenString {
		value, literal := stringLiteral(p.peek().Value, "String")
		permission, supported := ydbacl.LiteralPermission(value)
		if !literal || !supported {
			p.failf("unknown YDB permission name")
		}
		p.pos++
		return permission
	}
	var words []string
	for !p.done() && !p.word("ON") && p.peek().Value != "," {
		token := p.peek()
		if !token.MatchIdentifierValue(token.Value) || strings.HasPrefix(token.Value, "`") {
			p.failf("expected a permission keyword or literal")
			break
		}
		words = append(words, token.Value)
		p.pos++
	}
	permission, ok := ydbacl.Permission(strings.Join(words, " "))
	if !ok {
		p.failf("unknown YDB permission keyword")
	}
	return permission
}
