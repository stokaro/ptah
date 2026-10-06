package yqlparse

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
	"ptah.run/internal/ydbacl"
)

// userError hides every token in a user declaration error, including a password
// accidentally placed where a name, option or delimiter was expected.
func (p *parser) userError() {
	if !p.done() && p.peek().Type != lexer.TokenSemicolon {
		p.failf("expected ';' after the declaration")
	}
	if p.err != nil {
		p.err = fmt.Errorf("YQL schema at position %d: invalid user declaration; expected a user name and PASSWORD, HASH, LOGIN or NOLOGIN options", p.peek().Start)
	}
}

func (p *parser) createUser() *ast.CreateRoleNode {
	defer p.userError()
	node := &ast.CreateRoleNode{Name: decodedName(p.identifier()), Login: true, Inherit: true}
	p.checkPrincipalName(node.Name)
	for _, operation := range p.userOptions() {
		switch typed := operation.(type) {
		case *ast.SetPasswordOperation:
			node.Password = typed.Password
		case *ast.SetLoginOperation:
			node.Login = typed.Login
		}
	}
	return node
}

func (p *parser) alterUser() *ast.AlterRoleNode {
	defer p.userError()
	node := ast.NewAlterRole(decodedName(p.identifier()))
	p.checkPrincipalName(node.Name)
	if p.word("WITH") {
		p.pos++
	}
	node.Operations = p.userOptions()
	if len(node.Operations) == 0 {
		p.failf("ALTER USER needs an option")
	}
	return node
}

func (p *parser) checkPrincipalName(name string) {
	if err := ydbacl.CheckName(name); err != nil {
		p.failf("%s", err)
	}
}

func (p *parser) userOptions() []ast.RoleOperation {
	var operations []ast.RoleOperation
	var passwordSeen, loginSeen bool
	for !p.done() && p.peek().Type != lexer.TokenSemicolon {
		switch {
		case p.word("PASSWORD"), p.word("HASH"):
			clause := "PASSWORD"
			if p.word("HASH") {
				clause = "HASH"
			}
			if passwordSeen {
				p.failf("duplicate password option")
			}
			passwordSeen = true
			p.pos++
			operations = append(operations, ast.NewSetPasswordOperation(p.userPassword(clause)))
		case p.word("LOGIN"), p.word("NOLOGIN"):
			if loginSeen {
				p.failf("duplicate login option")
			}
			loginSeen = true
			operations = append(operations, ast.NewSetLoginOperation(p.word("LOGIN")))
			p.pos++
		default:
			p.failf("unsupported user option")
		}
	}
	return operations
}

func (p *parser) userPassword(clause string) string {
	if clause == "PASSWORD" && p.word("NULL") {
		p.pos++
		return ""
	}
	value, ok := stringLiteral(p.peek().Value, "String")
	if !ok || (clause == "HASH") != ydbacl.IsPasswordHash(value) {
		p.failf("expected a password literal or a YDB password hash")
	}
	p.pos++
	return value
}

func (p *parser) createGroup() *ast.StatementList {
	name := decodedName(p.identifier())
	p.checkPrincipalName(name)
	result := &ast.StatementList{Statements: []ast.Node{&ast.CreateRoleNode{Name: name, Group: true, Inherit: true}}}
	if p.word("WITH") {
		p.pos++
		p.wantWord("USER")
		result.Statements = append(result.Statements, p.groupMembers(name, "ADD").Statements...)
	}
	return result
}

func (p *parser) alterGroup() *ast.StatementList {
	name := decodedName(p.identifier())
	clause := "ADD"
	if p.word("DROP") {
		clause = "DROP"
	}
	if !p.word("ADD") && !p.word("DROP") {
		p.failf("expected ADD USER or DROP USER")
	}
	p.pos++
	p.wantWord("USER")
	return p.groupMembers(name, clause)
}

func (p *parser) groupMembers(group, clause string) *ast.StatementList {
	result := &ast.StatementList{}
	for !p.done() {
		member := decodedName(p.identifier())
		if clause == "ADD" {
			result.Statements = append(result.Statements, ast.NewGrantRoleMembership(group, member))
		} else {
			result.Statements = append(result.Statements, ast.NewRevokeRoleMembership(group, member))
		}
		if !p.accept(",") {
			break
		}
		if p.done() {
			p.failf("expected a group member after comma")
		}
	}
	if len(result.Statements) == 0 {
		p.failf("expected a group member")
	}
	return result
}
