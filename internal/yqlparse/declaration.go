package yqlparse

import (
	"ptah.run/core/ast"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbtopic"
)

func (p *parser) declaration() ast.Node {
	if p.word("GRANT") || p.word("REVOKE") {
		return p.privilegeStatement()
	}
	if p.word("ALTER") {
		return p.alterDeclaration()
	}
	if p.word("COMMENT") {
		return p.comment()
	}
	p.wantWord("CREATE")
	return p.createDeclaration()
}

func (p *parser) createDeclaration() ast.Node {
	switch {
	case p.word("ASYNC"):
		p.pos++
		return p.createReplication()
	case p.word("TRANSFER"):
		p.pos++
		return p.createTransfer()
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

func (p *parser) alterDeclaration() ast.Node {
	p.wantWord("ALTER")
	switch {
	case p.word("SEQUENCE"):
		p.pos++
		return p.alterSerialSequence()
	case p.word("ASYNC"):
		p.pos++
		return p.alterReplication()
	case p.word("TRANSFER"):
		p.pos++
		return p.alterTransfer()
	case p.word("USER"):
		p.pos++
		return p.alterUser()
	case p.word("GROUP"):
		p.pos++
		return p.alterGroup()
	case p.word("TABLE"):
		p.pos++
		return p.changefeed()
	case p.word("TOPIC"):
		p.pos++
		ref, err := ydbtopic.ParsePath(decodedName(p.path()))
		if err != nil {
			p.failf("%v", err)
		}
		p.wantWord("ADD")
		p.wantWord("CONSUMER")
		return &ast.ExtensionStatement{Payload: &ydbast.TopicConsumer{Schema: ref.Schema.Source, Name: ref.Name.Source, Consumer: p.consumer()}}
	default:
		return p.defaultPool()
	}
}
