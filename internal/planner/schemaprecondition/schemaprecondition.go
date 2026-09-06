// Package schemaprecondition builds the CREATE SCHEMA node a migration plan
// emits before the objects that are declared inside that schema.
//
// It exists so that the two planners which emit such a node -- the PostgreSQL
// family's and SQL Server's -- construct it the same way. Both derive the
// schema NAME from the qualified names of the objects they are creating, and
// both used to stop there, so everything else the declaration said about the
// schema had nowhere to go: a plan created a schema and the comment the author
// wrote for it was dropped, on every run, with the next comparison seeing a
// schema whose comment the declaration has and the database does not
// (stokaro/ptah#2618).
package schemaprecondition

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// Node returns the creation for one schema, carrying what the declaration says
// about it.
//
// name is the schema an added object needs, which is why the node is emitted at
// all; declared is every schema the desired document declares, normally
// [ptah.run/migration/schemadiff/difftypes.SchemaDiff.DeclaredSchemas].
// A name no declaration matches yields the bare guarded creation, which is the
// established behavior for a schema reached only through an object's qualifier.
//
// The renderers decide what survives: PostgreSQL writes COMMENT ON SCHEMA, SQL
// Server writes the comment as a leading `--` line, and the MySQL-family
// renderer writes DEFAULT CHARACTER SET and COLLATE. Attaching all three here
// keeps that decision in one place per dialect rather than in the planner.
//
// A name two declarations fold onto yields the bare creation as well. Two
// schemas that compare equal name no one declaration, and attaching one of
// their comments would be a coin toss written into the user's database; the
// schema still gets created, so nothing is lost that was not already ambiguous.
func Node(name string, declared []schemamodel.Schema, semantics identifier.Semantics) *ast.CreateSchemaNode {
	node := &ast.CreateSchemaNode{Name: name, IfNotExists: true}
	match := find(name, declared, semantics)
	if match == nil {
		return node
	}
	node.Comment = match.Comment
	node.Charset = match.Charset
	node.Collate = match.Collate
	return node
}

// find returns the single declaration naming the same schema as name, or nil.
//
// An exact match wins before any folding, so a document that spells the name
// the way the object's qualifier does is never re-interpreted. Otherwise the
// two are compared under the dialect's rule for a schema name, which is what
// joins `App` and `app` on SQL Server and keeps them apart on PostgreSQL.
func find(name string, declared []schemamodel.Schema, semantics identifier.Semantics) *schemamodel.Schema {
	for i := range declared {
		if declared[i].Name == name {
			return &declared[i]
		}
	}
	key := semantics.TableIdentityKey(name)
	var found *schemamodel.Schema
	for i := range declared {
		if semantics.TableIdentityKey(declared[i].Name) != key {
			continue
		}
		if found != nil {
			return nil
		}
		found = &declared[i]
	}
	return found
}
