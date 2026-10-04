// Package schemaprecondition builds the CREATE SCHEMA node a migration plan
// emits before the objects that are declared inside that schema, and refuses a
// database change a planner does not plan.
//
// It exists so that the two planners which emit such a node -- the PostgreSQL
// family's and SQL Server's -- construct it the same way. Both derive the
// schema NAME from the qualified names of the objects they are creating, and
// stopping there leaves everything else the declaration says about the schema
// with nowhere to go: the plan creates a schema and drops the comment the
// author wrote for it, on every run, with the next comparison seeing a schema
// whose comment the declaration has and the database does not
// (stokaro/ptah#2618).
package schemaprecondition

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
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

// RefuseServerSchemas refuses a diff that creates, drops or changes a
// database, for a planner of dialect that plans no databases. The comparison
// records those changes only for a whole MySQL or MariaDB server
// (stokaro/ptah#3789), so another planner reaches one only through a diff
// built by hand, and planning nothing would report the two sides equal.
func RefuseServerSchemas(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil || len(diff.SchemasAdded)+len(diff.SchemasRemoved)+len(diff.SchemasModified) == 0 {
		return nil
	}
	return fmt.Errorf("%w: the diff creates, drops or changes a database, which only a MySQL or MariaDB plan of a whole server does; the %s planner plans none",
		ptaherr.ErrUnsupportedFeature, dialect)
}

// RefuseIndexChangesInPlace refuses a diff that renames an index or changes an
// index's partitioning in place, for a planner of dialect that plans neither.
// The comparison records those changes only on a target with
// capability.IndexRename or capability.IndexPartitioning, which only YDB has,
// so another planner reaches one only through a diff built by hand, and
// planning nothing would leave the index as it was and report the two sides
// equal.
func RefuseIndexChangesInPlace(dialect string, diff *difftypes.SchemaDiff) error {
	switch {
	case diff == nil:
		return nil
	case len(diff.IndexesRenamed) > 0:
		rename := diff.IndexesRenamed[0]
		return fmt.Errorf("%w: the diff renames index %q of table %q to %q, and the %s planner plans no index rename",
			ptaherr.ErrUnsupportedFeature, rename.From, rename.TableName, rename.To, dialect)
	case len(diff.IndexPartitioningChanged) > 0:
		change := diff.IndexPartitioningChanged[0]
		return fmt.Errorf("%w: the diff changes the partitioning of index %q of table %q, which only a YDB plan does; "+
			"the %s planner plans none", ptaherr.ErrUnsupportedFeature, change.Name, change.TableName, dialect)
	default:
		return nil
	}
}
