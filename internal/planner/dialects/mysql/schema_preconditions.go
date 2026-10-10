package mysql

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/internal/tableref"
	"ptah.run/migration/schemadiff/difftypes"
)

// planSchemaPreconditions creates the schemas the added objects are declared
// in, before any of them.
//
// It runs on SQL SERVER ONLY, and the dialect check is the decision rather than
// a guard. A schema is an ordinary object inside the connected database there,
// and `CREATE SCHEMA` is ordinary DDL. On MySQL, MariaDB and ClickHouse a
// schema IS a database, and on Oracle it is a USER: creating one is
// `CREATE DATABASE` or `CREATE USER`, an administrative act outside what a
// schema migration owns, and emitting it from here would have a migration
// create databases nobody asked for.
//
// Without it a multi-schema SQL Server declaration could not be applied at all.
// Measured on SQL Server 2022 (16.0.4265.3), a document declaring `app` and one
// table in it, against a database holding only `dbo`:
//
//	CREATE TABLE [app].[widget] ([id] INT PRIMARY KEY);
//	Msg 2760: The specified schema name "app" either does not exist or you do
//	not have permission to use it.
//
// It is stokaro/ptah#1276's defect on the dialect that fix did not reach, and
// the renderer was already waiting for it: VisitCreateSchema writes the guarded
// form SQL Server needs, because there is no CREATE SCHEMA IF NOT EXISTS here
// and CREATE SCHEMA must be the first statement of its batch.
//
//	IF SCHEMA_ID(N'app') IS NULL
//	    EXEC(N'CREATE SCHEMA [app]');
//
// The schemas come from the qualified names the comparison already put on the
// diff, so a single-schema migration -- where nothing carries a schema --
// contributes none and the statement is not emitted at all. An apply that adds
// nothing therefore stays the clean no-op it has to be (stokaro/ptah#1996).
//
// A schema a document DECLARES and then puts nothing in contributes none
// either, which is the PostgreSQL behavior this mirrors. Whether an empty
// declared schema should be created is a question about what a schema
// declaration means, and it is the same question on both dialects; answering it
// on one would make them disagree.
//
// The guards go in front of the finished plan, because the schemas an owner's
// objects are created in are known only once the owners have planned: created
// names them, from the steps that create a schema-level owned object.
func (p *Planner) planSchemaPreconditions(result []ast.Node, diff *difftypes.SchemaDiff, created []string) []ast.Node {
	if p.targetDialect() != platform.SQLServer {
		return result
	}
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	schemas := schemasAddedObjectsNeed(diff, created)
	guarded := make([]ast.Node, 0, len(schemas)+len(result))
	for _, schema := range schemas {
		guarded = append(guarded, schemaprecondition.Node(schema, diff.DeclaredSchemas, semantics))
	}
	return append(guarded, result...)
}

// schemasAddedObjectsNeed is every schema an added object is declared in, in a
// stable order.
//
// EVERY added-object list is read rather than the tables alone, which is the
// half of stokaro/ptah#1276 that took two attempts there: preconditions derived
// from the tables covered none of the sequences, functions or views planned in
// the same run, and each of those fails on the same Msg 2760.
//
// The owners' objects are read from the schemas their creating steps name,
// see [createdOwnedSchemas]. A synonym contributes its own schema and NOT its
// target's, since the target is an object this migration does not own, in a
// database it may not even be in. An extended property contributes the schema
// it addresses, the one place a schema is needed without any object being
// created in it: a property on a schema names `@level0name = N'app'` and
// answers the same Msg 2760 when `app` is absent.
func schemasAddedObjectsNeed(diff *difftypes.SchemaDiff, created []string) []string {
	qualified := make([]string, 0, len(diff.TablesAdded))
	qualified = append(qualified, diff.TablesAdded.Names()...)
	qualified = append(qualified, diff.ViewsAdded.Names()...)
	qualified = append(qualified, diff.MaterializedViewsAdded.Names()...)
	qualified = append(qualified, diff.FunctionsAdded.Names()...)
	qualified = append(qualified, diff.SequencesAdded.Names()...)
	for _, trigger := range diff.TriggersAdded {
		qualified = append(qualified, trigger.TableName)
	}

	seen := make(map[string]struct{}, len(qualified))
	schemas := make([]string, 0, len(qualified))
	record := func(schema string) {
		schema = strings.TrimSpace(schema)
		if schema == "" {
			return
		}
		if _, known := seen[schema]; known {
			return
		}
		seen[schema] = struct{}{}
		schemas = append(schemas, schema)
	}

	for _, name := range qualified {
		ref, ok := tableref.Parse(name)
		if !ok || !ref.Qualified {
			continue
		}
		record(ref.Schema)
	}
	for _, schema := range created {
		record(schema)
	}
	slices.Sort(schemas)
	return schemas
}

// createdOwnedSchemas is the schema of every schema-level object an owner's
// step creates, as the step's effect names it. An object a table owns is in its
// table's schema, which the table's own creation or existence already
// accounts for.
func createdOwnedSchemas(features featurehost.Result) []string {
	var schemas []string
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			for _, effect := range step.Effects {
				if effect.Action == plangraph.Create && effect.Subject.Parent.Empty() && effect.Subject.Schema.Source != "" {
					schemas = append(schemas, effect.Subject.Schema.Source)
				}
			}
		}
	}
	return schemas
}
