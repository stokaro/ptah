package mysql

import (
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
func (p *Planner) planSchemaPreconditions(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if p.targetDialect() != platform.SQLServer {
		return result
	}
	for _, schema := range schemasAddedObjectsNeed(diff) {
		result = append(result, &ast.CreateSchemaNode{Name: schema, IfNotExists: true})
	}
	return result
}

// schemasAddedObjectsNeed is every schema an added object is declared in, in a
// stable order.
//
// EVERY added-object list is read rather than the tables alone, which is the
// half of stokaro/ptah#1276 that took two attempts there: preconditions derived
// from the tables covered none of the sequences, functions or views planned in
// the same run, and each of those fails on the same Msg 2760.
//
// A synonym contributes its own schema and NOT its target's: the target is an
// object this migration does not own, in a database it may not even be in.
//
// An extended property contributes the schema it addresses, which is the one
// place a schema is needed without any object being created in it -- a
// property on a schema names `@level0name = N'app'` and answers the same Msg
// 2760 when `app` is absent.
func schemasAddedObjectsNeed(diff *difftypes.SchemaDiff) []string {
	qualified := make([]string, 0, len(diff.TablesAdded))
	qualified = append(qualified, diff.TablesAdded.Names()...)
	qualified = append(qualified, diff.ViewsAdded.Names()...)
	qualified = append(qualified, diff.MaterializedViewsAdded.Names()...)
	qualified = append(qualified, diff.FunctionsAdded.Names()...)
	qualified = append(qualified, diff.SequencesAdded.Names()...)
	qualified = append(qualified, diff.SynonymsAdded.Names()...)
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
	for _, property := range diff.ExtendedPropertiesAdded {
		record(property.Schema)
	}
	slices.Sort(schemas)
	return schemas
}
