package mysql

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// reportUnsupportedObjects appends the AST nodes for the declared object kinds
// this planner's targets cannot host, so each target's renderer turns them into
// the same named not-supported comment `ptah schema render` already produces.
//
// Without this the two surfaces disagreed: `schema render --dialect mysql` on a
// schema declaring an extension and a standalone sequence emitted
// `-- Extension pg_trgm not supported in MySQL` and
// `-- CREATE SEQUENCE order_number_seq not supported in mysql`, while
// `schema apply --dry-run` against live MySQL 9.7 and live MariaDB 10.11.18
// planned the CREATE TABLE alone and said nothing about either object
// (stokaro/ptah#931 items 5 and 8). Render already moved; the plan path is the
// half that had not.
//
// The nodes carry identity only. Neither renderer emits DDL for these kinds, so
// identity is all they read, and the resulting comments are stripped before
// execution by atlasschema.SplitApplyStatements.
//
// Sequences are routed to the MySQL family only, matching the converter that
// feeds `render`. The SQL Server renderer answers a sequence node with a flat
// "CREATE SEQUENCE is not supported", which is false of an engine that has had
// sequences since 2012, so routing it there would trade a silent omission for a
// wrong statement.
func (p *Planner) reportUnsupportedObjects(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.ExtensionsAdded {
		result = append(result, ast.NewExtension(name))
	}
	for _, name := range diff.ExtensionsRemoved {
		result = append(result, ast.NewDropExtension(name))
	}
	if p.targetDialect() == platform.SQLServer {
		return result
	}
	for _, name := range diff.SequencesAdded {
		result = append(result, ast.NewCreateSequence(name))
	}
	for _, sequence := range diff.SequencesModified {
		result = append(result, ast.NewAlterSequence(sequence.SequenceName))
	}
	for _, name := range diff.SequencesRemoved {
		result = append(result, ast.NewDropSequence(name))
	}
	return result
}
