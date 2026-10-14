// Package ydbsql is the YDB owner's side of the SQL frontend. YQL is not
// written in the shared grammar, so the owner reads every YQL document of a
// load itself, in one call: it parses each document with internal/yqlparse,
// converts the YDB statements into the YDB owner's models, and leaves tables,
// columns, indexes and views to the shared conversion of internal/sqlschema.
// It claims what a YQL document declares, less the objects its
// `ptah:not-described` header leaves unmanaged. The built-in runtime registers
// [SQL] on the YDB target.
//
// The shared conversion is reached in process through
// [sqlschema.ConvertScript], which a plugin could not reach. That is plugin
// debt under stokaro/ptah#4204: an extracted YDB provider needs a public
// conversion library, or its own conversion of YQL tables.
package ydbsql

import (
	"context"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/ydbsource"
	"ptah.run/internal/yqlparse"
)

// SQL is the YDB owner's SQL extension: the grammar that reads YQL, the
// header kinds that leave a YDB object unmanaged, and the claim a YQL
// document makes.
func SQL() sqlext.Extension {
	return sqlext.Extension{
		Owner:    ydbschema.Owner,
		Targets:  []string{platform.YDB},
		Grammar:  grammar{},
		Header:   ydbsource.LimitKinds(),
		Coverage: claim,
	}
}

// grammar reads a load of YQL documents.
type grammar struct{}

// ReadSQL reads the documents of one load as one script.
func (grammar) ReadSQL(ctx context.Context, request sqlext.GrammarRequest) (sqlext.GrammarResult, error) {
	return sqlschema.ConvertScript(ctx, request, script{})
}

// script is the owner's side of the conversion: it parses YQL and converts
// the statements the shared conversion does not model.
type script struct{}

// Parse parses one YQL document.
func (script) Parse(_ context.Context, file sqlext.SourceFile) (*ast.StatementList, error) {
	return yqlparse.Parse(file.Text)
}

// Statement converts a YDB statement: an object of the YDB owner's models, a
// principal, a permission, a comment, or a sequence setting.
func (script) Statement(scope sqlschema.Scope, statement ast.Node) (bool, error) {
	return convertStatement(scope, statement)
}

// AlterOperation applies `ALTER TABLE ... ADD CHANGEFEED`.
func (script) AlterOperation(_ sqlschema.Scope, table sqlschema.Table, operation *ast.ExtensionAlterOperation) (bool, error) {
	added, ok := operation.Payload.(*ydbast.AddChangefeed)
	if !ok {
		return false, nil
	}
	return true, appendChangefeed(table, added.Changefeed)
}

// claim enrolls the namespaces a YQL document can declare, less the objects
// limits leave unmanaged.
func claim(request sqlext.CoverageRequest) (schemaext.Coverage, error) {
	var recorded ydbsource.Limits
	for _, limit := range request.Limits {
		recorded.Add(string(limit.Kind), limit.Name)
	}
	return ydbsource.Coverage(recorded)
}
