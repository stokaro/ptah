// Package mssqllint holds what the SQL Server owner contributes to the
// readers of migration SQL: the splitter that reads a SQL Server migration in
// T-SQL's lexing and judges the statements whose effect only this owner
// knows, as ptah.run/core/lintext describes. A safety assessment
// (ptah.run/migration/safety) reads the judgment in place of the statement's
// words.
//
// The owner judges PolyBase's DROP EXTERNAL TABLE and DROP EXTERNAL DATA
// SOURCE. Neither removes data SQL Server stores: an external table's rows
// live in its source, and a data source is a connection. Each still removes
// what queries read external data through, so each needs review. The bundled
// runtime ptah.run/engine/builtin registers the splitter on the sqlserver
// target.
package mssqllint

import (
	"context"

	"ptah.run/core/lintext"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintscan"
)

// The reasons the owner's judgments give, in the words a safety report shows.
const (
	// DropExternalTableReason is why dropping a PolyBase external table needs
	// review.
	DropExternalTableReason = "DROP EXTERNAL TABLE removes the table SQL Server reads external data through; " +
		"the data stays in its source, and a view, routine or query that reads the table stops working"
	// DropExternalDataSourceReason is why dropping a PolyBase external data
	// source needs review.
	DropExternalDataSourceReason = "DROP EXTERNAL DATA SOURCE removes the connection SQL Server reads " +
		"external data through; the data stays in its source"
)

// Splitter splits SQL Server migrations as the lint scanner does for the
// sqlserver target and reports its judgment of a statement whose effect only
// this owner knows ([lintext.FactSafetyEffect]). It judges no other fact. Its
// zero value is ready to use.
type Splitter struct{}

// SplitStatements splits every source of a request on the sqlserver target,
// in one call, and judges every statement it returns. A missing context wraps
// lintext.ErrInvalid, another target wraps ptaherr.ErrUnsupportedDialect, and
// cancellation while splitting returns the context's error; each returns no
// result.
func (Splitter) SplitStatements(ctx context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	// The scanner refuses a missing context, another target and a canceled
	// one, in the words this splitter would use.
	result, err := lintscan.Splitter{Dialect: platform.SQLServer}.SplitStatements(ctx, request)
	if err != nil {
		return lintext.SplitResult{}, err
	}
	result.Judged = []lintext.Fact{lintext.FactSafetyEffect}
	for i := range result.Files {
		for j := range result.Files[i] {
			statement := &result.Files[i][j]
			if effect, judged := safetyEffect(statement); judged {
				statement.Effects = append(statement.Effects, effect)
			}
		}
	}
	return result, nil
}

// safetyEffect reads the judgment of a PolyBase drop.
func safetyEffect(statement *lintext.Statement) (lintext.Effect, bool) {
	switch {
	case lintkit.HasWordPrefix(statement.Words, "DROP", "EXTERNAL", "TABLE"):
		return lintext.Effect{Fact: lintext.FactSafetyEffect, Impact: schemaext.Behavioral, Reason: DropExternalTableReason}, true
	case lintkit.HasWordPrefix(statement.Words, "DROP", "EXTERNAL", "DATA", "SOURCE"):
		return lintext.Effect{Fact: lintext.FactSafetyEffect, Impact: schemaext.Behavioral, Reason: DropExternalDataSourceReason}, true
	default:
		return lintext.Effect{}, false
	}
}
