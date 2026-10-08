// Package chprepare resolves ClickHouse column key membership before shared
// comparison. Key clauses belong to the dialect, not to the common comparator.
package chprepare

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/clickhouse/internal/chkey"
)

// Service resolves desired column primary-key flags from ClickHouse key clauses.
// It keeps declared facets and all observed state unchanged. The zero value is
// usable and safe for concurrent calls; no live server is consulted.
type Service struct{}

// PrepareTables returns independent captures whose column key flags match a
// nonempty PRIMARY KEY clause, or ORDER BY when no primary key is declared.
// Tables without either clause retain their column flags. Nil context returns
// schemapreparation.ErrInvalid; cancellation returns its context error with no
// partial result. Only ClickHouse and its platform aliases are accepted.
func (Service) PrepareTables(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	if ctx == nil {
		return schemapreparation.Result{}, fmt.Errorf("%w: preparation requires a context", schemapreparation.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	if platform.NormalizeDialect(request.Target) != platform.ClickHouse {
		return schemapreparation.Result{}, fmt.Errorf("%w: ClickHouse preparation target %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request = request.Clone()
	for i := range request.Tables {
		prepareTable(&request.Tables[i])
	}
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	return schemapreparation.Result{Complete: true, Tables: request.Tables}, nil
}

func prepareTable(table *schemapreparation.Table) {
	names := make([]string, 0, len(table.Desired.Fields))
	for _, field := range table.Desired.Fields {
		names = append(names, field.Name)
	}
	keys, declared := chkey.PrimaryKeyColumns(table.Desired.Table, names)
	if !declared {
		return
	}
	for i := range table.Desired.Fields {
		table.Desired.Fields[i].Primary = keys[table.Desired.Fields[i].Name]
	}
	table.ColumnPrimaryKeysPrepared = true
}
