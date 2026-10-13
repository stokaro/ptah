package pgtablesource

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/postgres/pgtable"
)

func deleteColumnsSQL() sqlext.Extension {
	return sqlext.Extension{Owner: pgtable.Owner, Targets: []string{platform.Postgres, platform.YugabyteDB}, Facets: []schemaext.Kind{pgtable.DeleteColumnsKind},
		Patterns: []sqlext.Pattern{{Name: "delete-columns", Site: sqlext.ReferenceOption, Words: []string{"("}}}, Parser: deleteColumnsParser{},
		Coverage: func(sqlext.CoverageRequest) (schemaext.Coverage, error) {
			return pgtable.DeleteColumnsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
		}}
}

type deleteColumnsParser struct{}

func (deleteColumnsParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	if ctx == nil {
		return sqlext.Result{}, fmt.Errorf("%w: ON DELETE SQL requires a context", schemaext.ErrInvalidValue)
	}
	result := sqlext.Result{}
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		columns, err := readDeleteColumnsClaim(claim)
		if err != nil {
			return sqlext.Result{}, err
		}
		facets, err := pgtable.WithDeleteColumns(schemaext.Facets{}, columns)
		if err != nil {
			return sqlext.Result{}, err
		}
		result.Values = append(result.Values, sqlext.Value{Facets: facets})
	}
	if err := ctx.Err(); err != nil {
		return sqlext.Result{}, err
	}
	return result, nil
}
func readDeleteColumnsClaim(claim sqlext.Claim) ([]string, error) {
	if claim.Pattern != "delete-columns" || claim.Site != sqlext.ReferenceOption || claim.Reference == nil {
		return nil, fmt.Errorf("%w: ON DELETE column list requires reference context", schemaext.ErrInvalidValue)
	}
	reference := claim.Reference
	if reference.Event != "DELETE" || (reference.Action != "SET NULL" && reference.Action != "SET DEFAULT") {
		return nil, fmt.Errorf("%w: a column list after ON %s %s is not supported; only ON DELETE SET NULL and ON DELETE SET DEFAULT take one", schemaext.ErrInvalidValue, reference.Event, reference.Action)
	}
	columns, err := pgtable.ParseDeleteColumnList(claim.Text)
	if err != nil {
		return nil, err
	}
	local := make([]string, len(reference.Columns))
	for i, name := range reference.Columns {
		local[i] = partitionName(name)
	}
	for _, column := range columns {
		if !slices.Contains(local, column) {
			return nil, fmt.Errorf("%w: ON DELETE %s names column %q, which is not part of the foreign key", schemaext.ErrInvalidValue, reference.Action, column)
		}
	}
	return columns, nil
}
