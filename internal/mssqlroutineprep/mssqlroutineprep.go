// Package mssqlroutineprep lowers SQL Server routine declarations to the
// parameter spelling its catalog reports before common comparison and facet
// capture. The owner supplies the grammar; the host keeps its identity contract.
package mssqlroutineprep

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/mssql/mssqlroutine"
	"ptah.run/internal/routineargs"
)

// Service lowers routine parameters through SQL Server's owner. Its zero value
// is ready for concurrent use and holds no catalog connection.
type Service struct{}

// LowerDesired returns a copy whose parameter lists omit optional AS tokens.
// Subject-specific coverage follows the normalized routine identity. Inputs are
// not modified. Nil context or schema, cancellation, and another target return
// an error and no schema. Conflicting normalized coverage is refused.
func (Service) LowerDesired(ctx context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
	if ctx == nil || request.Desired == nil {
		return nil, fmt.Errorf("%w: SQL Server routine lowering requires a context and desired schema", schemapreparation.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if platform.NormalizeDialect(request.Target) != platform.SQLServer {
		return nil, fmt.Errorf("%w: SQL Server routine lowering on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := *request.Desired
	result.Functions = slices.Clone(result.Functions)
	subjects := make(map[objectidentity.Key]objectidentity.ID)
	builder := objectidentity.NewBuilder(request.Semantics.Normalize(platform.SQLServer))
	for i := range result.Functions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		function := &result.Functions[i]
		before := routineargs.Subject(builder, function.Kind, function.Name, function.Parameters)
		function.Parameters = mssqlroutine.NormalizeParameters(function.Parameters)
		subjects[before.Key()] = routineargs.Subject(builder, function.Kind, function.Name, function.Parameters)
	}
	if result.FeatureCoverage.IsZero() {
		return &result, nil
	}
	claims := result.FeatureCoverage.SubjectRecords()
	for i := range claims {
		if subject, found := subjects[claims[i].Subject.Key()]; found {
			claims[i].Subject = subject
		}
	}
	coverage, err := schemaext.NewCoverage(result.FeatureCoverage.Representation(), result.FeatureCoverage.KindRecords(), claims)
	if err != nil {
		return nil, err
	}
	result.FeatureCoverage = coverage
	return &result, nil
}
