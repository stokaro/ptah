package chprepare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/clickhouse/chresolve"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chkey"
)

// ProjectTableCreations computes ClickHouse storage defaults and key membership
// even for common-only declarations. Computed facets use Desired representation
// for owner conversion; source intent and knowledge limits remain unchanged.
// Excluded storage facets receive no prediction. Invalid declarations, missing
// sorting keys, and cancellation return no partial result. Only ClickHouse is
// accepted; nil context wraps schemaprojection.ErrInvalid.
func (Service) ProjectTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	if ctx == nil {
		return schemaprojection.TableCreationResult{}, fmt.Errorf("%w: creation projection requires a context", schemaprojection.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	if platform.NormalizeDialect(request.Target) != platform.ClickHouse {
		return schemaprojection.TableCreationResult{}, fmt.Errorf("%w: ClickHouse creation projection target %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := schemaprojection.TableCreationResult{Complete: true}
	for _, table := range request.Tables {
		prediction, err := projectCreation(table)
		if err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		indexes, err := projectIndexCreations(table, request.Identifiers)
		if err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		prediction.Facets = append(prediction.Facets, indexes...)
		result.Tables = append(result.Tables, prediction)
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	return result, nil
}

func projectCreation(input schemaprojection.TableCreationInput) (schemaprojection.TableCreation, error) {
	declaration := input.Declaration
	result := schemaprojection.TableCreation{Subject: input.Subject}
	if err := refuseStorageOverrides(declaration.Table.Overrides); err != nil {
		return schemaprojection.TableCreation{}, err
	}
	value, found, err := schemaext.FacetAs[*chschema.DesiredTable](declaration.Table.Facets, chschema.TableKind)
	if err != nil {
		return schemaprojection.TableCreation{}, err
	}
	if !found {
		if slices.Contains(declaration.Table.Facets.DeclaredKinds(), chschema.TableKind) {
			return result, nil
		}
		value = &chschema.DesiredTable{}
	}
	resolved, err := chresolve.Table(chresolve.Request{
		Desired: value, Creating: true, BaseEngine: declaration.Table.Engine, CommonKey: chkey.CommonColumns(declaration),
	})
	if err != nil {
		return schemaprojection.TableCreation{}, err
	}
	facets, err := schemaext.NewFacets(&resolved.Prepared)
	if err != nil {
		return schemaprojection.TableCreation{}, err
	}
	result.Facets = []schemaext.FacetRecord{{Subject: input.Subject, Values: facets}}
	names := make([]string, 0, len(declaration.Fields))
	for _, field := range declaration.Fields {
		names = append(names, field.Name)
	}
	keys := chkey.ReferencedColumns(resolved.Prepared.PrimaryKey.Value, names)
	for _, name := range names {
		if keys[name] {
			result.ColumnPrimaryKeys = append(result.ColumnPrimaryKeys, name)
		}
	}
	result.ColumnPrimaryKeysPrepared = true
	return result, nil
}

func projectIndexCreations(input schemaprojection.TableCreationInput, semantics identifier.Semantics) ([]schemaext.FacetRecord, error) {
	var records []schemaext.FacetRecord
	builder := objectidentity.NewBuilder(semantics)
	for _, index := range input.Declaration.Indexes {
		value, err := declaredIndexSettings(index)
		if err != nil {
			return nil, err
		}
		if value == nil {
			continue
		}
		resolved, err := chresolve.Index(chresolve.IndexRequest{Desired: value, Creating: true})
		if err != nil {
			return nil, err
		}
		facets, err := schemaext.NewFacets(&resolved.Prepared)
		if err != nil {
			return nil, err
		}
		records = append(records, schemaext.FacetRecord{
			Subject: builder.IndexParts(input.Subject.Schema.Source, input.Subject.Name.Source, index.Name), Values: facets,
		})
	}
	return records, nil
}
