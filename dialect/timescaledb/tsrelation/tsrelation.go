// Package tsrelation describes the relations captured TimescaleDB values depend
// on. It reads captured values only: no database, no source file.
package tsrelation

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
)

// Service reports the hypertable an observed continuous aggregate reads. Its
// zero value is ready for concurrent use.
//
// A hypertable facet depends on nothing beyond the table it is attached to,
// which its subject already names. An observed aggregate depends on the
// hypertable the catalog names; the catalog does not list the other relations
// a body may join, so the record says it is not exhaustive. A declared
// aggregate's body is not parsed, so its record names no relation and says why.
type Service struct{}

// DescribeRelations returns one record per value, in input order.
func (Service) DescribeRelations(ctx context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
	if ctx == nil {
		return schemaext.RelationResult{}, fmt.Errorf("%w: relation discovery requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.RelationResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.RelationResult{}, fmt.Errorf("%w: TimescaleDB relation discovery on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := schemaext.RelationResult{Complete: true, Values: make([]schemaext.ValueRelations, 0, len(request.Values))}
	builder := objectidentity.NewBuilder(request.Identifiers)
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return schemaext.RelationResult{}, err
		}
		record := schemaext.ValueRelations{Subject: value.Subject, Complete: true}
		switch typed := value.Value.(type) {
		case *tsschema.DesiredHypertable, *tsschema.ObservedHypertable:
		case *tsschema.ObservedContinuousAggregate:
			record.Complete, record.Reason = false, "the catalog names the hypertable an aggregate reads, not every relation its body joins"
			if strings.TrimSpace(typed.HypertableName) != "" {
				record.Dependencies = []objectidentity.ID{builder.TableParts(typed.HypertableSchema, typed.HypertableName)}
			}
		case *tsschema.DesiredContinuousAggregate:
			record.Complete, record.Reason = false, "a declared aggregate's body is not parsed for the relations it reads"
		default:
			return schemaext.RelationResult{}, fmt.Errorf("%w: unexpected TimescaleDB relation value %T", schemaext.ErrInvalidValue, value.Value)
		}
		result.Values = append(result.Values, record)
	}
	return result, ctx.Err()
}
