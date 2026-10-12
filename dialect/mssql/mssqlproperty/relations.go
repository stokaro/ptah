package mssqlproperty

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// RelationService reports the table each extended property hangs off. Its
// zero value is ready for concurrent use.
//
// A property on a table, or on a column of one, is attached to the table: SQL
// Server drops the property with the table, and a scope keeps or leaves it
// with the table, but the property is not the table's child, since its owner
// plans its changes alone (see [Property.Ref]). A property on the database or
// on a schema is attached to nothing. A property references no other object,
// so every record is complete.
type RelationService struct{}

// DescribeRelations returns one record per value, in input order.
func (RelationService) DescribeRelations(ctx context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
	if err := checkTarget(ctx, request.Target, "relation discovery"); err != nil {
		return schemaext.RelationResult{}, err
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	result := schemaext.RelationResult{Complete: true, Values: make([]schemaext.ValueRelations, 0, len(request.Values))}
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return schemaext.RelationResult{}, err
		}
		var property Property
		switch typed := value.Value.(type) {
		case *DesiredProperty:
			property = typed.Property
		case *ObservedProperty:
			property = typed.Property
		default:
			return schemaext.RelationResult{}, fmt.Errorf("%w: unexpected extended property relation value %T", schemaext.ErrInvalidValue, value.Value)
		}
		record := schemaext.ValueRelations{Subject: value.Subject, Complete: true}
		if property.Table != "" {
			record.Attachment = builder.TableParts(property.Schema, property.Table)
		}
		result.Values = append(result.Values, record)
	}
	return result, ctx.Err()
}
