package providerhost

import (
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemamodel"
)

func copyPlanningPointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	return new(*value)
}
func planningInt64Pointer(value *int) *int64 {
	if value == nil {
		return nil
	}
	return new(int64(*value))
}
func optionalPlanningIdentity(value objectidentity.ID) *protocol.ComparisonIdentity {
	if value == (objectidentity.ID{}) {
		return nil
	}
	return comparisonIdentity(value)
}
func declaredPlanningIndexParts(parts []schemamodel.IndexPart) []*protocol.CapturedIndexPart {
	result := make([]*protocol.CapturedIndexPart, len(parts))
	for i, part := range parts {
		result[i] = &protocol.CapturedIndexPart{Column: part.Name, Expression: part.Expr, OperatorClass: part.Operator, Prefix: part.Prefix, Descending: part.Desc, Nulls: part.NullsOrder}
	}
	return result
}
func observedPlanningIndexParts(parts []catalog.IndexPart) []*protocol.CapturedIndexPart {
	result := make([]*protocol.CapturedIndexPart, len(parts))
	for i, part := range parts {
		result[i] = &protocol.CapturedIndexPart{Column: part.Name, Expression: part.Expr, OperatorClass: part.Operator, Prefix: part.Prefix, Descending: part.Desc, Nulls: part.NullsOrder}
	}
	return result
}
