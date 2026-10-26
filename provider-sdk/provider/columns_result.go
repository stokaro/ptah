package provider

import (
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ValidateComparedColumns requires an ordered receipt for each requested
// aspect. A failed batch exposes no successful prefix or invented operations.
func ValidateComparedColumns(description *protocol.Description, request *protocol.CompareColumnsRequest, result *protocol.ComparedColumns) error {
	if err := ValidateColumnsRequest(description, request); err != nil {
		return err
	}
	if !result.GetComplete() || len(result.GetTables()) != len(request.GetTables()) {
		return columnsError("incomplete comparison")
	}
	for i, table := range result.GetTables() {
		input := request.Tables[i]
		if !proto.Equal(table.GetSubject(), input.Subject) || !proto.Equal(table.GetCurrentSubject(), input.CurrentSubject) || len(table.GetColumns()) != len(input.Pairs) {
			return columnsError("a table receipt changes identity, order or pair inventory")
		}
		for j, column := range table.GetColumns() {
			pair := input.Pairs[j]
			if !proto.Equal(column.GetSubject(), pair.Subject) || !proto.Equal(column.GetCurrentSubject(), pair.CurrentSubject) || len(column.GetAspects()) != len(pair.Aspects) {
				return columnsError("a column receipt changes identity, order or aspect inventory")
			}
			for k, receipt := range column.GetAspects() {
				if receipt.GetAspect() != pair.Aspects[k] {
					return columnsError("an aspect receipt changes input order")
				}
				if err := validateColumnReceipt(receipt); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateColumnReceipt(receipt *protocol.ColumnAspectReceipt) error {
	switch receipt.GetAspect() {
	case protocol.ColumnAspect_COLUMN_TYPE, protocol.ColumnAspect_COLUMN_DEFAULT:
		difference := receipt.GetDifference()
		if difference.GetRelation() != protocol.ColumnDifference_SAME && difference.GetRelation() != protocol.ColumnDifference_CHANGED {
			return columnsError("a difference needs SAME or CHANGED")
		}
		if receipt.Aspect == protocol.ColumnAspect_COLUMN_TYPE && difference.Relation == protocol.ColumnDifference_CHANGED &&
			(!comparisonReason(difference.Current) || !comparisonReason(difference.Desired)) {
			return columnsError("a changed type requires both diagnostic spellings")
		}
	case protocol.ColumnAspect_COLUMN_NULLABILITY:
		if receipt.GetNullability() == nil || receipt.GetNullability().Nullable == nil {
			return columnsError("a nullability receipt requires an explicit boolean")
		}
	case protocol.ColumnAspect_COLUMN_SEQUENCE:
		if receipt.GetSequence() == nil {
			return columnsError("a sequence receipt is required even for no changes")
		}
		return validateColumnSequence(receipt.GetSequence())
	default:
		return columnsError("unknown aspect receipt")
	}
	return nil
}

func validateColumnSequence(sequence *protocol.ColumnSequenceDifferences) error {
	seen := make(map[protocol.ColumnSequenceDifference_Attribute]bool)
	for _, change := range sequence.GetChanges() {
		attribute := change.GetAttribute()
		if (attribute != protocol.ColumnSequenceDifference_IDENTITY_START && attribute != protocol.ColumnSequenceDifference_IDENTITY_INCREMENT) || seen[attribute] {
			return columnsError("a sequence delta has an unknown or repeated attribute")
		}
		if change.GetCurrent() == change.GetDesired() {
			return columnsError("a sequence delta needs distinct current and desired settings")
		}
		seen[attribute] = true
	}
	return nil
}
