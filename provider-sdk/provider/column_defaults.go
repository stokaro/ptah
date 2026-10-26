package provider

import (
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ValidateColumnDefaultsRequest validates every observed column, including
// observed-only columns used to construct rollback operands.
func ValidateColumnDefaultsRequest(description *protocol.Description, request *protocol.ReadColumnDefaultsRequest) error {
	if _, err := columnsTarget(description, request.GetEngine(), request.GetTargetFeatures()); err != nil {
		return err
	}
	if err := validatePlanningContext(description, &protocol.PlanFeaturesRequest{Identifiers: request.GetIdentifiers(), Capabilities: request.GetCapabilities()}); err != nil {
		return err
	}
	var identities []*protocol.ComparisonIdentity
	for _, input := range request.GetColumns() {
		if input.GetColumn() == nil {
			return columnsError("default readback requires an observed column")
		}
		if err := validateColumnIdentity(input.GetSubject(), input.GetColumn().GetName(), request.GetIdentifiers()); err != nil {
			return err
		}
		identities = append(identities, input.Subject)
	}
	return validateComparisonIdentities(identities)
}

// ValidateColumnDefaults requires typed, complete, ordered readback. Explicit
// empty literals remain distinct from absent defaults and empty expressions.
func ValidateColumnDefaults(description *protocol.Description, request *protocol.ReadColumnDefaultsRequest, result *protocol.ReadColumnDefaultsResult) error {
	if err := ValidateColumnDefaultsRequest(description, request); err != nil {
		return err
	}
	if !result.GetComplete() || len(result.GetColumns()) != len(request.GetColumns()) {
		return columnsError("incomplete default readback")
	}
	for i, receipt := range result.GetColumns() {
		input := request.Columns[i]
		if !proto.Equal(receipt.GetSubject(), input.Subject) {
			return columnsError("a default receipt changes identity or order")
		}
		switch receipt.GetKind() {
		case protocol.ColumnDefaultReceipt_ABSENT:
			if input.Column.DefaultExpression != nil || receipt.Value != "" {
				return columnsError("an absent default requires absent input and empty output")
			}
		case protocol.ColumnDefaultReceipt_LITERAL:
			if input.Column.DefaultExpression == nil {
				return columnsError("a literal cannot invent an absent default")
			}
		case protocol.ColumnDefaultReceipt_EXPRESSION:
			if !comparisonReason(input.Column.GetDefaultExpression()) || !comparisonReason(receipt.Value) {
				return columnsError("an expression requires nonempty input and output")
			}
		default:
			return columnsError("unknown default receipt kind")
		}
	}
	return nil
}
