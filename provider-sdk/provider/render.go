package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ValidateRenderRequest checks the complete rendering batch before owner code
// runs. Native operations must use the negotiated OPERATION model.
func ValidateRenderRequest(description *protocol.Description, request *protocol.RenderRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), RenderFeature) {
		return fmt.Errorf("%w: rendering is unavailable", ErrProtocol)
	}
	if _, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures()); err != nil {
		return err
	}
	for name := range request.GetCapabilities() {
		if !comparisonReason(name) {
			return fmt.Errorf("%w: invalid rendering capability name", ErrProtocol)
		}
	}
	seen := make(map[string]bool)
	for _, change := range request.GetChanges() {
		if !comparisonReason(change.GetId()) || seen[change.GetId()] || change.GetObject() == nil || change.GetObject().GetName() == "" {
			return fmt.Errorf("%w: missing or duplicate rendering identity", ErrProtocol)
		}
		seen[change.GetId()] = true
		if err := validateRenderOperation(description, request, change); err != nil {
			return err
		}
	}
	return nil
}

func validateRenderOperation(description *protocol.Description, request *protocol.RenderRequest, change *protocol.Change) error {
	switch operation := change.GetOperation().(type) {
	case *protocol.Change_Native:
		if operation == nil {
			return fmt.Errorf("%w: missing native rendering operation", ErrProtocol)
		}
		if err := validateNativeOperation(description, request, change); err != nil {
			return err
		}
	case *protocol.Change_CreateTable:
		if operation == nil || operation.CreateTable == nil {
			return fmt.Errorf("%w: missing table creation", ErrProtocol)
		}
	case *protocol.Change_DropTable:
		if operation == nil || operation.DropTable == nil {
			return fmt.Errorf("%w: missing table removal", ErrProtocol)
		}
	case *protocol.Change_AddColumn:
		if operation == nil || operation.AddColumn == nil {
			return fmt.Errorf("%w: missing column addition", ErrProtocol)
		}
	case *protocol.Change_DropColumn:
		if operation == nil || operation.DropColumn == "" {
			return fmt.Errorf("%w: missing column removal", ErrProtocol)
		}
	default:
		return fmt.Errorf("%w: missing rendering operation", ErrProtocol)
	}
	return nil
}

func validateNativeOperation(description *protocol.Description, request *protocol.RenderRequest, change *protocol.Change) error {
	operation := change.GetNative()
	value := operation.GetValue()
	if value.GetId() != change.GetId() || !proto.Equal(change.GetObject(), value.GetValue().GetOwner()) {
		return fmt.Errorf("%w: native rendering identity changed", ErrProtocol)
	}
	switch operation.GetRole() {
	case protocol.NativeOperation_STATEMENT:
		if operation.GetParent() != nil {
			return fmt.Errorf("%w: standalone operation carries a table parent", ErrProtocol)
		}
	case protocol.NativeOperation_ALTER_TABLE:
		if !validValueReference(operation.GetParent(), request.GetEngine()) || operation.GetParent().GetKind() != "table" {
			return fmt.Errorf("%w: table operation requires its actual parent", ErrProtocol)
		}
	default:
		return fmt.Errorf("%w: unknown operation role", ErrProtocol)
	}
	inputs := &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(),
		Values: []*protocol.ValueInput{{Id: value.GetId(), Representation: protocol.ValueInput_OPERATION, Value: value.GetValue()}}}
	return ValidateNormalizedValues(description, inputs, &protocol.NormalizedValues{Complete: true, Values: []*protocol.NormalizedValue{value}})
}

// ValidateRenderedBatch binds output validation to the negotiated request. A
// failed batch has no executable prefix, even when earlier units are well formed.
func ValidateRenderedBatch(description *protocol.Description, request *protocol.RenderRequest, result *protocol.RenderResult) error {
	if err := ValidateRenderRequest(description, request); err != nil {
		return err
	}
	return ValidateRender(request, result)
}
