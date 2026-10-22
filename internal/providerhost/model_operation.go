package providerhost

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// resolvedOperation contains captured safety facts and typed owner input. Its
// local interfaces never render SQL or reach the provider process.
type resolvedOperation struct{ model *resolvedModel }

func (v *resolvedOperation) Subject() objectidentity.ID { return v.model.subject }

func (v *resolvedOperation) Kind() schemaext.Kind { return v.model.Kind() }
func (v *resolvedOperation) CloneExtension() ast.ExtensionPayload {
	return &resolvedOperation{model: v.model.cloneModel()}
}
func (v *resolvedOperation) Effect() schemaext.Effect {
	facts := v.model.normalized.GetOperationSemantics()
	impacts := map[protocol.ChangeSemantics_Impact]schemaext.Impact{protocol.ChangeSemantics_ADDITIVE: schemaext.Additive, protocol.ChangeSemantics_BEHAVIORAL: schemaext.Behavioral, protocol.ChangeSemantics_DESTRUCTIVE: schemaext.Destructive}
	return schemaext.Effect{Impact: impacts[facts.GetImpact()], Reason: facts.GetReason()}
}
func (v *resolvedOperation) AccessEffect() schemaext.AccessEffect {
	facts := v.model.normalized.GetOperationSemantics()
	access := map[protocol.ChangeSemantics_Access]schemaext.Access{protocol.ChangeSemantics_ACCESS_UNKNOWN: schemaext.AccessUnknown,
		protocol.ChangeSemantics_UNCHANGED: schemaext.AccessUnchanged,
		protocol.ChangeSemantics_WIDENS:    schemaext.AccessWidens, protocol.ChangeSemantics_NARROWS: schemaext.AccessNarrows}
	return schemaext.AccessEffect{Access: access[facts.GetAccess()], Reason: facts.GetAccessReason()}
}

type operationFacts struct {
	Impact       string `json:"impact"`
	Reason       string `json:"reason"`
	Access       string `json:"access"`
	AccessReason string `json:"access_reason"`
}

func normalizedOperationFacts(facts *protocol.OperationSemantics) *operationFacts {
	if facts == nil {
		return nil
	}
	return &operationFacts{Impact: facts.GetImpact().String(), Reason: facts.GetReason(), Access: facts.GetAccess().String(), AccessReason: facts.GetAccessReason()}
}

func (t *target) nativeOperation(payload ast.ExtensionPayload, role protocol.NativeOperation_Role, parent *protocol.Reference) (*protocol.Change, error) {
	value, err := t.operationValue(payload)
	if err != nil {
		return nil, err
	}
	return &protocol.Change{Object: value.wire.Subject.protocol(), Operation: &protocol.Change_Native{Native: &protocol.NativeOperation{Value: proto.CloneOf(value.normalized), Role: role, Parent: proto.CloneOf(parent)}}}, nil
}

// operationValue checks the frozen executable, model, and target profile before
// either rendering or executing a captured operation.
func (t *target) operationValue(payload ast.ExtensionPayload) (*resolvedModel, error) {
	if payload == nil {
		return nil, fmt.Errorf("%w: missing native operation", schemaext.ErrInvalidValue)
	}
	operation, ok := payload.(*resolvedOperation)
	if !ok || operation == nil || operation.model == nil {
		return nil, fmt.Errorf("%w: uncaptured native operation", schemaext.ErrInvalidValue)
	}
	contract := t.host.models[modelKey{kind: operation.Kind(), representation: schemaext.Operation}]
	if contract == nil {
		return nil, fmt.Errorf("%w: unowned native operation", schemaext.ErrInvalidValue)
	}
	value, err := contract.value(operation)
	if err != nil {
		return nil, err
	}
	if value.wire.Engine != t.engine.GetId() || !slices.Equal(featureSet(value.wire.Features), featureSet(t.engine.GetFeatures())) {
		return nil, fmt.Errorf("%w: native operation target profile changed", schemaext.ErrIncompatibleCodec)
	}
	return value, nil
}
