package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func operationRender() (*protocol.Description, *protocol.RenderRequest) {
	d, inputs := valueInputs()
	d.Features = append(d.Features, provider.RenderFeature)
	d.Engines[0].ValueModels[0].Representation = protocol.ValueInput_OPERATION
	inputs.Values[0].Representation = protocol.ValueInput_OPERATION
	value := normalizedValues(inputs).Values[0]
	value.OperationSemantics = &protocol.OperationSemantics{Impact: protocol.ChangeSemantics_BEHAVIORAL, Reason: "Changes stored data", Access: protocol.ChangeSemantics_UNCHANGED, AccessReason: "Does not change grants"}
	return d, &protocol.RenderRequest{Engine: inputs.Engine, TargetFeatures: inputs.TargetFeatures, Capabilities: map[string]bool{"feature": false}, Changes: []*protocol.Change{{Id: value.Id, Object: proto.CloneOf(value.Value.Owner), Operation: &protocol.Change_Native{Native: &protocol.NativeOperation{Value: value, Role: protocol.NativeOperation_STATEMENT}}}}}
}

func TestRenderAcceptsCapturedOperationsAndTableParents(t *testing.T) {
	c := qt.New(t)
	d, request := operationRender()
	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
	operation := request.Changes[0].GetNative()
	operation.Role = protocol.NativeOperation_ALTER_TABLE
	operation.Parent = &protocol.Reference{Engine: "fixture", Kind: "table", Database: "tenant", Schema: "a.b", Path: []string{"a/b", "c"}, Name: "parent"}
	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
}

func TestRenderRefusesForeignOrUncapturedOperations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.RenderRequest)
	}{
		{"unadvertised", func(d *protocol.Description, _ *protocol.RenderRequest) { d.Features = nil }},
		{"foreign engine", func(_ *protocol.Description, r *protocol.RenderRequest) { r.Engine = "other" }},
		{"wrong representation", func(d *protocol.Description, _ *protocol.RenderRequest) {
			d.Engines[0].ValueModels[0].Representation = protocol.ValueInput_CHANGE
		}},
		{"wrong owner", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].GetNative().Value.Value.ProviderId = "other"
		}},
		{"changed subject", func(_ *protocol.Description, r *protocol.RenderRequest) { r.Changes[0].Object.Name = "other" }},
		{"missing facts", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].GetNative().Value.OperationSemantics = nil
		}},
		{"unknown impact", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].GetNative().Value.OperationSemantics.Impact = 99
		}},
		{"unknown access", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].GetNative().Value.OperationSemantics.Access = 99
		}},
		{"missing reason", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].GetNative().Value.OperationSemantics.Reason = ""
		}},
		{"change facts", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].GetNative().Value.ChangeSemantics = &protocol.ChangeSemantics{}
		}},
		{"changed projection", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].GetNative().Value.CanonicalFormat = "other"
		}},
		{"missing role", func(_ *protocol.Description, r *protocol.RenderRequest) { r.Changes[0].GetNative().Role = 0 }},
		{"unknown role", func(_ *protocol.Description, r *protocol.RenderRequest) { r.Changes[0].GetNative().Role = 99 }},
		{"missing parent", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].GetNative().Role = protocol.NativeOperation_ALTER_TABLE
		}},
		{"unexpected parent", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].GetNative().Parent = &protocol.Reference{Engine: "fixture", Kind: "table", Name: "p"}
		}},
		{"missing operation", func(_ *protocol.Description, r *protocol.RenderRequest) { r.Changes[0].Operation = nil }},
		{"nil native", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].Operation = (*protocol.Change_Native)(nil)
		}},
		{"duplicate ID", func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes = append(r.Changes, proto.CloneOf(r.Changes[0]))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := operationRender()
			c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
			test.mutate(d, request)
			c.Assert(provider.ValidateRenderRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
