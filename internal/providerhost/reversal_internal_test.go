package providerhost

// White-box testing required: the wire request a reversal service sends and
// the binding of its reply are private adapter state between the engine
// runtime and the provider process. A public test would need a provider
// whose comparison returns a change its planner then plans in reverse, and
// the SQLite fixture provider has no such executable native change.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

const (
	reversedObject = "example.org/fixture/node"
	reversedChange = "example.org/fixture/node-change"
)

// reversalFixture is a reversal service of the fixture engine for the node
// change kind, with the observed node model its forward states use, and one
// captured node change at app.workers.
func reversalFixture(c *qt.C) (modelReverser, schemaext.ReversalRequest) {
	selected := yamlIdentityTarget()
	selected.engine.Features = []string{provider.ReverseChangesFeature}
	models := make(map[modelKey]*modelContract)
	for _, key := range []modelKey{{kind: reversedChange, representation: schemaext.Change}, {kind: reversedObject, representation: schemaext.Observed}} {
		models[key] = &modelContract{key: key, signature: string(key.kind), engines: map[string]*protocol.ValueModel{"fixture": selected.engine.ValueModels[0]}}
	}
	selected.host.models = models
	declaration := &protocol.ModelReversal{Id: "nodes", ChangeKinds: []string{reversedChange}}
	selected.engine.Reversals = []*protocol.ModelReversal{declaration}
	change := reversalValue(c, selected, reversedChange, schemaext.Change, `{"after":{"mode":"strict"},"before":null}`)
	subject := objectidentity.ID{Kind: reversedObject, Schema: objectidentity.Part{Source: "app", Normalized: "app"}, Name: objectidentity.Part{Source: "workers", Normalized: "workers"}}
	request := schemaext.ReversalRequest{Target: "fixture", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Of(map[capability.Capability]bool{capability.CoordinationNodes: true}),
		Changes: []schemaext.ChangeRecord{{Subject: subject, Value: &resolvedChange{model: change}}}}
	return modelReverser{target: selected, declaration: declaration}, request
}

// reversalValue is a value of kind in representation, as the provider
// returned it under the fixture engine's profile with the reversal feature.
func reversalValue(c *qt.C, selected *target, kind string, representation schemaext.Representation, payload string) *resolvedModel {
	c.Helper()
	owner := &protocol.Reference{Engine: "fixture", Kind: reversedObject, Schema: "app", Name: "workers"}
	value := &protocol.NormalizedValue{Value: &protocol.Envelope{Kind: kind, Owner: owner, Payload: []byte(payload)}, NormalizedOwner: owner, CanonicalFormat: "fixture", CanonicalProjection: []byte(payload)}
	if representation == schemaext.Change {
		value.ChangeSemantics = &protocol.ChangeSemantics{ReplacesOwner: new(false), RemovesState: new(false), Impact: protocol.ChangeSemantics_ADDITIVE, Reason: "Creates the node",
			Access: protocol.ChangeSemantics_UNCHANGED, AccessReason: "No access change"}
	}
	model, err := selected.host.models[modelKey{kind: schemaext.Kind(kind), representation: representation}].resolved(
		modelWire{Format: 1, Engine: "fixture", Features: []string{provider.ReverseChangesFeature}}, value)
	c.Assert(err, qt.IsNil)
	return model
}

// The runtime's registration names the declared service and its kinds.
func TestRegisterReversals_DeclaresEachService(t *testing.T) {
	c := qt.New(t)
	service, _ := reversalFixture(c)
	var result engine.Provider

	service.target.registerReversals(&result)

	c.Assert(result.Reversals, qt.HasLen, 1)
	c.Assert(result.Reversals[0].Target, qt.Equals, "fixture")
	c.Assert(result.Reversals[0].Kinds, qt.DeepEquals, []schemaext.Kind{reversedChange})
	c.Assert(result.Reversals[0].Service, qt.Equals, schemaext.ReversalService(service))
}

// A change travels as the comparison returned it, under the profile it was
// captured with, beside the target's identifier rules and capabilities.
func TestReversalRequest_SendsTheCapturedChanges(t *testing.T) {
	c := qt.New(t)
	service, request := reversalFixture(c)

	remote, profile, err := service.reversalRequest(request)

	c.Assert(err, qt.IsNil)
	c.Assert(remote.Engine, qt.Equals, "fixture")
	c.Assert(remote.ReversalId, qt.Equals, "nodes")
	c.Assert(remote.TargetFeatures, qt.DeepEquals, []string{provider.ReverseChangesFeature})
	c.Assert(profile.TargetFeatures, qt.DeepEquals, []string{provider.ReverseChangesFeature})
	c.Assert(remote.Capabilities, qt.DeepEquals, map[string]bool{"coordination_nodes": true})
	c.Assert(remote.Identifiers.Rules.TableNames, qt.Equals, protocol.IdentifierRules_EXACT)
	c.Assert(remote.Changes, qt.HasLen, 1)
	c.Assert(remote.Changes[0].Subject.Name.Source, qt.Equals, "workers")
	c.Assert(remote.Changes[0].Subject.Kind, qt.Equals, reversedObject)
	c.Assert(remote.Changes[0].Value.Id, qt.Equals, "0")
	c.Assert(string(remote.Changes[0].Value.Value.Payload), qt.Equals, `{"after":{"mode":"strict"},"before":null}`)
}

// A result keeps the runtime's subject and carries the reverse change, the
// predicted states, the strategy and the limitations.
func TestBoundReversals_BindsEachResult(t *testing.T) {
	c := qt.New(t)
	service, request := reversalFixture(c)
	remote, profile, err := service.reversalRequest(request)
	c.Assert(err, qt.IsNil)
	reverse := reversalValue(c, service.target, reversedChange, schemaext.Change, `{"after":null,"before":{"mode":"strict"}}`)
	predicted := reversalValue(c, service.target, reversedObject, schemaext.Observed, `{"mode":"strict"}`)
	reply := &protocol.ReversedChanges{Complete: true, Changes: []*protocol.ReversedChange{{
		Subject: remote.Changes[0].Subject, Value: reverse.normalized, Strategy: "drop the created coordination node",
		Limitations: []string{"Dropping the new node discards its semaphores and rate limiter resources."},
		ForwardState: []*protocol.ForwardState{
			{Placement: protocol.ForwardState_OBJECT, Kind: reversedObject, Value: predicted.normalized},
			{Placement: protocol.ForwardState_FACET, Kind: reversedObject},
		},
	}}}

	result, err := service.boundReversals(profile, request, reply)

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.HasLen, 1)
	c.Assert(result[0].Change.Subject, qt.DeepEquals, request.Changes[0].Subject)
	c.Assert(result[0].Change.Value.Kind(), qt.Equals, schemaext.Kind(reversedChange))
	c.Assert(result[0].Strategy, qt.Equals, "drop the created coordination node")
	c.Assert(result[0].Limitations, qt.DeepEquals, []string{"Dropping the new node discards its semaphores and rate limiter resources."})
	c.Assert(result[0].ForwardState, qt.HasLen, 2)
	c.Assert(result[0].ForwardState[0].Placement, qt.Equals, schemaext.ObjectPlacement)
	c.Assert(result[0].ForwardState[0].Kind, qt.Equals, schemaext.Kind(reversedObject))
	c.Assert(result[0].ForwardState[0].Value.Kind(), qt.Equals, schemaext.Kind(reversedObject))
	c.Assert(result[0].ForwardState[1].Placement, qt.Equals, schemaext.FacetPlacement)
	c.Assert(result[0].ForwardState[1].Value, qt.IsNil)
}

// A reverse with nothing to run keeps its subject and no change value.
func TestBoundReversals_KeepsAReverseWithNothingToRun(t *testing.T) {
	c := qt.New(t)
	service, request := reversalFixture(c)
	remote, profile, err := service.reversalRequest(request)
	c.Assert(err, qt.IsNil)
	reply := &protocol.ReversedChanges{Complete: true, Changes: []*protocol.ReversedChange{{
		Subject: remote.Changes[0].Subject, Strategy: "keep the rotated value", Limitations: []string{"The server never returns a secret's value."},
	}}}

	result, err := service.boundReversals(profile, request, reply)

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.HasLen, 1)
	c.Assert(result[0].Change.Subject, qt.DeepEquals, request.Changes[0].Subject)
	c.Assert(result[0].Change.Value, qt.IsNil)
}

func TestBoundReversals_FailurePath(t *testing.T) {
	c := qt.New(t)
	service, request := reversalFixture(c)
	_, profile, err := service.reversalRequest(request)
	c.Assert(err, qt.IsNil)

	result, err := service.boundReversals(profile, request, &protocol.ReversedChanges{Complete: true})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
}
