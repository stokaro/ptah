package runtime

// White-box testing required: the server adapter answers Describe without a
// process, and the client's negotiation is the unexported step a started
// client takes on the description it receives.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

const (
	selectionTopic = "example.org/selections/topic"
	selectionPool  = "example.org/selections/pool"
)

// selectionsDescription declares a topic that grants name and a pool by a
// database-wide name, on an engine that lists scope.selections.
func selectionsDescription() *protocol.Description {
	model := func(kind string, representation protocol.ValueInput_Representation) *protocol.ValueModel {
		return &protocol.ValueModel{Kind: kind, Representation: representation, SchemaVersion: 1, Encoding: "json",
			DefinitionJson: []byte(`{"type":"object"}`), DefinitionSha256: minorDefinition, CanonicalFormat: "json-v1"}
	}
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/selections", Version: "1", SemanticIdentity: "selections-1",
		Features: []string{provider.ScopeSelectionsFeature},
		Engines: []*protocol.Engine{{
			Id: "selections", Features: []string{provider.ScopeSelectionsFeature},
			Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
				IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE},
			ValueModels: []*protocol.ValueModel{model(selectionTopic, protocol.ValueInput_DESIRED), model(selectionTopic, protocol.ValueInput_OBSERVED),
				model(selectionPool, protocol.ValueInput_DESIRED), model(selectionPool, protocol.ValueInput_OBSERVED)},
			ScopeSelections: []*protocol.ScopeSelection{
				{Kind: selectionTopic, SelectorType: "topic", Naming: protocol.ScopeSelection_IN_SCHEMA, GrantObjectType: "TOPIC", KeepsRequired: []string{selectionPool}},
				{Kind: selectionPool, SelectorType: "resource_pool", Naming: protocol.ScopeSelection_DATABASE_WIDE},
			},
		}},
	}
}

// A host that lists scope.selections receives every selection; a host that
// lists none, such as one of contract 25.3, receives a valid description
// without them.
func TestScopeSelectionsAreTwoWayAcrossDescribe(t *testing.T) {
	c := qt.New(t)
	server := Server{Description: selectionsDescription(), Sessions: &sessionOwner{}}
	c.Assert(server.Validate(), qt.IsNil)
	listed, err := newServerAdapter(server).Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision,
		HostContract: provider.Contract(), HostFeatures: []string{provider.ScopeSelectionsFeature}})
	c.Assert(err, qt.IsNil)
	c.Assert(listed.Engines[0].ScopeSelections, qt.HasLen, 2)
	c.Assert(listed.Engines[0].Features, qt.DeepEquals, []string{provider.ScopeSelectionsFeature})

	older, err := newServerAdapter(server).Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision,
		HostContract: &protocol.ContractVersion{Major: provider.Revision, Minor: 3}})
	c.Assert(err, qt.IsNil)
	c.Assert(older.Engines[0].ScopeSelections, qt.HasLen, 0)
	c.Assert(older.Engines[0].Features, qt.HasLen, 0)
	c.Assert(provider.ValidateDescription(older, nil), qt.IsNil)
}

// A host keeps the selections only when it lists the feature, whatever the
// provider sent, and a provider without selections negotiates with a host
// that lists it.
func TestScopeSelectionsNegotiateOnTheHost(t *testing.T) {
	c := qt.New(t)
	kept, err := negotiate(selectionsDescription(), Options{HostFeatures: []string{provider.ScopeSelectionsFeature}})
	c.Assert(err, qt.IsNil)
	c.Assert(kept.Engines[0].ScopeSelections, qt.HasLen, 2)
	dropped, err := negotiate(selectionsDescription(), Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(dropped.Engines[0].ScopeSelections, qt.HasLen, 0)
	without := selectionsDescription()
	without.Features, without.Engines[0].Features, without.Engines[0].ScopeSelections = nil, nil, nil
	_, err = negotiate(without, Options{HostFeatures: []string{provider.ScopeSelectionsFeature}})
	c.Assert(err, qt.IsNil)
	broken := selectionsDescription()
	broken.Engines[0].ScopeSelections[0].KeepsRequired = []string{selectionTopic}
	_, err = negotiate(broken, Options{HostFeatures: []string{provider.ScopeSelectionsFeature}})
	c.Assert(err, qt.ErrorMatches, `.*a kept requirement names no other selection, or names one twice`)
}
