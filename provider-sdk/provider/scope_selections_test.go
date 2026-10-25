package provider_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

const (
	topicKind      = "example.org/fixture/topic"
	poolKind       = "example.org/fixture/pool"
	classifierKind = "example.org/fixture/classifier"
)

func selectionModel(kind string, representation protocol.ValueInput_Representation) *protocol.ValueModel {
	return &protocol.ValueModel{Kind: kind, Representation: representation, SchemaVersion: 1, Encoding: "json",
		DefinitionJson: []byte(`{"type":"object"}`), DefinitionSha256: testValueDefinition, CanonicalFormat: "json-v1"}
}

// selectionDescription declares a topic in a schema that grants name, and a
// pool and a classifier by database-wide names, where a kept classifier keeps
// the pool it requires.
func selectionDescription() *protocol.Description {
	d := description()
	d.Features = append(d.Features, provider.ScopeSelectionsFeature)
	engine := d.Engines[0]
	engine.Features = []string{provider.ScopeSelectionsFeature}
	for _, kind := range []string{topicKind, poolKind, classifierKind} {
		engine.ValueModels = append(engine.ValueModels, selectionModel(kind, protocol.ValueInput_DESIRED), selectionModel(kind, protocol.ValueInput_OBSERVED))
	}
	engine.ScopeSelections = []*protocol.ScopeSelection{
		{Kind: topicKind, SelectorType: "topic", Naming: protocol.ScopeSelection_IN_SCHEMA, GrantObjectType: "TOPIC"},
		{Kind: poolKind, SelectorType: "resource_pool", Naming: protocol.ScopeSelection_DATABASE_WIDE, GrantObjectType: "RESOURCE POOL"},
		{Kind: classifierKind, SelectorType: "resource_pool_classifier", Naming: protocol.ScopeSelection_DATABASE_WIDE, KeepsRequired: []string{poolKind}},
	}
	return d
}

func TestScopeSelections_HappyPath(t *testing.T) {
	c := qt.New(t)
	d := selectionDescription()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	// Two kinds may share a selector type that they define alike, and a
	// relation is an in-schema object's.
	d.Engines[0].ScopeSelections[1].SelectorType = "resource_pool_classifier"
	d.Engines[0].ScopeSelections[0].Relation = true
	d.Engines[0].ScopeSelections = append(d.Engines[0].ScopeSelections[:2:2], &protocol.ScopeSelection{Kind: classifierKind,
		SelectorType: "resource_pool_classifier", Naming: protocol.ScopeSelection_DATABASE_WIDE, KeepsRequired: []string{poolKind, topicKind}})
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	// An engine may list the feature and declare nothing.
	d.Engines[0].ScopeSelections = nil
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	d.Engines[0].ScopeSelections = []*protocol.ScopeSelection{{Kind: topicKind, SelectorType: "topic", Naming: protocol.ScopeSelection_SCHEMA_OR_DATABASE}}
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
}

func TestScopeSelections_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
		want   string
	}{
		{"the provider does not list the feature", func(d *protocol.Description) { d.Features = d.Features[:1] }, "lists the feature without the provider"},
		{"the engine does not list the feature", func(d *protocol.Description) { d.Engines[0].Features = nil }, "declares selections without listing the feature"},
		{"a kind the engine does not model", func(d *protocol.Description) { d.Engines[0].ScopeSelections[0].Kind = "example.org/fixture/other" },
			"not a desired and observed model of the engine"},
		{"a kind with no observed model", func(d *protocol.Description) {
			d.Engines[0].ValueModels = d.Engines[0].ValueModels[:len(d.Engines[0].ValueModels)-1]
		}, "not a desired and observed model of the engine"},
		{"a kind with no desired model", func(d *protocol.Description) {
			models := d.Engines[0].ValueModels
			d.Engines[0].ValueModels = append(models[:len(models)-2:len(models)-2], models[len(models)-1])
		}, "not a desired and observed model of the engine"},
		{"a kind selected twice", func(d *protocol.Description) {
			d.Engines[0].ScopeSelections = append(d.Engines[0].ScopeSelections, &protocol.ScopeSelection{Kind: topicKind, SelectorType: "stream",
				Naming: protocol.ScopeSelection_IN_SCHEMA})
		}, "a kind is selected twice"},
		{"an unspecified naming", func(d *protocol.Description) {
			d.Engines[0].ScopeSelections[0].Naming = protocol.ScopeSelection_NAMING_UNSPECIFIED
		}, "needs a known naming"},
		{"an unknown naming", func(d *protocol.Description) { d.Engines[0].ScopeSelections[0].Naming = 9 }, "needs a known naming"},
		{"no selector type", func(d *protocol.Description) { d.Engines[0].ScopeSelections[0].SelectorType = "" }, "a selector type is not lower-case"},
		{"a selector type in capitals", func(d *protocol.Description) { d.Engines[0].ScopeSelections[0].SelectorType = "Topic" },
			"a selector type is not lower-case"},
		{"a selector type starting with a digit", func(d *protocol.Description) { d.Engines[0].ScopeSelections[0].SelectorType = "1topic" },
			"a selector type is not lower-case"},
		{"a selector type with a dot", func(d *protocol.Description) { d.Engines[0].ScopeSelections[0].SelectorType = "ydb.topic" },
			"a selector type is not lower-case"},
		{"a selector type with two namings", func(d *protocol.Description) { d.Engines[0].ScopeSelections[1].SelectorType = "topic" },
			"defined with two namings or relations"},
		{"a selector type with two relations", func(d *protocol.Description) {
			d.Engines[0].ScopeSelections[1].SelectorType, d.Engines[0].ScopeSelections[1].Naming = "topic", protocol.ScopeSelection_IN_SCHEMA
			d.Engines[0].ScopeSelections[1].Relation = true
		}, "defined with two namings or relations"},
		{"a relation named by the database", func(d *protocol.Description) { d.Engines[0].ScopeSelections[1].Relation = true },
			"only an in-schema selection names a relation"},
		{"a grant object type in lower case", func(d *protocol.Description) { d.Engines[0].ScopeSelections[0].GrantObjectType = "topic" },
			"a grant object type is not upper-case words, or is a common target"},
		{"a grant object type with two spaces", func(d *protocol.Description) { d.Engines[0].ScopeSelections[1].GrantObjectType = "RESOURCE  POOL" },
			"a grant object type is not upper-case words, or is a common target"},
		{"a grant object type with an underscore", func(d *protocol.Description) { d.Engines[0].ScopeSelections[1].GrantObjectType = "RESOURCE_POOL" },
			"a grant object type is not upper-case words, or is a common target"},
		{"a common grant target", func(d *protocol.Description) { d.Engines[0].ScopeSelections[0].GrantObjectType = "MATERIALIZED VIEW" },
			"a grant object type is not upper-case words, or is a common target"},
		{"a grant object type of two selections", func(d *protocol.Description) { d.Engines[0].ScopeSelections[1].GrantObjectType = "TOPIC" },
			"a grant object type names two selections"},
		{"a kept requirement of no selection", func(d *protocol.Description) {
			d.Engines[0].ScopeSelections[2].KeepsRequired = []string{"example.org/fixture/other"}
		}, "a kept requirement names no other selection"},
		{"a kept requirement of itself", func(d *protocol.Description) {
			d.Engines[0].ScopeSelections[2].KeepsRequired = []string{classifierKind}
		}, "a kept requirement names no other selection"},
		{"a kept requirement twice", func(d *protocol.Description) {
			d.Engines[0].ScopeSelections[2].KeepsRequired = []string{poolKind, poolKind}
		}, "a kept requirement names no other selection, or names one twice"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := selectionDescription()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*scope selections: .*"+regexp.QuoteMeta(test.want)+".*")
		})
	}
}

// A host that does not list the feature receives no selection, and a
// description that is still valid; a host that lists it receives every one.
func TestScopeSelectionsAreTwoWay(t *testing.T) {
	c := qt.New(t)
	offered := selectionDescription()
	before := proto.CloneOf(offered)
	old := provider.NegotiatedDescription(offered, nil)
	c.Assert(old.Features, qt.DeepEquals, []string{provider.InspectFeature})
	c.Assert(old.Engines[0].Features, qt.HasLen, 0)
	c.Assert(old.Engines[0].ScopeSelections, qt.HasLen, 0)
	c.Assert(provider.ValidateDescription(old, nil), qt.IsNil)
	c.Assert(proto.Equal(offered, before), qt.IsTrue)
	listed := provider.NegotiatedDescription(offered, []string{provider.ScopeSelectionsFeature})
	c.Assert(proto.Equal(listed, offered), qt.IsTrue)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ScopeSelectionsFeature)
	c.Assert(provider.TwoWayFeatures(), qt.Not(qt.Contains), provider.StatementsFeature)
}
