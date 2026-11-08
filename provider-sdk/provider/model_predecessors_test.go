package provider_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// predecessorDescription is a valid description whose engine names one
// predecessor of its desired model: the identity a former built-in owner
// captured the same model's claims under.
func predecessorDescription() *protocol.Description {
	d, _ := valueInputs()
	engine := d.Engines[0]
	d.Features = append(d.Features, provider.ModelPredecessorsFeature)
	engine.Features = append(engine.Features, provider.ModelPredecessorsFeature)
	engine.ModelPredecessors = []*protocol.ModelPredecessor{{Kind: engine.ValueModels[0].Kind, Representation: protocol.ValueInput_DESIRED,
		FormerOwner: "example.org/builtin", FormerKind: "example.org/builtin/retention", SchemaVersion: 1, DefinitionSha256: strings.Repeat("ab", 32)}}
	return d
}

// ownPredecessorDescription is predecessorDescription whose engine also names
// an earlier identity of its own desired model, under models.own-predecessors:
// the provider as the owner, the model's kind, and a digest the model's
// definition no longer has.
func ownPredecessorDescription() *protocol.Description {
	d := predecessorDescription()
	engine := d.Engines[0]
	d.Features = append(d.Features, provider.OwnPredecessorsFeature)
	engine.Features = append(engine.Features, provider.OwnPredecessorsFeature)
	engine.ModelPredecessors = append(engine.ModelPredecessors, &protocol.ModelPredecessor{Kind: engine.ValueModels[0].Kind,
		Representation: protocol.ValueInput_DESIRED, FormerOwner: d.ProviderId, FormerKind: engine.ValueModels[0].Kind, SchemaVersion: 1,
		DefinitionSha256: strings.Repeat("cd", 32)})
	return d
}

func TestModelPredecessorsDescription_HappyPath(t *testing.T) {
	t.Run("one predecessor", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(provider.ValidateDescription(predecessorDescription(), nil), qt.IsNil)
	})
	t.Run("two former versions of one model", func(t *testing.T) {
		c := qt.New(t)
		d := predecessorDescription()
		earlier := proto.CloneOf(d.Engines[0].ModelPredecessors[0])
		earlier.SchemaVersion, earlier.DefinitionSha256 = 2, strings.Repeat("cd", 32)
		d.Engines[0].ModelPredecessors = append(d.Engines[0].ModelPredecessors, earlier)
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	})
	t.Run("an earlier version under the provider's own owner", func(t *testing.T) {
		c := qt.New(t)
		d := predecessorDescription()
		d.Engines[0].ModelPredecessors[0].FormerOwner = d.ProviderId
		d.Engines[0].ModelPredecessors[0].FormerKind = "example.org/fixture/old-retention"
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	})
	t.Run("an earlier definition of the model itself", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(provider.ValidateDescription(ownPredecessorDescription(), nil), qt.IsNil)
	})
	t.Run("an earlier schema version of the model itself", func(t *testing.T) {
		c := qt.New(t)
		d := ownPredecessorDescription()
		d.Engines[0].ModelPredecessors[1].SchemaVersion = 2
		d.Engines[0].ModelPredecessors[1].DefinitionSha256 = d.Engines[0].ValueModels[0].DefinitionSha256
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	})
	t.Run("the own feature with no predecessor of the model itself", func(t *testing.T) {
		c := qt.New(t)
		d := ownPredecessorDescription()
		d.Engines[0].ModelPredecessors = d.Engines[0].ModelPredecessors[:1]
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	})
}

func TestModelPredecessorsDescription_FailurePath(t *testing.T) {
	const (
		unlisted   = `an engine declares model predecessors exactly when it lists models\.predecessors`
		undeclared = `a model predecessor names a desired or observed model the engine does not declare`
		incomplete = `a model predecessor needs an owner, a kind, a schema version and a lowercase SHA-256 definition digest`
	)
	for _, test := range []struct {
		name   string
		change func(*protocol.Description)
		reason string
	}{
		{"predecessors without the feature", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.ModelPredecessorsFeature })
		}, unlisted},
		{"the feature without the provider's", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.ModelPredecessorsFeature })
		}, `an engine's model predecessors need the provider's models\.predecessors`},
		{"the feature without a predecessor", func(d *protocol.Description) { d.Engines[0].ModelPredecessors = nil }, unlisted},
		{"a model the engine does not declare", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].Kind = "example.org/fixture/unknown"
		}, undeclared},
		{"an operation model", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].Representation = protocol.ValueInput_OPERATION
		}, undeclared},
		{"an observed model the engine does not declare", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].Representation = protocol.ValueInput_OBSERVED
		}, undeclared},
		{"no former owner", func(d *protocol.Description) { d.Engines[0].ModelPredecessors[0].FormerOwner = "" }, incomplete},
		{"a former kind with a space", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].FormerKind = "example.org/builtin/a retention"
		}, incomplete},
		{"no schema version", func(d *protocol.Description) { d.Engines[0].ModelPredecessors[0].SchemaVersion = 0 }, incomplete},
		{"a digest with its prefix", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].DefinitionSha256 = "sha256:" + strings.Repeat("ab", 32)
		}, incomplete},
		{"an uppercase digest", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].DefinitionSha256 = strings.Repeat("AB", 32)
		}, incomplete},
		{"a short digest", func(d *protocol.Description) { d.Engines[0].ModelPredecessors[0].DefinitionSha256 = "ab" }, incomplete},
		{"an earlier identity of the model itself without the own feature", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].FormerOwner = d.ProviderId
			d.Engines[0].ModelPredecessors[0].FormerKind = d.Engines[0].ValueModels[0].Kind
		}, `an earlier identity of the model itself needs models\.own-predecessors`},
		{"the model's current identity", func(d *protocol.Description) {
			d.Features = append(d.Features, provider.OwnPredecessorsFeature)
			d.Engines[0].Features = append(d.Engines[0].Features, provider.OwnPredecessorsFeature)
			d.Engines[0].ModelPredecessors[0].FormerOwner = d.ProviderId
			d.Engines[0].ModelPredecessors[0].FormerKind = d.Engines[0].ValueModels[0].Kind
			d.Engines[0].ModelPredecessors[0].DefinitionSha256 = d.Engines[0].ValueModels[0].DefinitionSha256
		}, `a model predecessor is the model's current identity`},
		{"another model the provider declares", func(d *protocol.Description) {
			d.Features = append(d.Features, provider.OwnPredecessorsFeature)
			d.Engines[0].Features = append(d.Engines[0].Features, provider.OwnPredecessorsFeature)
			other := proto.CloneOf(d.Engines[0].ValueModels[0])
			other.Kind = "example.org/fixture/expiry"
			d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, other)
			d.Engines[0].ModelPredecessors[0].FormerOwner = d.ProviderId
			d.Engines[0].ModelPredecessors[0].FormerKind = other.Kind
		}, `a model predecessor is another model the provider declares`},
		{"the engine's own feature without the provider's", func(d *protocol.Description) {
			d.Engines[0].Features = append(d.Engines[0].Features, provider.OwnPredecessorsFeature)
		}, `an engine's own model predecessors need the provider's models\.own-predecessors`},
		{"a former identity named twice", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors = append(d.Engines[0].ModelPredecessors, proto.CloneOf(d.Engines[0].ModelPredecessors[0]))
		}, `a former model identity is named twice`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := predecessorDescription()
			test.change(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*model predecessors: `+test.reason)
		})
	}
}

// A host that does not list the feature receives no predecessors and no
// feature; one that lists it receives both.
func TestModelPredecessorsNegotiation(t *testing.T) {
	c := qt.New(t)
	d := predecessorDescription()
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(unlisted.GetEngines()[0].GetModelPredecessors(), qt.HasLen, 0)
	c.Assert(unlisted.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.ModelPredecessorsFeature)
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
	listed := provider.NegotiatedDescription(d, []string{provider.ModelPredecessorsFeature})
	c.Assert(proto.Equal(listed, d), qt.IsTrue)
}

// A host that lists models.predecessors and not models.own-predecessors
// receives the predecessors of other owners and none of the model itself, and
// an engine left with no predecessor no longer lists models.predecessors; one
// that lists both receives every predecessor. A host of an earlier contract,
// whose SDK refuses a predecessor of the model itself, never receives one.
func TestOwnPredecessorsNegotiation(t *testing.T) {
	c := qt.New(t)
	d := ownPredecessorDescription()
	former := provider.NegotiatedDescription(d, []string{provider.ModelPredecessorsFeature})
	c.Assert(former.GetEngines()[0].GetModelPredecessors(), qt.HasLen, 1)
	c.Assert(proto.Equal(former.GetEngines()[0].GetModelPredecessors()[0], d.Engines[0].ModelPredecessors[0]), qt.IsTrue)
	c.Assert(former.GetEngines()[0].GetFeatures(), qt.Contains, provider.ModelPredecessorsFeature)
	c.Assert(former.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.OwnPredecessorsFeature)
	c.Assert(former.GetFeatures(), qt.Not(qt.Contains), provider.OwnPredecessorsFeature)
	c.Assert(provider.ValidateDescription(former, nil), qt.IsNil)

	own := ownPredecessorDescription()
	own.Engines[0].ModelPredecessors = own.Engines[0].ModelPredecessors[1:]
	alone := provider.NegotiatedDescription(own, []string{provider.ModelPredecessorsFeature})
	c.Assert(alone.GetEngines()[0].GetModelPredecessors(), qt.HasLen, 0)
	c.Assert(alone.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.ModelPredecessorsFeature)
	c.Assert(provider.ValidateDescription(alone, nil), qt.IsNil)

	both := provider.NegotiatedDescription(d, []string{provider.ModelPredecessorsFeature, provider.OwnPredecessorsFeature})
	c.Assert(proto.Equal(both, d), qt.IsTrue)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.OwnPredecessorsFeature)
}
