package provider_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
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
}

func TestModelPredecessorsDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*protocol.Description)
	}{
		{"predecessors without the feature", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.ModelPredecessorsFeature })
		}},
		{"the feature without the provider's", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.ModelPredecessorsFeature })
		}},
		{"the feature without a predecessor", func(d *protocol.Description) { d.Engines[0].ModelPredecessors = nil }},
		{"a model the engine does not declare", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].Kind = "example.org/fixture/unknown"
		}},
		{"an operation model", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].Representation = protocol.ValueInput_OPERATION
		}},
		{"an observed model the engine does not declare", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].Representation = protocol.ValueInput_OBSERVED
		}},
		{"no former owner", func(d *protocol.Description) { d.Engines[0].ModelPredecessors[0].FormerOwner = "" }},
		{"a former kind with a space", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].FormerKind = "example.org/builtin/a retention"
		}},
		{"no schema version", func(d *protocol.Description) { d.Engines[0].ModelPredecessors[0].SchemaVersion = 0 }},
		{"a digest with its prefix", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].DefinitionSha256 = "sha256:" + strings.Repeat("ab", 32)
		}},
		{"an uppercase digest", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].DefinitionSha256 = strings.Repeat("AB", 32)
		}},
		{"a short digest", func(d *protocol.Description) { d.Engines[0].ModelPredecessors[0].DefinitionSha256 = "ab" }},
		{"the provider's own model", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors[0].FormerOwner = d.ProviderId
			d.Engines[0].ModelPredecessors[0].FormerKind = d.Engines[0].ValueModels[0].Kind
		}},
		{"a former identity named twice", func(d *protocol.Description) {
			d.Engines[0].ModelPredecessors = append(d.Engines[0].ModelPredecessors, proto.CloneOf(d.Engines[0].ModelPredecessors[0]))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := predecessorDescription()
			test.change(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*model predecessors: .*`)
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
