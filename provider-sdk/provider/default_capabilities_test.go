package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// defaultsDescription states offline capability facts for its engine, with
// the feature on the provider and the engine.
func defaultsDescription() *protocol.Description {
	d := description()
	d.Features = append(d.Features, provider.DefaultCapabilitiesFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.DefaultCapabilitiesFeature)
	d.Engines[0].DefaultCapabilities = map[string]bool{"transactional_ddl": false, "returning_clause": true}
	return d
}

// An engine that lists the feature states its facts, and a host that lists
// the feature receives them; negotiation removes them, with the feature, for
// a host that does not.
func TestDefaultCapabilities_HappyPath(t *testing.T) {
	c := qt.New(t)
	d := defaultsDescription()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)

	listed := provider.NegotiatedDescription(d, []string{provider.DefaultCapabilitiesFeature})
	c.Assert(listed.Engines[0].DefaultCapabilities, qt.DeepEquals, map[string]bool{"transactional_ddl": false, "returning_clause": true})
	c.Assert(provider.ValidateDescription(listed, nil), qt.IsNil)

	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(unlisted.Engines[0].DefaultCapabilities, qt.IsNil)
	c.Assert(unlisted.Engines[0].Features, qt.Not(qt.Contains), provider.DefaultCapabilitiesFeature)
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
}

func TestDefaultCapabilities_FailurePath(t *testing.T) {
	withoutProvider := defaultsDescription()
	withoutProvider.Features = description().Features
	withoutFacts := defaultsDescription()
	withoutFacts.Engines[0].DefaultCapabilities = nil
	withoutFeature := defaultsDescription()
	withoutFeature.Engines[0].Features = description().Engines[0].Features
	badName := defaultsDescription()
	badName.Engines[0].DefaultCapabilities = map[string]bool{" padded": true}
	for _, test := range []struct {
		name        string
		description *protocol.Description
		want        string
	}{
		{"the engine's feature without the provider's", withoutProvider, "engine engine.default-capabilities requires the provider's"},
		{"the feature without facts", withoutFacts, "engine engine.default-capabilities states no capability"},
		{"facts without the feature", withoutFeature, "default capabilities require engine engine.default-capabilities"},
		{"a name a session could not state", badName, "invalid default capability name"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.ValidateDescription(test.description, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, errorEndsWith(test.want))
		})
	}
}
