package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// defaultsDescription states offline capability facts for its engine, with
// the feature on the provider and the engine.
func defaultsDescription() *protocol.Description {
	d := description()
	d.Features = append(d.Features, provider.DefaultCapabilitiesFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.DefaultCapabilitiesFeature)
	d.Engines[0].DefaultCapabilities = map[string]bool{"transactional_ddl": false, "returning_clause": true}
	d.Engines[0].ReleaseLines = []*protocol.ReleaseLine{
		{Line: "2.1", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "measured by hand", Capabilities: map[string]bool{"transactional_ddl": false, "returning_clause": true}},
		{Line: "1.4", Support: protocol.ReleaseLine_BEST_EFFORT, Unprobed: "measured by hand", Capabilities: map[string]bool{"transactional_ddl": false, "returning_clause": false}},
	}
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
	c.Assert(listed.Engines[0].ReleaseLines[1].Capabilities, qt.DeepEquals, map[string]bool{"transactional_ddl": false, "returning_clause": false})
	c.Assert(provider.ValidateDescription(listed, nil), qt.IsNil)

	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(unlisted.Engines[0].DefaultCapabilities, qt.IsNil)
	c.Assert(unlisted.Engines[0].ReleaseLines[0].Capabilities, qt.IsNil)
	c.Assert(unlisted.Engines[0].ReleaseLines[1].Capabilities, qt.IsNil)
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
	lineWithoutFacts := defaultsDescription()
	lineWithoutFacts.Engines[0].ReleaseLines[1].Capabilities = nil
	lineFactsWithoutFeature := description()
	lineFactsWithoutFeature.Engines[0].ReleaseLines = defaultsDescription().Engines[0].ReleaseLines
	badLineName := defaultsDescription()
	badLineName.Engines[0].ReleaseLines[0].Capabilities = map[string]bool{"": true}
	for _, test := range []struct {
		name        string
		description *protocol.Description
		want        string
	}{
		{"the engine's feature without the provider's", withoutProvider, "engine engine.default-capabilities requires the provider's"},
		{"the feature without facts", withoutFacts, "engine engine.default-capabilities states no capability"},
		{"facts without the feature", withoutFeature, "default capabilities require engine engine.default-capabilities"},
		{"a name a session could not state", badName, "invalid default capability name"},
		{"a release line without facts", lineWithoutFacts, "release line 1.4 states no capability"},
		{"release line facts without the feature", lineFactsWithoutFeature, "release line 2.1 capabilities require engine engine.default-capabilities"},
		{"a release line name a session could not state", badLineName, "invalid capability name on release line 2.1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.ValidateDescription(test.description, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, errorEndsWith(test.want))
		})
	}
}
