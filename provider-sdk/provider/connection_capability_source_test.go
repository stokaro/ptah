package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// monitoringSource is the line the YDB provider states for its connection.
const monitoringSource = "read from this cluster's feature flags at its monitoring endpoint, not from its release line"

// sourceDescription is a description whose engine states source, with the
// feature listed by the provider and the engine.
func sourceDescription(source string) *protocol.Description {
	description := dataDescription()
	description.Features = append(description.Features, provider.ConnectionCapabilitySourceFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.ConnectionCapabilitySourceFeature)
	description.Engines[0].ConnectionCapabilitySource = source
	return description
}

func TestConnectionCapabilitySource_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("an engine that states where its connection reads capabilities", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(provider.ValidateDescription(sourceDescription(monitoringSource), nil), qt.IsNil)
	})
	t.Run("a host that lists the feature receives the line", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(sourceDescription(monitoringSource), provider.TwoWayFeatures())
		c.Assert(negotiated.Engines[0].GetConnectionCapabilitySource(), qt.Equals, monitoringSource)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.ConnectionCapabilitySourceFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
	// A host of contract 25.34 prints its own sentence for a provider's
	// connection and receives neither the line nor the feature.
	t.Run("a host that does not list the feature receives no line", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(sourceDescription(monitoringSource),
			withoutFeature(provider.TwoWayFeatures(), provider.ConnectionCapabilitySourceFeature))
		c.Assert(negotiated.Engines[0].GetConnectionCapabilitySource(), qt.Equals, "")
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ConnectionCapabilitySourceFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ConnectionCapabilitySourceFeature)
	})
}

func TestConnectionCapabilitySource_FailurePath(t *testing.T) {
	t.Parallel()
	t.Run("the engine's feature without the provider's", func(t *testing.T) {
		c := qt.New(t)
		description := sourceDescription(monitoringSource)
		description.Features = withoutFeature(description.Features, provider.ConnectionCapabilitySourceFeature)
		c.Assert(provider.ValidateDescription(description, nil), qt.ErrorMatches, `.*engine engine\.connection-capability-source requires the provider's`)
	})
	t.Run("the feature without a line", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(provider.ValidateDescription(sourceDescription(""), nil), qt.ErrorMatches,
			`.*engine rows states a connection capability source exactly when it lists engine\.connection-capability-source`)
	})
	t.Run("a line without the feature", func(t *testing.T) {
		c := qt.New(t)
		description := sourceDescription(monitoringSource)
		description.Engines[0].Features = withoutFeature(description.Engines[0].Features, provider.ConnectionCapabilitySourceFeature)
		c.Assert(provider.ValidateDescription(description, nil), qt.ErrorMatches,
			`.*engine rows states a connection capability source exactly when it lists engine\.connection-capability-source`)
	})
	for _, test := range []struct{ name, source string }{
		{name: "a line on two lines", source: "read from the cluster\nat its monitoring endpoint"},
		{name: "a line longer than a host prints", source: strings.Repeat("x", 513)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDescription(sourceDescription(test.source), nil), qt.ErrorMatches,
				`.*the connection capability source of engine rows is not one line of text`)
		})
	}
}
