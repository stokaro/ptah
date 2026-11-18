package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// replayTrustDescription declares the replay guard and trust in replayed SQL
// on the provider and its engine.
func replayTrustDescription() *protocol.Description {
	description := devCleanupDescription()
	description.Features = append(description.Features, provider.ReplayTrustFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.ReplayTrustFeature)
	return description
}

func TestReplayTrustDescription_HappyPath(t *testing.T) {
	c := qt.New(t)
	description := replayTrustDescription()

	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ReplayTrustFeature)
	// A host that does not list the feature negotiates it away from the
	// provider and its engine, and both sides then refuse a trusted request.
	negotiated := provider.NegotiatedDescription(description, nil)
	c.Assert(negotiated.GetFeatures(), qt.Not(qt.Contains), provider.ReplayTrustFeature)
	c.Assert(negotiated.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.ReplayTrustFeature)
	kept := provider.NegotiatedDescription(description, []string{provider.ReplayTrustFeature})
	c.Assert(provider.EngineOffers(kept, devEngineID(), provider.ReplayTrustFeature), qt.IsTrue)
}

// Trust lifts the refusals of a replay guard, so a provider or an engine that
// has no guard cannot declare it, and an engine cannot declare it without its
// provider.
func TestReplayTrustDescription_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description)
		want   string
	}{
		{"engine trust without the provider's", func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.ReplayTrustFeature)
		}, `.*engine database\.dev-replay-trust requires the provider's`},
		{"provider trust without its replay guard", func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.ReplayGuardFeature)
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.ReplayGuardFeature)
		}, `.*database\.dev-replay-trust requires the provider's replay guard`},
		{"engine trust without the engine's replay guard", func(d *protocol.Description) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.ReplayGuardFeature)
		}, `.*engine database\.dev-replay-trust requires the engine's replay guard`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description := replayTrustDescription()
			test.mutate(description)
			err := provider.ValidateDescription(description, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

func trustedGuardRequest(trusted bool) *protocol.GuardReplayRequest {
	return &protocol.GuardReplayRequest{Engine: devEngineID(), Scope: &protocol.ReplayScope{Database: "dev", TrustedSql: trusted}, Statements: []string{"CREATE TABLE t (id INTEGER)"}}
}

// A negotiated trust is accepted in both requests, and a request that carries
// no trust is accepted whether or not trust was negotiated, which keeps an
// older provider working.
func TestValidateReplayTrustRequests_HappyPath(t *testing.T) {
	c := qt.New(t)
	negotiated := provider.NegotiatedDescription(replayTrustDescription(), []string{provider.ReplayTrustFeature})
	older := devCleanupDescription()

	c.Assert(provider.ValidateGuardReplayRequest(negotiated, trustedGuardRequest(true)), qt.IsNil)
	c.Assert(provider.ValidateOpenRequest(negotiated, &protocol.OpenRequest{Engine: devEngineID(), Address: "test://dev", TrustedSql: true}), qt.IsNil)
	c.Assert(provider.ValidateGuardReplayRequest(older, trustedGuardRequest(false)), qt.IsNil)
	c.Assert(provider.ValidateOpenRequest(older, &protocol.OpenRequest{Engine: devEngineID(), Address: "test://dev"}), qt.IsNil)
}

// Trust that the provider, its engine or the host did not negotiate is
// refused in both requests, rather than dropped.
func TestValidateReplayTrustRequests_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		description *protocol.Description
	}{
		{"an older provider", devCleanupDescription()},
		{"a host that does not list the feature", provider.NegotiatedDescription(replayTrustDescription(), nil)},
		{"an engine that does not declare it", func() *protocol.Description {
			description := replayTrustDescription()
			description.Engines[0].Features = withoutFeature(description.Engines[0].Features, provider.ReplayTrustFeature)
			return description
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			guardErr := provider.ValidateGuardReplayRequest(test.description, trustedGuardRequest(true))
			openErr := provider.ValidateOpenRequest(test.description, &protocol.OpenRequest{Engine: devEngineID(), Address: "test://dev", TrustedSql: true})
			c.Assert(guardErr, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(guardErr, qt.ErrorMatches, `.*trusted replay SQL requires database\.dev-replay-trust for engine .*`)
			c.Assert(openErr, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(openErr, qt.ErrorMatches, `.*a session for trusted dev SQL requires database\.dev-replay-trust for engine .*`)
		})
	}
	c := qt.New(t)
	c.Assert(provider.ValidateOpenRequest(replayTrustDescription(), nil), qt.ErrorIs, provider.ErrProtocol)
}
