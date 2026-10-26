package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func TestLifecyclePlacementRequiresNegotiatedGraphSupport(t *testing.T) {
	c := qt.New(t)
	d, request, result := planningExchange()
	result.Contributions[0].Steps[0].Operation.Phase = protocol.PlanningOperation_LIFECYCLE
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorIs, provider.ErrProtocol)
	d.Features = append(d.Features, provider.PlanningLifecycleFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.PlanningLifecycleFeature)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(provider.ValidatePlannedFeatures(unlisted, request, result), qt.ErrorIs, provider.ErrProtocol)
	result.Contributions[0].Steps[0].Operation.Phase = protocol.PlanningOperation_DEPENDENT
	c.Assert(provider.ValidatePlannedFeatures(unlisted, request, result), qt.IsNil)
	result.Contributions[0].Steps[0].Operation.Phase = 99
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorIs, provider.ErrProtocol)
}

func TestLifecycleNegotiationRequiresAnAvailablePlanner(t *testing.T) {
	c := qt.New(t)
	d, _, _ := declarationsExchange()
	d.Features = append(d.Features, provider.PlanningLifecycleFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.PlanningLifecycleFeature)
	d.Engines[0].Planning = nil
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	negotiated := provider.NegotiatedDescription(d, []string{provider.PlanningLifecycleFeature})
	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.PlanningLifecycleFeature)
}
