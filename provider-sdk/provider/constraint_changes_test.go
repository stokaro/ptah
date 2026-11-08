package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// constraintChangesFeatures are the features an engine lists beside
// constraint changes, the feature included.
var constraintChangesFeatures = []string{provider.ObservedColumnsFeature, provider.ObservedConstraintsFeature, provider.ConstraintChangesFeature}

// constraintChangesDescription is a provider whose engine owns the lifecycle
// of its tables through a planning service with parent kinds, reports the
// constraints its tables hold and plans their changes.
func constraintChangesDescription() *protocol.Description {
	d, _, _ := planningExchange()
	d.Features = append(d.Features, constraintChangesFeatures...)
	d.Engines[0].Features = append(d.Engines[0].Features, constraintChangesFeatures...)
	return d
}

func TestConstraintChangesFeature_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("an engine that owns its tables plans their constraint changes", func(t *testing.T) {
		c := qt.New(t)
		d := constraintChangesDescription()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ConstraintChangesFeature)
		negotiated := provider.NegotiatedDescription(d, constraintChangesFeatures)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.ConstraintChangesFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
	// A host that does not list the feature leaves it out of the engine, and
	// the engine keeps the constraints it reports.
	t.Run("a host that does not list the feature negotiates it away", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(constraintChangesDescription(), []string{provider.ObservedColumnsFeature, provider.ObservedConstraintsFeature})
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ConstraintChangesFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.ObservedConstraintsFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
}

// An engine lists the feature beside the provider's, beside observed
// constraints, through which the host compares the constraints, and with a
// planning service that owns its tables; an engine whose table operations
// the host plans has no owner to hand the change to.
func TestConstraintChangesFeature_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"without the provider's feature", func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.ConstraintChangesFeature)
		}},
		{"without observed constraints", func(d *protocol.Description) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.ObservedConstraintsFeature)
		}},
		{"without a planning service that owns tables", func(d *protocol.Description) { d.Engines[0].Planning[0].ParentKinds = nil }},
		{"with common table operations", func(d *protocol.Description) {
			d.Engines[0].Features = append(d.Engines[0].Features, provider.CommonTableIdentityFeature)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := constraintChangesDescription()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*constraint changes: the engine requires the provider's feature, observed constraints and a planning service that owns its tables`)
		})
	}
}
