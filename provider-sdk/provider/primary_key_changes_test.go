package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// keyChangesFeatures are the features an engine lists beside primary key
// changes, the feature included.
var keyChangesFeatures = []string{provider.ObservedColumnsFeature, provider.ObservedPrimaryKeysFeature, provider.PrimaryKeyChangesFeature}

// keyChangesDescription is a provider whose engine owns the lifecycle of its
// tables through a planning service with parent kinds, reports ordered
// primary keys and plans their changes.
func keyChangesDescription() *protocol.Description {
	d, _, _ := planningExchange()
	d.Features = append(d.Features, keyChangesFeatures...)
	d.Engines[0].Features = append(d.Engines[0].Features, keyChangesFeatures...)
	return d
}

func TestPrimaryKeyChangesFeature_HappyPath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d := keyChangesDescription()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.PrimaryKeyChangesFeature)
	negotiated := provider.NegotiatedDescription(d, keyChangesFeatures)
	c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.PrimaryKeyChangesFeature)
	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
}

// A host that does not list the feature leaves it out of the engine, and the
// engine keeps the ordered keys it reports.
func TestPrimaryKeyChangesWithoutTheHost_HappyPath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	negotiated := provider.NegotiatedDescription(keyChangesDescription(), []string{provider.ObservedColumnsFeature, provider.ObservedPrimaryKeysFeature})
	c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.PrimaryKeyChangesFeature)
	c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.ObservedPrimaryKeysFeature)
	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
}

// An engine lists the feature beside the provider's, beside observed primary
// keys, through which the host compares the keys, and with a planning service
// that owns its tables; an engine whose table operations the host plans has
// no owner to hand the change to.
func TestPrimaryKeyChangesFeatureOfAnEngine_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"without the provider's feature", func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.PrimaryKeyChangesFeature)
		}},
		{"without observed primary keys", func(d *protocol.Description) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.ObservedPrimaryKeysFeature)
		}},
		{"without a planning service that owns tables", func(d *protocol.Description) { d.Engines[0].Planning[0].ParentKinds = nil }},
		{"with common table operations", func(d *protocol.Description) {
			d.Engines[0].Features = append(d.Engines[0].Features, provider.CommonTableIdentityFeature)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			d := keyChangesDescription()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*primary key changes: the engine requires the provider's feature, observed primary keys and a planning service that owns its tables`)
		})
	}
}
