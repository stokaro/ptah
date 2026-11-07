package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// seedLedgerInspection is a read negotiated with a reserved seed ledger,
// which it reports beside the migration history.
func seedLedgerInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := inspection()
	d.Features = append(d.Features, provider.SeedsFeature, provider.ReservedSeedsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.SeedsFeature, provider.ReservedSeedsFeature)
	ledger := reservedTable()
	ledger.Object.Name, ledger.NormalizedObject.Name = "schema_seeds", "schema_seeds"
	ledger.Purpose = protocol.ReservedObject_SEEDS
	snapshot.ReservedObjects = []*protocol.ReservedObject{reservedTable(), ledger}
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.SeedsFeature, provider.ReservedSeedsFeature)
	return d, request, snapshot
}

func TestReservedSeeds_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a negotiated read reserves the seed ledger", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := seedLedgerInspection()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a host that does not list the feature negotiates it away", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := seedLedgerInspection()
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.ReservedSeedsFeature))
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ReservedSeedsFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.SeedsFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ReservedSeedsFeature)
	})
}

func TestReservedSeeds_FailurePath(t *testing.T) {
	t.Parallel()
	t.Run("a ledger the host did not negotiate", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := seedLedgerInspection()
		snapshot.TargetFeatures = withoutFeature(snapshot.TargetFeatures, provider.ReservedSeedsFeature)
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(err, qt.ErrorMatches, `.*a reserved seed ledger needs schema\.reserved-seeds`)
	})
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"without the provider's feature", func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.ReservedSeedsFeature)
		}},
		{"without the seed service", func(d *protocol.Description) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.SeedsFeature)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := seedLedgerInspection()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*a reserved seed ledger requires the provider's feature and the engine's seed service`)
		})
	}
}
