package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// repeatableRecordInspection is a read negotiated with a reserved record of
// repeatable migrations, which it reports beside the revision table.
func repeatableRecordInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := inspection()
	features := []string{provider.HistoryFeature, provider.RepeatableRecordFeature, provider.ReservedRepeatableRecordFeature}
	d.Features = append(d.Features, features...)
	d.Engines[0].Features = append(d.Engines[0].Features, features...)
	record := reservedTable()
	record.Object.Name, record.NormalizedObject.Name = "history_repeatable", "history_repeatable"
	record.Purpose = protocol.ReservedObject_REPEATABLE
	snapshot.ReservedObjects = []*protocol.ReservedObject{reservedTable(), record}
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.ReservedRepeatableRecordFeature)
	return d, request, snapshot
}

func TestReservedRepeatableRecord_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a negotiated read reserves the record", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := repeatableRecordInspection()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a host that does not list the feature negotiates it away", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := repeatableRecordInspection()
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.ReservedRepeatableRecordFeature))
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ReservedRepeatableRecordFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.RepeatableRecordFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ReservedRepeatableRecordFeature)
	})
	t.Run("a host that lists the reservation without the record negotiates both away", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := repeatableRecordInspection()
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.RepeatableRecordFeature))
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.RepeatableRecordFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ReservedRepeatableRecordFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
}

func TestReservedRepeatableRecord_FailurePath(t *testing.T) {
	t.Parallel()
	t.Run("a record the read did not negotiate", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := repeatableRecordInspection()
		snapshot.TargetFeatures = withoutFeature(snapshot.TargetFeatures, provider.ReservedRepeatableRecordFeature)
		values, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(err, qt.ErrorMatches, `.*a reserved repeatable migration record needs schema\.reserved-repeatable-record`)
		c.Assert(values, qt.IsNil)
	})
	t.Run("a record reserved for a host that did not list the feature", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := repeatableRecordInspection()
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.ReservedRepeatableRecordFeature))
		snapshot.TargetFeatures = withoutFeature(snapshot.TargetFeatures, provider.ReservedRepeatableRecordFeature)
		values, err := provider.InspectionValues(negotiated, request, snapshot)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(err, qt.ErrorMatches, `.*a reserved repeatable migration record needs schema\.reserved-repeatable-record`)
		c.Assert(values, qt.IsNil)
	})
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"without the provider's feature", func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.ReservedRepeatableRecordFeature)
		}},
		{"without the engine's record", func(d *protocol.Description) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.RepeatableRecordFeature)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := repeatableRecordInspection()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*a reserved repeatable migration record requires the provider's feature and the engine's record`)
		})
	}
}
