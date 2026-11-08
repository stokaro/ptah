package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// notDescribedInspection is a read negotiated with not-described records of
// an engine that declares change streams and publications: every change
// stream, whatever the database holds, and one publication by name.
func notDescribedInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := inspection()
	d.Features = append(d.Features, provider.NotDescribedFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.NotDescribedFeature)
	d.Engines[0].NotDescribedKinds = []string{"change_stream", "publication"}
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.NotDescribedFeature)
	snapshot.NotDescribed = []*protocol.SchemaCoverageLimit{
		{Kind: "change_stream", Reason: "unsupported", Provenance: "derived-from-target"},
		{Kind: "publication", Name: "Orders", Reason: "not-inspected", Provenance: "observed"},
	}
	return d, request, snapshot
}

func TestNotDescribed_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a negotiated read records what it does not describe and the records survive the wire", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := notDescribedInspection()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		before := proto.CloneOf(snapshot)
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
		c.Assert(proto.Equal(snapshot, before), qt.IsTrue)
		data, err := proto.Marshal(snapshot)
		c.Assert(err, qt.IsNil)
		decoded := &protocol.Schema{}
		c.Assert(proto.Unmarshal(data, decoded), qt.IsNil)
		_, err = provider.InspectionValues(d, request, decoded)
		c.Assert(err, qt.IsNil)
		c.Assert(proto.Equal(decoded.NotDescribed[1], before.NotDescribed[1]), qt.IsTrue)
	})
	t.Run("a negotiated read with nothing to record", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := notDescribedInspection()
		snapshot.NotDescribed = nil
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a whole kind and one object of it", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := notDescribedInspection()
		snapshot.NotDescribed = append(snapshot.NotDescribed, &protocol.SchemaCoverageLimit{Kind: "change_stream", Name: "cs1", Reason: "unsupported", Provenance: "observed"})
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a host that does not list the feature negotiates it and the kinds away", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := notDescribedInspection()
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.NotDescribedFeature))
		c.Assert(negotiated.Features, qt.Not(qt.Contains), provider.NotDescribedFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.NotDescribedFeature)
		c.Assert(negotiated.Engines[0].NotDescribedKinds, qt.IsNil)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.NotDescribedFeature)
		c.Assert(d.Engines[0].NotDescribedKinds, qt.DeepEquals, []string{"change_stream", "publication"})
	})
	t.Run("a host that lists the feature keeps the kinds", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := notDescribedInspection()
		negotiated := provider.NegotiatedDescription(d, provider.TwoWayFeatures())
		c.Assert(negotiated.Engines[0].NotDescribedKinds, qt.DeepEquals, []string{"change_stream", "publication"})
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
}

// An engine lists the feature only beside the provider's and with at least
// one kind, declares kinds only with the feature, and spells each kind once as
// a lowercase token.
func TestNotDescribedDescription_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description)
	}{
		{"without the provider's feature", `.*not-described records: the engine's feature requires the provider's`, func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.NotDescribedFeature)
		}},
		{"without a kind", `.*not-described records: engine fixture lists schema\.not-described and declares no kind`, func(d *protocol.Description) {
			d.Engines[0].NotDescribedKinds = nil
		}},
		{"kinds without the engine's feature", `.*not-described records: engine fixture declares kinds without listing schema\.not-described`, func(d *protocol.Description) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.NotDescribedFeature)
		}},
		{"a kind that is not a token", `.*not-described records: engine fixture declares a kind "Change Stream" that is not a lowercase token`, func(d *protocol.Description) {
			d.Engines[0].NotDescribedKinds = []string{"Change Stream"}
		}},
		{"an empty kind", `.*not-described records: engine fixture declares a kind "" that is not a lowercase token`, func(d *protocol.Description) {
			d.Engines[0].NotDescribedKinds = []string{"change_stream", ""}
		}},
		{"a kind declared twice", `.*not-described records: engine fixture declares kind "change_stream" twice`, func(d *protocol.Description) {
			d.Engines[0].NotDescribedKinds = []string{"change_stream", "publication", "change_stream"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := notDescribedInspection()
			test.mutate(d)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, test.want)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A read records what it does not describe only where both sides listed the
// feature, and each record names a kind the engine declares, an object by a
// name a host can show or none, a reason and a provenance a read gives; no
// kind and name are recorded twice.
func TestNotDescribedInspection_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.Schema)
	}{
		{"the provider did not list the feature", `.*not-described records: the read's records were not negotiated`, func(d *protocol.Description, _ *protocol.Schema) {
			d.Features = withoutFeature(d.Features, provider.NotDescribedFeature)
		}},
		{"the host did not list the feature", `.*not-described records: the read's records were not negotiated`, func(_ *protocol.Description, s *protocol.Schema) {
			s.TargetFeatures = withoutFeature(s.TargetFeatures, provider.NotDescribedFeature)
		}},
		{"a kind the engine does not declare", `.*not-described records: kind "sequence" is not one engine fixture declares`, func(_ *protocol.Description, s *protocol.Schema) {
			s.NotDescribed[0].Kind = "sequence"
		}},
		{"no kind", `.*not-described records: kind "" is not one engine fixture declares`, func(_ *protocol.Description, s *protocol.Schema) {
			s.NotDescribed[0].Kind = ""
		}},
		{"a blank name", `.*not-described records: a publication record has a blank or malformed name`, func(_ *protocol.Description, s *protocol.Schema) {
			s.NotDescribed[1].Name = " \t"
		}},
		{"a name with a control character", `.*not-described records: a publication record has a blank or malformed name`, func(_ *protocol.Description, s *protocol.Schema) {
			s.NotDescribed[1].Name = "Ord\ners"
		}},
		{"a name that is not UTF-8", `.*not-described records: a publication record has a blank or malformed name`, func(_ *protocol.Description, s *protocol.Schema) {
			s.NotDescribed[1].Name = "Orders\xff"
		}},
		{"no reason", `.*not-described records: a change_stream record gives reason "", which is none of .*`, func(_ *protocol.Description, s *protocol.Schema) {
			s.NotDescribed[0].Reason = ""
		}},
		{"a reason a read does not give", `.*not-described records: a change_stream record gives reason "suppressed", which is none of \[not-inspected outside-scope unsupported unresolved\]`,
			func(_ *protocol.Description, s *protocol.Schema) {
				s.NotDescribed[0].Reason = "suppressed"
			}},
		{"no provenance", `.*not-described records: a change_stream record gives provenance "", which is none of .*`, func(_ *protocol.Description, s *protocol.Schema) {
			s.NotDescribed[0].Provenance = ""
		}},
		{"a provenance a read does not give", `.*not-described records: a change_stream record gives provenance "declared", which is none of \[observed derived-from-target derived-from-fact inferred unavailable\]`,
			func(_ *protocol.Description, s *protocol.Schema) {
				s.NotDescribed[0].Provenance = "declared"
			}},
		{"a whole kind recorded twice", `.*not-described records: change_stream "" is recorded twice`, func(_ *protocol.Description, s *protocol.Schema) {
			s.NotDescribed = append(s.NotDescribed, &protocol.SchemaCoverageLimit{Kind: "change_stream", Reason: "not-inspected", Provenance: "observed"})
		}},
		{"an object recorded twice", `.*not-described records: publication "Orders" is recorded twice`, func(_ *protocol.Description, s *protocol.Schema) {
			s.NotDescribed = append(s.NotDescribed, &protocol.SchemaCoverageLimit{Kind: "publication", Name: "Orders", Reason: "unsupported", Provenance: "inferred"})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := notDescribedInspection()
			test.mutate(d, snapshot)
			got, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(got, qt.IsNil)
		})
	}
}
