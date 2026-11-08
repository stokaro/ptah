package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// goDocumentExchange writes the native value with a Go directive of the
// document that says where it stands, and reports the facet as a loss.
func goDocumentExchange() (*protocol.Description, *protocol.EncodeSourceRequest, *protocol.EncodedSource) {
	d, request, result := goEncodeExchange()
	d.Features = append(d.Features, provider.GoPlacementsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.GoPlacementsFeature)
	claim := d.Engines[0].GoSourceDirectives[0]
	claim.Scope = protocol.GoSourceDirective_DOCUMENT
	encoded := result.Fragments[0]
	encoded.Fragment.GoDirective, encoded.Fragment.BindingId = proto.CloneOf(claim), ""
	encoded.ValueIds, encoded.Placement = []string{"second"}, protocol.EncodedFragment_GO_AFTER_PRIVILEGES
	result.Losses = []*protocol.EncodeLoss{{ValueId: request.Values[0].Value.Id, Message: "a retention facet has no Go directive"}}
	return d, request, result
}

func TestGoPlacements_HappyPath(t *testing.T) {
	for _, placement := range []protocol.EncodedFragment_Placement{protocol.EncodedFragment_GO_AFTER_SCHEMAS, protocol.EncodedFragment_GO_AFTER_VIEWS,
		protocol.EncodedFragment_GO_AFTER_PRIVILEGES, protocol.EncodedFragment_PLACEMENT_UNSPECIFIED} {
		t.Run(placement.String(), func(t *testing.T) {
			c := qt.New(t)
			d, request, result := goDocumentExchange()
			result.Fragments[0].Placement = placement
			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
		})
	}
}

func TestGoPlacements_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.EncodedSource)
	}{
		{"a placement the engine did not negotiate", `.*a Go placement needs source\.go-placements`, func(d *protocol.Description, _ *protocol.EncodedSource) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.GoPlacementsFeature })
		}},
		{"a placement of a table directive", `.*a Go placement belongs to a Go directive of the document`, func(d *protocol.Description, r *protocol.EncodedSource) {
			d.Engines[0].GoSourceDirectives[0].Scope = protocol.GoSourceDirective_TABLE
			r.Fragments[0].Fragment.GoDirective.Scope = protocol.GoSourceDirective_TABLE
			r.Fragments[0].Fragment.BindingId = "table"
		}},
		{"an HCL placement of a Go directive", `.*an HCL block of the owner's says where it stands, and no other fragment does`,
			func(_ *protocol.Description, r *protocol.EncodedSource) {
				r.Fragments[0].Placement = protocol.EncodedFragment_AFTER_TABLES
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := goDocumentExchange()
			test.mutate(d, result)
			err := provider.ValidateEncodedSource(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// An HCL block keeps saying where it stands with the feature negotiated: the
// Go placements are not HCL's.
func TestGoPlacementsOfAnHCLBlock(t *testing.T) {
	c := qt.New(t)
	d, request, result := hclEncodeExchange()
	d.Features = append(d.Features, provider.GoPlacementsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.GoPlacementsFeature)
	result.Fragments[0].Placement = protocol.EncodedFragment_GO_AFTER_VIEWS
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorMatches, `.*a Go placement belongs to a Go directive of the document`)
}

func TestGoPlacementsDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"the engine's feature without the provider's", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.GoPlacementsFeature })
		}},
		{"an engine that does not encode", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.EncodeSourceFeature })
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := goDocumentExchange()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*Go placements without the provider's source\.go-placements and source encoding`)
		})
	}
}

// A host that does not list the feature receives an engine without it, so a
// reply that places a directive is refused, and one that places none is not.
func TestGoPlacementsNegotiation(t *testing.T) {
	c := qt.New(t)
	d, request, result := goDocumentExchange()
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(unlisted.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.GoPlacementsFeature)
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
	c.Assert(provider.ValidateEncodedSource(unlisted, request, result), qt.ErrorMatches, `.*a Go placement needs source\.go-placements`)
	result.Fragments[0].Placement = protocol.EncodedFragment_PLACEMENT_UNSPECIFIED
	c.Assert(provider.ValidateEncodedSource(unlisted, request, result), qt.IsNil)
	listed := provider.NegotiatedDescription(d, []string{provider.GoPlacementsFeature})
	c.Assert(proto.Equal(listed, d), qt.IsTrue)
}
