package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func groupedExchange() (*protocol.Description, *protocol.EncodeSourceRequest, *protocol.EncodedSource) {
	d, request, result := encodeExchange()
	d.Features = append(d.Features, provider.EncodeGroupsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.EncodeGroupsFeature)
	first := result.Fragments[0]
	second := proto.CloneOf(first.Fragment)
	second.Id = "other-setting"
	claim := proto.CloneOf(second.YamlKey)
	claim.Key, claim.Selector = "other_setting", "other_setting"
	d.Engines[0].YamlSourceKeys = append(d.Engines[0].YamlSourceKeys, claim)
	second.YamlKey, second.Selector = proto.CloneOf(claim), claim.Selector
	result.Groups = []*protocol.EncodedSourceGroup{{ValueIds: first.ValueIds, Parts: []*protocol.EncodedSourcePart{{Fragment: first.Fragment}, {Fragment: second}}}}
	result.Fragments = nil
	return d, request, result
}

func TestEncodingGroupsKeepOneValueAcrossOrderedClaims(t *testing.T) {
	c := qt.New(t)
	d, request, result := groupedExchange()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
	parts := provider.EncodedSourceParts(result)
	c.Assert(parts, qt.HasLen, 2)
	c.Assert(parts[0].Fragment.Id, qt.Equals, "settings")
	c.Assert(parts[1].Fragment.Id, qt.Equals, "other-setting")
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(provider.ValidateEncodedSource(unlisted, request, result), qt.ErrorIs, provider.ErrProtocol)
	result.Groups = nil
	_, _, flat := encodeExchange()
	c.Assert(provider.ValidateEncodedSource(unlisted, request, flat), qt.IsNil)
	c.Assert(provider.ValidateEncodedSource(d, request, flat), qt.ErrorIs, provider.ErrProtocol)
}

func TestEncodingGroupsKeepDistinctColumnSubjects(t *testing.T) {
	c := qt.New(t)
	d, request, result := groupedExchange()
	d.Features = append(d.Features, provider.EntryKeysFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.EntryKeysFeature)
	first := request.Values[0]
	first.Value.Value.Owner.Kind = "column"
	first.Value.Value.Owner.Path = []string{request.Bindings[0].Object.Name}
	first.Value.Value.Owner.Name = "first"
	first.Value.NormalizedOwner = proto.CloneOf(first.Value.Value.Owner)
	second := proto.CloneOf(first)
	second.Value.Id, second.Value.Value.Owner.Name = "second-column", "second"
	second.Value.NormalizedOwner = proto.CloneOf(second.Value.Value.Owner)
	request.Values = []*protocol.EncodeSourceValue{first, second}
	result.Losses = nil
	result.Groups[0].ValueIds = []string{first.Value.Id, second.Value.Id}
	for i, value := range request.Values {
		claim := d.Engines[0].YamlSourceKeys[i]
		claim.Scope = protocol.YAMLSourceKey_COLUMN
		part := result.Groups[0].Parts[i]
		part.Fragment.YamlKey = proto.CloneOf(claim)
		part.Fragment.Subject = proto.CloneOf(value.Value.NormalizedOwner)
	}
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
	result.Groups[0].Parts[1].Fragment.Subject = proto.CloneOf(first.Value.NormalizedOwner)
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorIs, provider.ErrProtocol)
}

func TestEncodingGroupsRefuseUnaccountedOrUnboundParts(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.EncodeSourceRequest, *protocol.EncodedSource)
	}{
		{"empty group", func(_ *protocol.Description, _ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.Groups[0].ValueIds = nil
		}},
		{"empty parts", func(_ *protocol.Description, _ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.Groups[0].Parts = nil
		}},
		{"duplicate value", func(_ *protocol.Description, _ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.Groups[0].ValueIds = append(r.Groups[0].ValueIds, r.Groups[0].ValueIds[0])
		}},
		{"missing value", func(_ *protocol.Description, _ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.Groups = nil
		}},
		{"unknown value", func(_ *protocol.Description, _ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.Groups[0].ValueIds[0] = "foreign"
		}},
		{"written and lost", func(_ *protocol.Description, _ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.Losses[0].ValueId = r.Groups[0].ValueIds[0]
		}},
		{"mixed shapes", func(_ *protocol.Description, _ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.Fragments = []*protocol.EncodedFragment{{}}
		}},
		{"unclaimed syntax", func(_ *protocol.Description, _ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.Groups[0].Parts[1].Fragment.YamlKey.Key = "foreign"
		}},
		{"position", func(_ *protocol.Description, _ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.Groups[0].Parts[1].Fragment.Span = &protocol.SourceSpan{}
		}},
		{"placement on YAML", func(_ *protocol.Description, _ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.Groups[0].Parts[1].Placement = protocol.EncodedFragment_AFTER_TABLES
		}},
		{"another binding", func(_ *protocol.Description, request *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			binding := proto.CloneOf(request.Bindings[0])
			binding.Id, binding.Object.Name = "other", "other"
			request.Bindings = append(request.Bindings, binding)
			r.Groups[0].Parts[1].Fragment.BindingId = binding.Id
		}},
		{"another subject", func(d *protocol.Description, request *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			// The second table has a valid value of the same model, but the first
			// group's parts cannot account for its distinct binding and subject.
			binding := proto.CloneOf(request.Bindings[0])
			binding.Id, binding.Object.Name = "other", "other"
			request.Bindings = append(request.Bindings, binding)
			value := proto.CloneOf(request.Values[0])
			value.BindingId, value.Value.Id = binding.Id, "other"
			value.Value.Value.Owner, value.Value.NormalizedOwner = proto.CloneOf(binding.Object), proto.CloneOf(binding.Object)
			request.Values = append(request.Values, value)
			r.Groups[0].ValueIds = append(r.Groups[0].ValueIds, value.Value.Id)
			_ = d
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := groupedExchange()
			test.mutate(d, request, result)
			c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
