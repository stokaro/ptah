package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func goLimitClaims(d *protocol.Description) {
	d.Features = append(d.Features, provider.SourceLimitsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.SourceLimitsFeature)
	d.Engines[0].GoHeaderKinds = []string{"retention"}
}

func TestGoLimitsKeepTheirEvidenceAcrossEncodingAndCapture(t *testing.T) {
	c := qt.New(t)
	d, encode, encoded := goEncodeExchange()
	goLimitClaims(d)
	encoded.Limits[0].Reason, encoded.Limits[0].Provenance = "not-inspected", "observed"
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateEncodedSource(d, encode, encoded), qt.IsNil)
	decodeDescription, decode, decoded := goSourceExchange()
	goLimitClaims(decodeDescription)
	decode.Documents[0].Limits = []*protocol.SchemaCoverageLimit{proto.CloneOf(encoded.Limits[0])}
	c.Assert(provider.ValidateDecodedSource(decodeDescription, decode, decoded), qt.IsNil)
	c.Assert(proto.Equal(decode.Documents[0].Limits[0], encoded.Limits[0]), qt.IsTrue)
	legacy := provider.NegotiatedDescription(decodeDescription, nil)
	c.Assert(legacy.Engines[0].GoHeaderKinds, qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(legacy, decode), qt.ErrorIs, provider.ErrProtocol)
	decode.Documents[0].Limits = nil
	c.Assert(provider.ValidateSourceRequest(legacy, decode), qt.IsNil)
}

func TestGoLimitsRefuseUnclaimedAndRepeatedHeaders(t *testing.T) {
	c := qt.New(t)
	d, request, result := goEncodeExchange()
	goLimitClaims(d)
	result.Limits[0].Kind = "foreign"
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorIs, provider.ErrProtocol)
	result.Limits[0].Kind = "retention"
	result.Limits = append(result.Limits, proto.CloneOf(result.Limits[0]))
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorIs, provider.ErrProtocol)
	d.Engines[0].GoHeaderKinds = append(d.Engines[0].GoHeaderKinds, "retention")
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
	for _, kind := range []string{"", " ", "UPPER", "two words", "kind\"", "kind=value", "kind/other"} {
		t.Run(kind, func(t *testing.T) {
			c := qt.New(t)
			d.Engines[0].GoHeaderKinds = []string{kind}
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestClaimedSourceLimitsPreserveNarrowedKnowledge(t *testing.T) {
	for _, test := range []struct {
		name     string
		exchange func() (*protocol.Description, *protocol.DecodeSourceRequest, *protocol.DecodedSource)
	}{
		{"Go", goSourceExchange},
		{"HCL", hclExchange},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := test.exchange()
			d.Features = append(d.Features, provider.SourceLimitsFeature)
			d.Engines[0].Features = append(d.Engines[0].Features, provider.SourceLimitsFeature)
			if request.Documents[0].Format == protocol.SourceDocument_GO_ANNOTATIONS {
				d.Engines[0].GoHeaderKinds = []string{"retention"}
			}
			request.Documents[0].Limits = []*protocol.SchemaCoverageLimit{{Kind: "retention", Reason: "not-inspected", Provenance: "observed"}}
			unknown := &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "The source preserves an uninspected model"}
			result.Documents[0].Coverage.Kinds[0].Knowledge = unknown
			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			c.Assert(provider.ValidateDecodedSource(d, request, result), qt.IsNil)
			legacy := provider.NegotiatedDescription(d, []string{provider.HCLSourceFeature})
			c.Assert(provider.ValidateDecodedSource(legacy, request, result), qt.ErrorIs, provider.ErrProtocol)
			request.Documents[0].Limits = nil
			c.Assert(provider.ValidateDecodedSource(d, request, result), qt.ErrorIs, provider.ErrProtocol)
			request.Documents[0].Limits = []*protocol.SchemaCoverageLimit{{Kind: "foreign"}}
			c.Assert(provider.ValidateDecodedSource(d, request, result), qt.ErrorIs, provider.ErrProtocol)
			request.Documents[0].Limits = []*protocol.SchemaCoverageLimit{{Kind: "retention", Name: "hidden"}}
			result.Documents[0].Coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}
			subject := proto.CloneOf(result.Documents[0].Coverage.Subjects[0])
			subject.Subject.Name, subject.NormalizedSubject.Name, subject.Knowledge = "hidden", "hidden", unknown
			result.Documents[0].Coverage.Subjects = append(result.Documents[0].Coverage.Subjects, subject)
			c.Assert(provider.ValidateDecodedSource(d, request, result), qt.IsNil)
			request.Documents[0].Limits = append(request.Documents[0].Limits, proto.CloneOf(request.Documents[0].Limits[0]))
			c.Assert(provider.ValidateDecodedSource(d, request, result), qt.ErrorIs, provider.ErrProtocol)
			request.Documents[0].Limits = request.Documents[0].Limits[:1]
			result.Documents[0].Coverage.Kinds = nil
			c.Assert(provider.ValidateDecodedSource(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestSourceLimitsDisappearWithTheirOnlyFormat(t *testing.T) {
	c := qt.New(t)
	d, _, _ := hclExchange()
	d.Features = append(d.Features, provider.SourceLimitsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.SourceLimitsFeature)
	negotiated := provider.NegotiatedDescription(d, []string{provider.SourceLimitsFeature})
	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.SourceLimitsFeature)
}

func TestSourceEncodingCannotDiscardNarrowedKnowledge(t *testing.T) {
	for _, test := range []struct {
		name     string
		exchange func() (*protocol.Description, *protocol.EncodeSourceRequest, *protocol.EncodedSource)
	}{
		{"Go", goEncodeExchange},
		{"HCL", hclEncodeExchange},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := test.exchange()
			goLimitClaims(d)
			request.Coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "not inspected"}
			c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
			result.Limits = nil
			c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorIs, provider.ErrProtocol)
			request.Coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}
			c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
			request.Coverage.Subjects[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNREPRESENTABLE, Reason: "unread setting"}
			c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorIs, provider.ErrProtocol)
			result.Limits = []*protocol.SchemaCoverageLimit{{Kind: "retention", Name: "records"}}
			c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
		})
	}
}

func TestSourceLimitsDoNotChangeYAMLEncoding(t *testing.T) {
	c := qt.New(t)
	d, request, result := encodeExchange()
	request.Coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "not inspected"}
	result.Limits = nil
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
	goLimitClaims(d)
	model := d.Engines[0].ValueModels[0]
	model.SourceFormats = append(model.SourceFormats, protocol.SourceDocument_GO_ANNOTATIONS)
	d.Engines[0].GoSourceDirectives = []*protocol.GoSourceDirective{{Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:fixture:retention",
		Selector: "retention", Models: []string{model.Kind}}}
	request.Coverage.Kinds[0].Model = proto.CloneOf(model)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
	result.Limits = []*protocol.SchemaCoverageLimit{{Kind: "yaml-limit"}}
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
}
