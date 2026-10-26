package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func goLimitClaims(d *protocol.Description) {
	d.Features = append(d.Features, provider.GoLimitsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.GoLimitsFeature)
	d.Engines[0].GoHeaderKinds = []string{"retention"}
}

func TestGoLimitsKeepTheirEvidenceAcrossEncodingAndCapture(t *testing.T) {
	c := qt.New(t)
	d, encode, encoded := goEncodeExchange()
	goLimitClaims(d)
	encoded.Limits[0].Reason, encoded.Limits[0].Provenance = "Not inspected", "database read"
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
