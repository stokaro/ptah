package providerhost

// White-box testing required: engine selection uses the negotiated description
// before an encoding RPC. The public-path fixture checks target refusal; these
// cases exercise ambiguity and aliases without starting a provider per shape.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ptaherr"
)

func encodingHost(engines ...string) *Host {
	host := hclHost(engines...)
	host.description.Features = append(host.description.Features, provider.EncodeSourceFeature)
	for _, engine := range host.description.GetEngines() {
		engine.Features = append(engine.Features, provider.EncodeSourceFeature)
		engine.GoSourceDirectives = []*protocol.GoSourceDirective{{Directive: "ptah:fixture:node", Scope: protocol.GoSourceDirective_DOCUMENT}}
	}
	return host
}

func TestSourceEncodingEngine_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name    string
		engines []string
		target  string
		want    string
	}{
		{"infer the sole engine", []string{"first"}, "", "first"},
		{"explicit engine", []string{"first", "second"}, "second", "second"},
		{"explicit alias", []string{"first", "second"}, "second-alias", "second"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			host := encodingHost(row.engines...)

			goTarget, goErr := (goEncoder{host: host}).goEncodingEngine(row.target)
			hclTarget, hclErr := (&hclOwner{host: host, engines: host.description.GetEngines()}).encodingEngine(row.target)

			c.Assert(goErr, qt.IsNil)
			c.Assert(hclErr, qt.IsNil)
			c.Assert(goTarget.engine.GetId(), qt.Equals, row.want)
			c.Assert(hclTarget.engine.GetId(), qt.Equals, row.want)
		})
	}
}

func TestSourceEncodingEngine_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		engines []string
		target  string
	}{
		{"an explicit target cannot infer the sole engine", []string{"first"}, "postgres"},
		{"an empty target is ambiguous", []string{"first", "second"}, ""},
		{"no encoder", nil, ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			host := encodingHost(row.engines...)

			goTarget, goErr := (goEncoder{host: host}).goEncodingEngine(row.target)
			hclTarget, hclErr := (&hclOwner{host: host, engines: host.description.GetEngines()}).encodingEngine(row.target)

			c.Assert(goErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(hclErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(goTarget, qt.IsNil)
			c.Assert(hclTarget, qt.IsNil)
		})
	}
}
