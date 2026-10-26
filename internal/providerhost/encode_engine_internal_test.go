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
	"ptah.run/core/schemaext"
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

func TestGoNativeObjectScope(t *testing.T) {
	const kind schemaext.Kind = "example.org/fixture/node"
	for _, row := range []struct {
		name         string
		modelEngines []string
		targets      []string
		want         error
	}{
		{"same engine", []string{"first", "second"}, []string{"first"}, nil},
		{"same engine alias", []string{"first"}, []string{"first-alias"}, nil},
		{"unscoped single-engine model", []string{"first"}, nil, nil},
		{"another engine", []string{"first", "second"}, []string{"second"}, ptaherr.ErrUnsupportedFeature},
		{"several engines", []string{"first", "second"}, []string{"first", "second"}, ptaherr.ErrUnsupportedFeature},
		{"unscoped shared model", []string{"first", "second"}, nil, ptaherr.ErrUnsupportedFeature},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			host := encodingHost("first", "second")
			contract := &modelContract{engines: make(map[string]*protocol.ValueModel)}
			for _, name := range row.modelEngines {
				contract.engines[name] = &protocol.ValueModel{Kind: string(kind)}
			}
			host.models = map[modelKey]*modelContract{{kind: kind, representation: schemaext.Desired}: contract}
			err := host.targets["first"].encodeBatch(protocol.SourceDocument_GO_ANNOTATIONS).goObjectScope(kind, row.targets)
			c.Assert(err, qt.ErrorIs, row.want)
		})
	}
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
