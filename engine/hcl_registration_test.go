package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// hclProvider registers an HCL extension of owner that claims kinds and
// names unrecorded as models HCL cannot record a limit of. The provider owns
// the codecs of conversionFirst alone.
func hclProvider(owner string, kinds, unrecorded []schemaext.Kind) engine.Provider {
	return engine.Provider{ID: "example.org/converter", Targets: []engine.Target{{Name: "custom"}},
		Codecs: []schemaext.Codec{conversionCodec(conversionFirst, schemaext.Desired), conversionCodec(conversionFirst, schemaext.Observed)},
		HCL:    []hclext.Extension{{Owner: owner, Kinds: kinds, Unrecorded: unrecorded}},
	}
}

// TestHCL_HappyPath freezes a provider's HCL extension into the runtime's
// set, the models whose limits it cannot record included.
func TestHCL_HappyPath(t *testing.T) {
	c := qt.New(t)

	runtime, err := engine.New(hclProvider("example.org/converter", nil, []schemaext.Kind{conversionFirst}))

	c.Assert(err, qt.IsNil)
	c.Assert(runtime.HCL().Selected(), qt.IsTrue)
	c.Assert(runtime.HCL().Unrecorded(conversionFirst), qt.IsTrue)
	c.Assert(runtime.HCL().Unrecorded(conversionSecond), qt.IsFalse)
}

// TestHCL_FailurePath refuses an extension another provider owns, and one
// that claims a model, or says it cannot record a model's limits, whose
// desired codec its provider does not own.
func TestHCL_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		provider engine.Provider
		wantErr  string
	}{
		{name: "an extension another provider owns", provider: hclProvider("example.org/other", []schemaext.Kind{conversionFirst}, nil),
			wantErr: `.*provider "example.org/converter" registers HCL blocks owned by "example.org/other"`},
		{name: "a model whose desired codec the provider does not own", provider: hclProvider("example.org/converter", []schemaext.Kind{conversionSecond}, nil),
			wantErr: `.*provider "example.org/converter" claims model "example.org/second" for HCL without owning its desired codec`},
		{name: "an unrecorded model whose desired codec the provider does not own",
			provider: hclProvider("example.org/converter", nil, []schemaext.Kind{conversionSecond}),
			wantErr:  `.*provider "example.org/converter" claims model "example.org/second" for HCL without owning its desired codec`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			runtime, err := engine.New(test.provider)

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(runtime, qt.IsNil)
		})
	}
}
