package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/engine"
)

// selectionProvider registers a scope selection of kind owned by owner. The
// provider owns the codecs of conversionFirst alone.
func selectionProvider(owner string, kind schemaext.Kind) engine.Provider {
	return engine.Provider{ID: "example.org/converter", Targets: []engine.Target{{Name: "custom"}},
		Codecs: []schemaext.Codec{conversionCodec(conversionFirst, schemaext.Desired), conversionCodec(conversionFirst, schemaext.Observed)},
		Selections: []selectext.Extension{{Owner: owner, Selections: []selectext.Selection{
			{Kind: kind, Type: "widget", Naming: selectext.InSchema}}}},
	}
}

// TestSelections_HappyPath freezes a provider's scope selections into the
// runtime's set, and a runtime without one into a selected empty set.
func TestSelections_HappyPath(t *testing.T) {
	c := qt.New(t)

	runtime, err := engine.New(selectionProvider("example.org/converter", conversionFirst))
	empty, emptyErr := engine.New()

	c.Assert(err, qt.IsNil)
	selection, found := runtime.Selections().Selection("widget")
	c.Assert(found, qt.IsTrue)
	c.Assert(selection.Kind, qt.Equals, conversionFirst)
	c.Assert(emptyErr, qt.IsNil)
	c.Assert(empty.Selections().Selected(), qt.IsTrue)
	c.Assert(empty.Selections().Selections(), qt.HasLen, 0)
}

// TestSelections_FailurePath refuses a selection another provider owns, and
// one of a model whose desired codec the provider does not own.
func TestSelections_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		provider engine.Provider
		wantErr  string
	}{
		{name: "a selection another provider owns", provider: selectionProvider("example.org/other", conversionFirst),
			wantErr: `.*provider "example.org/converter" registers scope selections owned by "example.org/other"`},
		{name: "a model whose desired codec the provider does not own", provider: selectionProvider("example.org/converter", conversionSecond),
			wantErr: `.*provider "example.org/converter" selects model "example.org/second" without owning its desired codec`},
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
