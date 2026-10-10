package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/engine"
)

func annotationProvider(owner string, kinds []schemaext.Kind, directive string) engine.Provider {
	return engine.Provider{ID: "example.org/converter", Targets: []engine.Target{{Name: "custom"}},
		Codecs: []schemaext.Codec{conversionCodec(conversionFirst, schemaext.Desired), conversionCodec(conversionFirst, schemaext.Observed)},
		Annotations: []annotation.Extension{{
			Owner: owner, Kinds: kinds,
			Directives: []annotation.Directive{{Name: directive, Scopes: []annotation.Scope{annotation.ScopeStruct}}},
			Decode: func(annotation.Declaration) ([]annotation.Contribution, error) {
				return []annotation.Contribution{{Facet: &conversionValue{ID: conversionFirst, Number: 1}}}, nil
			},
			Coverage: func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil },
		}},
	}
}

func TestAnnotations_HappyPath(t *testing.T) {
	c := qt.New(t)

	runtime, err := engine.New(annotationProvider("example.org/converter", []schemaext.Kind{conversionFirst}, "ptah:schema:widget"))
	c.Assert(err, qt.IsNil)
	owner, found := runtime.Annotations().Owner("ptah:schema:widget")

	c.Assert(runtime.Annotations().Selected(), qt.IsTrue)
	c.Assert(found, qt.IsTrue)
	c.Assert(owner, qt.Equals, "example.org/converter")
}

// TestAnnotations_ARuntimeWithoutExtensionsSelectsNone pins that a runtime is
// always an explicit selection: one whose providers contribute no directive
// selects none rather than handing a parse the zero set it would refuse.
func TestAnnotations_ARuntimeWithoutExtensionsSelectsNone(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New()
	c.Assert(err, qt.IsNil)
	c.Assert(runtime.Annotations().Selected(), qt.IsTrue)
	c.Assert(runtime.Annotations().Directives(), qt.HasLen, 0)
}

func TestAnnotations_FailurePath(t *testing.T) {
	duplicate := annotationProvider("example.org/converter", []schemaext.Kind{conversionFirst}, "ptah:schema:widget")
	second := engine.Provider{ID: "example.org/second", Annotations: []annotation.Extension{{
		Owner: "example.org/second", Directives: []annotation.Directive{{Name: "ptah:schema:widget"}},
		Decode:   func(annotation.Declaration) ([]annotation.Contribution, error) { return nil, nil },
		Coverage: func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil },
	}}}
	tests := []struct {
		name      string
		providers []engine.Provider
		wantErr   string
	}{
		{name: "an extension another provider owns",
			providers: []engine.Provider{annotationProvider("example.org/other", []schemaext.Kind{conversionFirst}, "ptah:schema:widget")},
			wantErr:   `.*provider "example.org/converter" registers annotations owned by "example.org/other"`},
		{name: "a model whose desired codec the provider does not own",
			providers: []engine.Provider{annotationProvider("example.org/converter", []schemaext.Kind{conversionSecond}, "ptah:schema:widget")},
			wantErr:   `.*provider "example.org/converter" declares model "example.org/second" from annotations without owning its desired codec`},
		{name: "a directive two providers declare",
			providers: []engine.Provider{duplicate, second},
			wantErr:   `.*directive "ptah:schema:widget" is declared by example.org/converter and example.org/second`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			runtime, err := engine.New(test.providers...)

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

func yamlProvider(owner string, kinds []schemaext.Kind) engine.Provider {
	return engine.Provider{ID: "example.org/converter", Targets: []engine.Target{{Name: "custom"}},
		Codecs: []schemaext.Codec{conversionCodec(conversionFirst, schemaext.Desired), conversionCodec(conversionFirst, schemaext.Observed)},
		YAML: []yamlext.Extension{{Owner: owner, Kinds: kinds,
			Coverage: func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }}},
	}
}

func TestYAML_HappyPath(t *testing.T) {
	c := qt.New(t)

	runtime, err := engine.New(yamlProvider("example.org/converter", []schemaext.Kind{conversionFirst}))
	c.Assert(err, qt.IsNil)
	empty, err := engine.New()
	c.Assert(err, qt.IsNil)

	c.Assert(runtime.YAML().Selected(), qt.IsTrue)
	c.Assert(runtime.YAML().Kinds(), qt.DeepEquals, []schemaext.Kind{conversionFirst})
	c.Assert(empty.YAML().Selected(), qt.IsTrue)
	c.Assert(empty.YAML().Kinds(), qt.HasLen, 0)
}

func TestYAML_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		provider engine.Provider
		wantErr  string
	}{
		{name: "a claim another provider owns", provider: yamlProvider("example.org/other", []schemaext.Kind{conversionFirst}),
			wantErr: `.*provider "example.org/converter" registers YAML claims owned by "example.org/other"`},
		{name: "a model whose desired codec the provider does not own", provider: yamlProvider("example.org/converter", []schemaext.Kind{conversionSecond}),
			wantErr: `.*provider "example.org/converter" claims model "example.org/second" for YAML without owning its desired codec`},
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
