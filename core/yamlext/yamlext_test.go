package yamlext_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
)

type level struct{ Value string }

const levelKind schemaext.Kind = "example.org/widget/level"

func (*level) Kind() schemaext.Kind { return levelKind }
func (l *level) Clone() schemaext.Value {
	cloned := *l
	return &cloned
}
func (l *level) Equal(other schemaext.Value) bool {
	o, ok := other.(*level)
	return ok && *o == *l
}

func levelCoverage() (schemaext.Coverage, error) {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload) }
	registry, err := schemaext.NewRegistry(schemaext.OwnedCodec{Owner: "example.org/widget", Codec: schemaext.Codec{
		Prototype: &level{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(`{"type":"object"}`),
		Clone:  func(payload schemaext.Payload) (schemaext.Payload, error) { return payload.(*level).Clone(), nil },
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*level](data) },
	}})
	if err != nil {
		return schemaext.Coverage{}, err
	}
	model, _ := registry.Identity(levelKind, schemaext.Desired)
	return schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}, nil)
}

func TestNewSet_HappyPath(t *testing.T) {
	c := qt.New(t)
	extension := yamlext.Extension{Owner: "example.org/widget", Kinds: []schemaext.Kind{levelKind}, Coverage: levelCoverage}

	set, err := yamlext.NewSet(extension)
	c.Assert(err, qt.IsNil)
	extension.Kinds[0] = "example.org/changed"
	coverage, coverageErr := set.Coverage()

	c.Assert(set.Selected(), qt.IsTrue)
	c.Assert(set.Kinds(), qt.DeepEquals, []schemaext.Kind{levelKind})
	c.Assert(coverageErr, qt.IsNil)
	c.Assert(coverage.Lookup(levelKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}

func TestNone_IsSelectedAndClaimsNothing(t *testing.T) {
	c := qt.New(t)
	coverage, err := yamlext.None().Coverage()

	c.Assert(yamlext.None().Selected(), qt.IsTrue)
	c.Assert(yamlext.Set{}.Selected(), qt.IsFalse)
	c.Assert(err, qt.IsNil)
	c.Assert(coverage.IsZero(), qt.IsTrue)
}

func TestNewSet_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		extensions []yamlext.Extension
		wantErr    string
	}{
		{name: "an extension without an owner", extensions: []yamlext.Extension{{Coverage: levelCoverage}},
			wantErr: `YAML extension 0 names no owner`},
		{name: "an extension without a coverage claim", extensions: []yamlext.Extension{{Owner: "example.org/widget"}},
			wantErr: `YAML extension of example.org/widget makes no coverage claim`},
		{name: "a model two owners claim", extensions: []yamlext.Extension{
			{Owner: "example.org/first", Kinds: []schemaext.Kind{levelKind}, Coverage: levelCoverage},
			{Owner: "example.org/second", Kinds: []schemaext.Kind{levelKind}, Coverage: levelCoverage},
		}, wantErr: `duplicate.*model "example.org/widget/level" is claimed by example.org/first and example.org/second`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := yamlext.NewSet(test.extensions...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}
