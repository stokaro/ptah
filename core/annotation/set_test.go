package annotation_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

type level struct {
	Value string `json:"value"`
}

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

func widget(owner, directive string) annotation.Extension {
	return annotation.Extension{
		Owner:      owner,
		Directives: []annotation.Directive{{Name: directive, Scopes: []annotation.Scope{annotation.ScopeStruct}}},
		Kinds:      []schemaext.Kind{levelKind},
		Decode: func(declaration annotation.Declaration) ([]annotation.Contribution, error) {
			return []annotation.Contribution{{Facet: &level{Value: declaration.Attributes["level"]}, Label: "a level"}}, nil
		},
		Coverage: levelCoverage,
	}
}

func TestNewSet_HappyPath(t *testing.T) {
	c := qt.New(t)
	extension := widget("example.org/widget", "ptah:schema:widget")

	set, err := annotation.NewSet(extension)
	c.Assert(err, qt.IsNil)
	extension.Directives[0].Name = "changed after registration"
	contributions, err := set.Decode(annotation.Declaration{Directive: "ptah:schema:widget", Attributes: map[string]string{"level": "high"}})
	c.Assert(err, qt.IsNil)
	owner, found := set.Owner("ptah:schema:widget")
	coverage, coverageErr := set.Coverage()

	c.Assert(set.Selected(), qt.IsTrue)
	c.Assert(found, qt.IsTrue)
	c.Assert(owner, qt.Equals, "example.org/widget")
	c.Assert(set.Directives(), qt.HasLen, 1)
	c.Assert(set.Directives()[0].Name, qt.Equals, "ptah:schema:widget")
	c.Assert(set.Kinds(), qt.DeepEquals, []schemaext.Kind{levelKind})
	c.Assert(contributions, qt.DeepEquals, []annotation.Contribution{{Facet: &level{Value: "high"}, Label: "a level"}})
	c.Assert(coverageErr, qt.IsNil)
	c.Assert(coverage.Lookup(levelKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}

func TestNone_IsSelectedAndEmpty(t *testing.T) {
	c := qt.New(t)
	coverage, err := annotation.None().Coverage()

	c.Assert(annotation.None().Selected(), qt.IsTrue)
	c.Assert(annotation.Set{}.Selected(), qt.IsFalse)
	c.Assert(annotation.None().Directives(), qt.HasLen, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(coverage.IsZero(), qt.IsTrue)
}

func TestNewSet_FailurePath(t *testing.T) {
	noOwner := widget("", "ptah:schema:widget")
	noCoverage := widget("example.org/widget", "ptah:schema:widget")
	noCoverage.Coverage = nil
	noDecoder := widget("example.org/widget", "ptah:schema:widget")
	noDecoder.Decode = nil
	unnamed := widget("example.org/widget", " ")
	tests := []struct {
		name       string
		extensions []annotation.Extension
		wantErr    string
	}{
		{name: "an extension without an owner", extensions: []annotation.Extension{noOwner}, wantErr: `annotation extension 0 names no owner`},
		{name: "an extension without a coverage claim", extensions: []annotation.Extension{noCoverage}, wantErr: `.* makes no coverage claim`},
		{name: "directives without a decoder", extensions: []annotation.Extension{noDecoder}, wantErr: `.* declares directives and no decoder`},
		{name: "a directive without a name", extensions: []annotation.Extension{unnamed}, wantErr: `.* declares a directive without a name`},
		{name: "a directive two owners declare", extensions: []annotation.Extension{
			widget("example.org/first", "ptah:schema:widget"),
			{Owner: "example.org/second", Directives: []annotation.Directive{{Name: "ptah:schema:widget"}}, Decode: noContributions, Coverage: levelCoverage},
		}, wantErr: `duplicate.*directive "ptah:schema:widget" is declared by example.org/first and example.org/second`},
		{name: "a model two owners produce", extensions: []annotation.Extension{
			widget("example.org/first", "ptah:schema:widget"), widget("example.org/second", "ptah:schema:other"),
		}, wantErr: `duplicate.*model "example.org/widget/level" is produced by example.org/first and example.org/second`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := annotation.NewSet(test.extensions...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

func noContributions(annotation.Declaration) ([]annotation.Contribution, error) { return nil, nil }

func TestSet_Decode_FailurePath(t *testing.T) {
	both := widget("example.org/widget", "ptah:schema:widget")
	both.Decode = func(annotation.Declaration) ([]annotation.Contribution, error) {
		object := schemaext.Object{Value: &level{}}
		return []annotation.Contribution{{Object: &object, Facet: &level{}}}, nil
	}
	undeclared := widget("example.org/widget", "ptah:schema:widget")
	undeclared.Kinds = []schemaext.Kind{"example.org/widget/other"}
	tests := []struct {
		name      string
		extension annotation.Extension
		directive string
		wantErr   string
	}{
		{name: "a directive no owner declares", extension: widget("example.org/widget", "ptah:schema:widget"),
			directive: "ptah:schema:gadget", wantErr: `no selected owner declares directive "ptah:schema:gadget"`},
		{name: "a contribution with both an object and a facet", extension: both,
			directive: "ptah:schema:widget", wantErr: `.*contributed neither or both of an object and a facet`},
		{name: "a contribution of a model the owner does not declare", extension: undeclared,
			directive: "ptah:schema:widget", wantErr: `.*contributed a model example.org/widget does not declare`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, err := annotation.NewSet(test.extension)
			c.Assert(err, qt.IsNil)

			contributions, err := set.Decode(annotation.Declaration{Directive: test.directive})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(contributions, qt.IsNil)
		})
	}
}

func TestQualifiedName(t *testing.T) {
	tests := []struct {
		name, schema, object, wantSchema, wantName string
	}{
		{name: "a qualified name names its schema", object: "metrics.hourly", wantSchema: "metrics", wantName: "hourly"},
		{name: "a schema attribute names the schema", schema: "metrics", object: "hourly", wantSchema: "metrics", wantName: "hourly"},
		{name: "an unqualified name", object: "hourly", wantName: "hourly"},
		{name: "surrounding space is trimmed", schema: " metrics ", object: " hourly ", wantSchema: "metrics", wantName: "hourly"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema, name := annotation.QualifiedName(test.schema, test.object)
			c.Assert(schema, qt.Equals, test.wantSchema)
			c.Assert(name, qt.Equals, test.wantName)
		})
	}
}
