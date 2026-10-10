package goschema_test

import (
	"encoding/json"
	"errors"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// widget is a synthetic owner model: a level set on a table.
type widget struct {
	Level string `json:"level"`
}

const widgetKind schemaext.Kind = "example.org/widget/level"

func (*widget) Kind() schemaext.Kind { return widgetKind }
func (w *widget) Clone() schemaext.Value {
	cloned := *w
	return &cloned
}
func (w *widget) Equal(other schemaext.Value) bool {
	o, ok := other.(*widget)
	return ok && *o == *w
}

func widgetCoverage() (schemaext.Coverage, error) {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload) }
	registry, err := schemaext.NewRegistry(schemaext.OwnedCodec{Owner: "example.org/widget", Codec: schemaext.Codec{
		Prototype: &widget{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(`{"type":"object"}`),
		Clone:  func(payload schemaext.Payload) (schemaext.Payload, error) { return payload.(*widget).Clone(), nil },
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*widget](data) },
	}})
	if err != nil {
		return schemaext.Coverage{}, err
	}
	model, _ := registry.Identity(widgetKind, schemaext.Desired)
	return schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}, nil)
}

// widgetOwner declares //ptah:schema:widget, which sets a level on the table
// it names. decode stands in for the owner's decoder.
func widgetOwner(c *qt.C, name string, decode func(annotation.Declaration) ([]annotation.Contribution, error)) annotation.Set {
	c.Helper()
	set, err := annotation.NewSet(annotation.Extension{
		Owner: "example.org/widget",
		Directives: []annotation.Directive{{Name: name, Scopes: []annotation.Scope{annotation.ScopeStruct},
			Attributes: []annotation.Attribute{{Name: "table", Value: "string", Required: true}, {Name: "level", Value: "string"}}}},
		Kinds:    []schemaext.Kind{widgetKind},
		Decode:   decode,
		Coverage: annotation.Unlimited(widgetCoverage),
	})
	c.Assert(err, qt.IsNil)
	return set
}

func decodeWidget(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	return []annotation.Contribution{{Table: declaration.Attributes["table"], Label: "a widget",
		Facet: &widget{Level: declaration.Attributes["level"]}}}, nil
}

const widgetSource = "package models\n\n//ptah:schema:widget table=\"items\" level=\"high\"\ntype W struct{}\n\n" +
	"//ptah:schema:table name=\"items\"\ntype Item struct{}\n"

// TestParseSource_AnOwnerDirectiveJoinsTheSchema drives the frontend's half of
// the owner contract: the owner's grammar validates the attributes, its decoder
// answers, the facet reaches the table it names, and the owner's coverage
// claim joins the source's.
func TestParseSource_AnOwnerDirectiveJoinsTheSchema(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(widgetOwner(c, "ptah:schema:widget", decodeWidget), "items.go", widgetSource)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	value, found, err := schemaext.FacetAs[*widget](db.Tables[0].Facets, widgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value, qt.DeepEquals, &widget{Level: "high"})
	c.Assert(db.FeatureCoverage.Lookup(widgetKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}

func TestParseSource_OwnerDirectiveFailurePath(t *testing.T) {
	refused := errors.New("the level is not one the owner knows")
	tests := []struct {
		name    string
		decode  func(annotation.Declaration) ([]annotation.Contribution, error)
		source  string
		wantIs  error
		wantErr string
	}{
		{
			name:    "an attribute the owner's grammar does not declare",
			decode:  decodeWidget,
			source:  "package models\n\n//ptah:schema:widget table=\"items\" colour=\"red\"\ntype W struct{}\n",
			wantIs:  ptaherr.ErrUnknownAttribute,
			wantErr: `(?s).*colour.*`,
		},
		{
			name:    "a required attribute missing",
			decode:  decodeWidget,
			source:  "package models\n\n//ptah:schema:widget level=\"high\"\ntype W struct{}\n",
			wantIs:  ptaherr.ErrMissingRequiredAttribute,
			wantErr: `(?s).*table.*`,
		},
		{
			name:    "the owner refuses the declaration",
			decode:  func(annotation.Declaration) ([]annotation.Contribution, error) { return nil, refused },
			source:  widgetSource,
			wantIs:  ptaherr.ErrInvalidAttributeValue,
			wantErr: `(?s).*the level is not one the owner knows.*`,
		},
		{
			name: "the owner contributes a model it does not declare",
			decode: func(annotation.Declaration) ([]annotation.Contribution, error) {
				return []annotation.Contribution{{Facet: &otherValue{}, Label: "a widget"}}, nil
			},
			source:  widgetSource,
			wantIs:  ptaherr.ErrInvalidAttributeValue,
			wantErr: `(?s).*contributed a model example.org/widget does not declare.*`,
		},
		{
			name:    "a table the file does not declare",
			decode:  decodeWidget,
			source:  "package models\n\n//ptah:schema:widget table=\"missing\"\ntype W struct{}\n",
			wantIs:  ptaherr.ErrInvalidAttributeValue,
			wantErr: `(?s).*table "missing" is not declared in this file, and a widget is declared beside its table.*`,
		},
		{
			name:    "a table given the facet twice",
			decode:  decodeWidget,
			source:  widgetSource + "//ptah:schema:widget table=\"items\" level=\"low\"\ntype V struct{}\n",
			wantIs:  ptaherr.ErrInvalidAttributeValue,
			wantErr: `(?s).*table "items" declares a widget twice.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(widgetOwner(c, "ptah:schema:widget", test.decode), "items.go", test.source)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db.Tables, qt.HasLen, 0)
		})
	}
}

// otherValue is a model no owner of the test declares.
type otherValue struct{}

func (*otherValue) Kind() schemaext.Kind               { return "example.org/other/value" }
func (v *otherValue) Clone() schemaext.Value           { return &otherValue{} }
func (v *otherValue) Equal(other schemaext.Value) bool { _, ok := other.(*otherValue); return ok }

// TestParse_RefusesAnOwnerTakingACommonDirective pins that an owner cannot
// redefine one of the frontend's own directives.
func TestParse_RefusesAnOwnerTakingACommonDirective(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(widgetOwner(c, "ptah:schema:table", decodeWidget), "items.go", widgetSource)

	c.Assert(err, qt.ErrorMatches, `.*directive "ptah:schema:table" of example.org/widget takes the name of one of the frontend's own.*`)
	c.Assert(db.Tables, qt.HasLen, 0)
}

// TestParse_RefusesAnUnselectedOwnerSet holds every entry point to an explicit
// selection: the zero set is a caller that forgot, not one that chose no owner.
func TestParse_RefusesAnUnselectedOwnerSet(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name  string
		parse func() error
	}{
		{name: "ParseSource", parse: func() error {
			_, err := goschema.ParseSource(annotation.Set{}, "items.go", widgetSource)
			return err
		}},
		{name: "ParseFile", parse: func() error { _, err := goschema.ParseFile(annotation.Set{}, "items.go"); return err }},
		{name: "ParseDir", parse: func() error { _, err := goschema.ParseDir(annotation.Set{}, root); return err }},
		{name: "ParseDirRaw", parse: func() error { _, err := goschema.ParseDirRaw(annotation.Set{}, root); return err }},
		{name: "ParseDirs", parse: func() error { _, err := goschema.ParseDirs(annotation.Set{}, root, root); return err }},
		{name: "ParseFS", parse: func() error {
			_, err := goschema.ParseFS(annotation.Set{}, fstest.MapFS{"items.go": {Data: []byte(widgetSource)}}, ".")
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.parse(), qt.ErrorIs, annotation.ErrUnselected)
		})
	}
}
