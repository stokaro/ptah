package annotation_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// mark is a feature owner's named object in these tests.
type mark struct{ Text string }

const markKind schemaext.Kind = "example.org/widget/mark"

func (*mark) Kind() schemaext.Kind { return markKind }
func (m *mark) Clone() schemaext.Value {
	cloned := *m
	return &cloned
}
func (m *mark) Equal(other schemaext.Value) bool {
	o, ok := other.(*mark)
	return ok && *o == *m
}

// recorder is an encoder that records what it is handed and answers with a
// fixed result.
type recorder struct {
	handed *[]annotation.EncodeRequest
	result annotation.EncodeResult
	err    error
}

func (r recorder) EncodeAnnotations(_ context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	*r.handed = append(*r.handed, request)
	return r.result, r.err
}

// encoding is a widget extension whose encoder writes result for the models
// of encodes.
func encoding(owner, directive string, kinds []schemaext.Kind, handed *[]annotation.EncodeRequest, result annotation.EncodeResult) annotation.Extension {
	extension := widget(owner, directive)
	extension.Kinds = kinds
	extension.Encoder = recorder{handed: handed, result: result}
	extension.Encodes = kinds
	return extension
}

func markObject(name string) schemaext.Object {
	return schemaext.Object{Ref: objectidentity.ID{Kind: objectidentity.Kind(markKind), Name: objectidentity.Part{Source: name, Normalized: name}},
		Value: &mark{Text: name}}
}

func TestSet_Encode_HappyPath(t *testing.T) {
	c := qt.New(t)
	var handed []annotation.EncodeRequest
	table := annotation.Table{Schema: "app", Name: "rooms", Struct: "Room"}
	written := annotation.EncodedDirective{Directive: "ptah:schema:level", Placement: annotation.AfterTable, Table: &table,
		Attributes: []annotation.EncodedAttribute{{Name: "value", Value: "high"}}}
	marked := annotation.EncodedDirective{Directive: "ptah:schema:mark", Placement: annotation.AfterSchemas}
	// Registered with the later name first: the result is ordered by owner
	// name, not by registration.
	set := must.Must(annotation.NewSet(
		encoding("example.org/marks", "ptah:schema:mark", []schemaext.Kind{markKind}, &handed, annotation.EncodeResult{Directives: []annotation.EncodedDirective{marked}}),
		encoding("example.org/levels", "ptah:schema:level", []schemaext.Kind{levelKind}, &handed, annotation.EncodeResult{Directives: []annotation.EncodedDirective{written}}),
	))
	coverage := must.Must(levelCoverage())
	levels := must.Must(schemaext.NewFacets(&level{Value: "high"}))
	slot := annotation.Slot{Directive: "ptah:schema:table", Table: table}

	result, err := set.Encode(c.Context(), annotation.EncodeRequest{
		Objects: []schemaext.Object{markObject("north")},
		Tables:  annotation.Tables{table},
		Slots: []annotation.SlotFacets{
			{Slot: slot, Facets: levels},
			{Slot: annotation.Slot{Directive: "ptah:schema:field", Table: table, Name: "id"}, Facets: must.Must(schemaext.NewFacets(&shade{Name: "teal"}))},
		},
		Coverage: coverage,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Directives, qt.DeepEquals, []annotation.EncodedDirective{written, marked})
	c.Assert(handed, qt.DeepEquals, []annotation.EncodeRequest{
		{Tables: annotation.Tables{table}, Slots: []annotation.SlotFacets{{Slot: slot, Facets: levels}}, Coverage: coverage},
		{Objects: []schemaext.Object{markObject("north")}, Tables: annotation.Tables{table}, Coverage: coverage.SelectKinds([]schemaext.Kind{markKind})},
	})
	c.Assert(set.Encodes(levelKind), qt.IsTrue)
	c.Assert(set.Encodes(markKind), qt.IsTrue)
	c.Assert(set.Encodes("example.org/paint/shade"), qt.IsFalse)
}

// TestSet_Encode_AsksAnOwnerWhoseModelsTheSchemaLacks pins that an encoder is
// asked for a schema holding none of its models, since an owner may refuse a
// coverage record it cannot write.
func TestSet_Encode_AsksAnOwnerWhoseModelsTheSchemaLacks(t *testing.T) {
	c := qt.New(t)
	var handed []annotation.EncodeRequest
	set := must.Must(annotation.NewSet(encoding("example.org/levels", "ptah:schema:level", []schemaext.Kind{levelKind}, &handed, annotation.EncodeResult{})))

	result, err := set.Encode(c.Context(), annotation.EncodeRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Directives, qt.HasLen, 0)
	c.Assert(handed, qt.HasLen, 1)
}

// TestSet_Encode_WritesAFrontendDirectiveTheOwnerReadsByScope pins that an
// owner may write one of the frontend's own directives where its targets hand
// the declaration back to the owner.
func TestSet_Encode_WritesAFrontendDirectiveTheOwnerReadsByScope(t *testing.T) {
	c := qt.New(t)
	var handed []annotation.EncodeRequest
	policy := annotation.EncodedDirective{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterSchemas, Targets: []string{"postgres"}}
	extension := encoding("example.org/levels", "ptah:schema:level", []schemaext.Kind{levelKind}, &handed,
		annotation.EncodeResult{Directives: []annotation.EncodedDirective{policy}})
	extension.TargetScopes = []annotation.TargetScope{{Directive: "ptah:schema:rls:policy", Targets: []string{"postgres"}, Label: "PostgreSQL"}}
	set := must.Must(annotation.NewSet(extension))

	result, err := set.Encode(c.Context(), annotation.EncodeRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Directives, qt.DeepEquals, []annotation.EncodedDirective{policy})
}

func TestSet_Encode_FailurePath(t *testing.T) {
	table := annotation.Table{Name: "rooms", Struct: "Room"}
	other := annotation.Table{Name: "halls", Struct: "Hall"}
	directive := func(name string, placement annotation.Placement, at *annotation.Table, attributes ...annotation.EncodedAttribute) annotation.EncodedDirective {
		return annotation.EncodedDirective{Directive: name, Placement: placement, Table: at, Attributes: attributes}
	}
	scoped := annotation.EncodedDirective{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterSchemas, Targets: []string{"mysql"}}
	mixed := annotation.EncodedDirective{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterSchemas, Targets: []string{"postgres", "mysql"}}
	tests := []struct {
		name      string
		directive annotation.EncodedDirective
		wantErr   string
	}{
		{name: "a directive without a name", directive: directive(" ", annotation.AfterSchemas, nil),
			wantErr: `.*encoder of example.org/levels wrote a directive without a name`},
		{name: "another owner's directive", directive: directive("ptah:schema:mark", annotation.AfterSchemas, nil),
			wantErr: `.*wrote directive "ptah:schema:mark", which reading the source back does not hand to it`},
		{name: "a directive nobody reads", directive: directive("ptah:schema:nobody", annotation.AfterSchemas, nil),
			wantErr: `.*wrote directive "ptah:schema:nobody", which reading the source back does not hand to it`},
		{name: "a frontend directive scoped to targets the owner does not read", directive: scoped,
			wantErr: `.*wrote directive "ptah:schema:rls:policy", which reading the source back does not hand to it`},
		{name: "a frontend directive scoped to targets of two owners", directive: mixed,
			wantErr: `.*wrote directive "ptah:schema:rls:policy": .*mixes PostgreSQL with others.*`},
		{name: "an unknown placement", directive: directive("ptah:schema:level", "nowhere", nil),
			wantErr: `.*placed directive "ptah:schema:level" at unknown placement "nowhere"`},
		{name: "a table directive without a table", directive: directive("ptah:schema:level", annotation.AfterTable, nil),
			wantErr: `.*placed directive "ptah:schema:level" at a table the schema does not declare`},
		{name: "a table the schema does not declare", directive: directive("ptah:schema:level", annotation.AfterTable, &other),
			wantErr: `.*placed directive "ptah:schema:level" at a table the schema does not declare`},
		{name: "a table on a directive of the standalone objects", directive: directive("ptah:schema:level", annotation.AfterPrivileges, &table),
			wantErr: `.*placed directive "ptah:schema:level" at after-privileges with a table`},
		{name: "an attribute without a name", directive: directive("ptah:schema:level", annotation.AfterSchemas, nil, annotation.EncodedAttribute{Name: ""}),
			wantErr: `.*wrote directive "ptah:schema:level" with attribute "" unnamed, twice or as its targets`},
		{name: "an attribute written twice", directive: directive("ptah:schema:level", annotation.AfterSchemas, nil,
			annotation.EncodedAttribute{Name: "value", Value: "a"}, annotation.EncodedAttribute{Name: "value", Value: "b"}),
			wantErr: `.*wrote directive "ptah:schema:level" with attribute "value" unnamed, twice or as its targets`},
		{name: "the dialects attribute", directive: directive("ptah:schema:level", annotation.AfterSchemas, nil, annotation.EncodedAttribute{Name: "dialects", Value: "postgres"}),
			wantErr: `.*wrote directive "ptah:schema:level" with attribute "dialects" unnamed, twice or as its targets`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var handed []annotation.EncodeRequest
			levels := encoding("example.org/levels", "ptah:schema:level", []schemaext.Kind{levelKind}, &handed,
				annotation.EncodeResult{Directives: []annotation.EncodedDirective{test.directive}})
			levels.TargetScopes = []annotation.TargetScope{{Directive: "ptah:schema:rls:policy", Targets: []string{"postgres"}, Label: "PostgreSQL"}}
			marks := encoding("example.org/marks", "ptah:schema:mark", []schemaext.Kind{markKind}, &handed, annotation.EncodeResult{})
			set := must.Must(annotation.NewSet(levels, marks))

			result, err := set.Encode(c.Context(), annotation.EncodeRequest{Tables: annotation.Tables{table}})

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Directives, qt.IsNil)
		})
	}
}

// TestSet_Encode_ReturnsTheEncodersRefusal pins that an owner's refusal
// refuses the export as it is.
func TestSet_Encode_ReturnsTheEncodersRefusal(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("no spelling for this level")
	var handed []annotation.EncodeRequest
	extension := encoding("example.org/levels", "ptah:schema:level", []schemaext.Kind{levelKind}, &handed, annotation.EncodeResult{})
	extension.Encoder = recorder{handed: &handed, err: refused}
	set := must.Must(annotation.NewSet(extension))

	result, err := set.Encode(c.Context(), annotation.EncodeRequest{})

	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(result.Directives, qt.IsNil)
}

// TestSet_Encode_StopsOnACanceledContext pins that a canceled export returns
// the cancellation rather than what the owners wrote.
func TestSet_Encode_StopsOnACanceledContext(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	var handed []annotation.EncodeRequest
	written := annotation.EncodedDirective{Directive: "ptah:schema:level", Placement: annotation.AfterSchemas}
	set := must.Must(annotation.NewSet(encoding("example.org/levels", "ptah:schema:level", []schemaext.Kind{levelKind}, &handed,
		annotation.EncodeResult{Directives: []annotation.EncodedDirective{written}})))

	result, err := set.Encode(ctx, annotation.EncodeRequest{})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.Directives, qt.IsNil)
}

func TestNewSet_Encoder_FailurePath(t *testing.T) {
	var handed []annotation.EncodeRequest
	withoutModels := encoding("example.org/levels", "ptah:schema:level", []schemaext.Kind{levelKind}, &handed, annotation.EncodeResult{})
	withoutModels.Encodes = nil
	withoutEncoder := widget("example.org/levels", "ptah:schema:level")
	withoutEncoder.Encodes = []schemaext.Kind{levelKind}
	undeclared := encoding("example.org/levels", "ptah:schema:level", []schemaext.Kind{levelKind}, &handed, annotation.EncodeResult{})
	undeclared.Encodes = []schemaext.Kind{markKind}
	tests := []struct {
		name      string
		extension annotation.Extension
		wantErr   string
	}{
		{name: "an encoder without models", extension: withoutModels,
			wantErr: `annotation extension of example.org/levels declares an encoder without models to encode, or models without an encoder`},
		{name: "models without an encoder", extension: withoutEncoder,
			wantErr: `annotation extension of example.org/levels declares an encoder without models to encode, or models without an encoder`},
		{name: "a model the extension does not declare", extension: undeclared,
			wantErr: `annotation extension of example.org/levels encodes model "example.org/widget/mark", which it does not declare`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := annotation.NewSet(test.extension)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

// TestNewSet_KeepsItsOwnEncodedModels pins that a set does not share the
// caller's list of encoded models: changing it after registration changes
// nothing the set encodes.
func TestNewSet_KeepsItsOwnEncodedModels(t *testing.T) {
	c := qt.New(t)
	var handed []annotation.EncodeRequest
	extension := encoding("example.org/levels", "ptah:schema:level", []schemaext.Kind{levelKind}, &handed, annotation.EncodeResult{})
	extension.Encodes = []schemaext.Kind{levelKind}
	set := must.Must(annotation.NewSet(extension))

	extension.Encodes[0] = markKind

	c.Assert(set.Encodes(levelKind), qt.IsTrue)
	c.Assert(set.Encodes(markKind), qt.IsFalse)
}

func TestTable_QualifiedName(t *testing.T) {
	tests := []struct {
		name  string
		table annotation.Table
		want  string
	}{
		{name: "a table without a schema", table: annotation.Table{Name: "rooms"}, want: "rooms"},
		{name: "a table in a schema", table: annotation.Table{Schema: "app", Name: "rooms"}, want: "app.rooms"},
		{name: "a name holding a dot", table: annotation.Table{Schema: "app", Name: "rooms.v1"}, want: `app."rooms.v1"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := test.table.QualifiedName()
			schema, name := annotation.QualifiedName("", got)

			c.Assert(got, qt.Equals, test.want)
			c.Assert([]string{schema, name}, qt.DeepEquals, []string{test.table.Schema, test.table.Name})
		})
	}
}
