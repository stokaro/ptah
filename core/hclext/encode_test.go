package hclext_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
)

// namingEncoder writes one block of type kind for each object, named after
// the object, and records the objects and the requests it is given.
type namingEncoder struct {
	block    string
	objects  *[]string
	requests *[]hclext.EncodeRequest
}

func (e namingEncoder) EncodeHCL(_ context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if e.requests != nil {
		*e.requests = append(*e.requests, request)
	}
	blocks := make([]hclext.EncodedBlock, 0, len(request.Objects))
	var header []string
	for _, object := range request.Objects {
		*e.objects = append(*e.objects, object.Ref.Name.Source)
		blocks = append(blocks, hclext.EncodedBlock{Type: e.block, Placement: hclext.AfterTables, Name: object.Ref.Name.Source,
			Object: object.Ref, Attributes: []hclext.EncodedAttribute{{Name: "size", Value: hclext.Value{Kind: hclext.String, Text: "s"}}}})
		header = append(header, e.block+" "+object.Ref.Name.Source)
	}
	return hclext.EncodeResult{Blocks: blocks, Header: header}, nil
}

// answeringEncoder answers every request with result and err.
type answeringEncoder struct {
	result hclext.EncodeResult
	err    error
}

func (e answeringEncoder) EncodeHCL(context.Context, hclext.EncodeRequest) (hclext.EncodeResult, error) {
	return e.result, e.err
}

func encodingExtension(owner string, kind schemaext.Kind, encoder hclext.Encoder) hclext.Extension {
	return hclext.Extension{Owner: owner, Kinds: []schemaext.Kind{kind}, Encoder: encoder}
}

// TestSet_Encode_HappyPath hands each extension the objects of its own kinds
// in one call, and returns the blocks every extension writes. An extension
// with an encoder is asked whatever the document holds of its kinds, since it
// may refuse the coverage; one without an encoder is not.
func TestSet_Encode_HappyPath(t *testing.T) {
	c := qt.New(t)
	var first, second, third []string
	var empty []hclext.EncodeRequest
	set := must.Must(hclext.NewSet(
		encodingExtension("example.org/a", "example.org/a/x", namingEncoder{block: "gadget", objects: &first}),
		encodingExtension("example.org/b", "example.org/b/x", namingEncoder{block: "widget", objects: &second}),
		encodingExtension("example.org/c", "example.org/c/x", namingEncoder{block: "gizmo", objects: &third, requests: &empty}),
		hclext.Extension{Owner: "example.org/d", Kinds: []schemaext.Kind{"example.org/d/x"}},
	))
	objects := must.Must(must.Must(schemaext.NewObjects(object("example.org/a/x", "g1"), object("example.org/b/x", "w1"),
		object("example.org/a/x", "g2"), object("example.org/d/x", "d1"))).All())

	encoded, err := set.Encode(t.Context(), hclext.EncodeRequest{Objects: objects})

	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.DeepEquals, []string{"g1", "g2"})
	c.Assert(second, qt.DeepEquals, []string{"w1"})
	c.Assert(empty, qt.DeepEquals, []hclext.EncodeRequest{{}})
	c.Assert(encoded.Blocks, qt.HasLen, 3)
	c.Assert([]string{encoded.Blocks[0].Name, encoded.Blocks[1].Name, encoded.Blocks[2].Name}, qt.DeepEquals, []string{"g1", "g2", "w1"})
	c.Assert(encoded.Header, qt.DeepEquals, []string{"gadget g1", "gadget g2", "widget w1"})
	c.Assert(set.Encodes("example.org/a/x"), qt.IsTrue)
	c.Assert(set.Encodes("example.org/d/x"), qt.IsFalse)
	c.Assert(hclext.None().Encodes("example.org/a/x"), qt.IsFalse)
}

// TestSet_Encode_FailurePath refuses an encoder's failure as it is, and every
// block the writer could not write.
func TestSet_Encode_FailurePath(t *testing.T) {
	refused := errors.New("refused")
	written := func(block hclext.EncodedBlock) hclext.Encoder {
		return answeringEncoder{result: hclext.EncodeResult{Blocks: []hclext.EncodedBlock{block}}}
	}
	text := func(name string) hclext.EncodedAttribute {
		return hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.String, Text: "t"}}
	}
	tests := []struct {
		name    string
		encoder hclext.Encoder
		wantErr string
	}{
		{name: "a failure", encoder: answeringEncoder{err: refused}, wantErr: `refused`},
		{name: "no type", encoder: written(hclext.EncodedBlock{Placement: hclext.AfterTables, Name: "g"}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote a block without a type or a name`},
		{name: "no name", encoder: written(hclext.EncodedBlock{Type: "gadget", Placement: hclext.AfterTables}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote a block without a type or a name`},
		{name: "an unknown placement", encoder: written(hclext.EncodedBlock{Type: "gadget", Name: "g", Placement: "first"}),
			wantErr: `invalid feature value: HCL encoder of example.org/a placed block "gadget" at unknown placement "first"`},
		{name: "an object of another kind", encoder: written(hclext.EncodedBlock{Type: "gadget", Name: "g", Placement: hclext.AfterTables,
			Object: object("example.org/b/x", "w").Ref}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote an object of kind "example.org/b/x", which its extension does not declare`},
		{name: "an attribute without a name", encoder: written(hclext.EncodedBlock{Type: "gadget", Name: "g", Placement: hclext.AfterTables,
			Attributes: []hclext.EncodedAttribute{text(" ")}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote block "gadget" with attribute " " unnamed, twice or as its naming's own`},
		{name: "an attribute twice", encoder: written(hclext.EncodedBlock{Type: "gadget", Name: "g", Placement: hclext.AfterTables,
			Attributes: []hclext.EncodedAttribute{text("size"), text("size")}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote block "gadget" with attribute "size" unnamed, twice or as its naming's own`},
		{name: "the naming's own attribute", encoder: written(hclext.EncodedBlock{Type: "gadget", Name: "g", Placement: hclext.AfterTables,
			Attributes: []hclext.EncodedAttribute{text("schema")}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote block "gadget" with attribute "schema" unnamed, twice or as its naming's own`},
		{name: "an empty header line", encoder: answeringEncoder{result: hclext.EncodeResult{Header: []string{" "}}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote a header line that is empty or holds a line break: " "`},
		{name: "a header line with a line break", encoder: answeringEncoder{result: hclext.EncodeResult{Header: []string{"a\nb"}}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote a header line that is empty or holds a line break: "a\\nb"`},
		{name: "a block in a table without the table", encoder: written(hclext.EncodedBlock{Type: "switch", Placement: hclext.WithinTable}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote block "switch" in a table without the table or with a name`},
		{name: "a block in a table with a name", encoder: written(hclext.EncodedBlock{Type: "switch", Name: "s", Placement: hclext.WithinTable,
			Table: &hclext.Table{Name: "t"}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote block "switch" in a table without the table or with a name`},
		{name: "a top-level block with a table", encoder: written(hclext.EncodedBlock{Type: "rule", Name: "r", Placement: hclext.AfterTriggers,
			Table: &hclext.Table{Name: "t"}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote top-level block "rule" with a table`},
		{name: "a reference to a view", encoder: written(hclext.EncodedBlock{Type: "rule", Name: "r", Placement: hclext.AfterTriggers,
			Attributes: []hclext.EncodedAttribute{{Name: "on", Value: hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "view", Name: "v"}}}}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attribute "on" of block "rule" as "reference", which the writer cannot write`},
		{name: "a list element of another kind", encoder: written(hclext.EncodedBlock{Type: "rule", Name: "r", Placement: hclext.AfterTriggers,
			Attributes: []hclext.EncodedAttribute{{Name: "to", Value: hclext.Value{Kind: hclext.Refs, References: []hclext.Reference{{Kind: "view", Name: "v"}}}}}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attribute "to" of block "rule" as "references", which the writer cannot write`},
		{name: "a list element without a name", encoder: written(hclext.EncodedBlock{Type: "rule", Name: "r", Placement: hclext.AfterTriggers,
			Attributes: []hclext.EncodedAttribute{{Name: "to", Value: hclext.Value{Kind: hclext.Refs, References: []hclext.Reference{{Kind: "role"}}}}}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attribute "to" of block "rule" as "references", which the writer cannot write`},
		{name: "an unknown schema role", encoder: written(hclext.EncodedBlock{Type: "plugin", Name: "p", Placement: hclext.BeforeTypes, SchemaRole: "owner"}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote block "plugin" with schema role "owner"`},
		{name: "a kind the writer cannot write", encoder: written(hclext.EncodedBlock{Type: "gadget", Name: "g", Placement: hclext.AfterTables,
			Attributes: []hclext.EncodedAttribute{{Name: "on", Value: hclext.Value{Kind: hclext.Ref}}}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attribute "on" of block "gadget" as "reference", which the writer cannot write`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := must.Must(hclext.NewSet(encodingExtension("example.org/a", "example.org/a/x", test.encoder)))

			encoded, err := set.Encode(t.Context(), hclext.EncodeRequest{Objects: []schemaext.Object{object("example.org/a/x", "g")}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(encoded, qt.DeepEquals, hclext.EncodeResult{})
		})
	}
}

// TestNewSet_RefusesAnEncoderWithoutAModel refuses an encoder that no kind
// would ever reach.
func TestNewSet_RefusesAnEncoderWithoutAModel(t *testing.T) {
	c := qt.New(t)

	set, err := hclext.NewSet(hclext.Extension{Owner: "example.org/a", Encoder: answeringEncoder{}})

	c.Assert(err, qt.ErrorMatches, `HCL extension of example.org/a declares an encoder without a model to encode`)
	c.Assert(set.Selected(), qt.IsFalse)
}

// TestSet_Encode_HandsEachOwnerItsFacets hands each extension the tables that
// hold a facet of its kinds, with those facets alone.
func TestSet_Encode_HandsEachOwnerItsFacets(t *testing.T) {
	c := qt.New(t)
	var objects []string
	var requests []hclext.EncodeRequest
	set := must.Must(hclext.NewSet(encodingExtension("example.org/a", "example.org/a/x",
		namingEncoder{block: "gadget", objects: &objects, requests: &requests})))
	both := must.Must(schemaext.NewFacets(&widget{kind: "example.org/a/x"}, &widget{kind: "example.org/b/x"}))
	other := must.Must(schemaext.NewFacets(&widget{kind: "example.org/b/x"}))

	_, err := set.Encode(t.Context(), hclext.EncodeRequest{Tables: []hclext.TableFacets{
		{Table: hclext.Table{Name: "both"}, Facets: both}, {Table: hclext.Table{Name: "other"}, Facets: other}}})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Tables, qt.HasLen, 1)
	c.Assert(requests[0].Tables[0].Table, qt.Equals, hclext.Table{Name: "both"})
	c.Assert(requests[0].Tables[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{"example.org/a/x"})
}

// slotEncoder answers every request with the attributes and losses it is
// given, and records the requests.
type slotEncoder struct {
	slots    []hclext.SlotAttributes
	requests *[]hclext.EncodeRequest
}

func (e slotEncoder) EncodeHCL(_ context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if e.requests != nil {
		*e.requests = append(*e.requests, request)
	}
	return hclext.EncodeResult{Attributes: e.slots}, nil
}

// slotDocument is a table t with a column c, an index i and a primary key,
// each holding a facet of example.org/a and of example.org/b, and a column d
// holding one of example.org/b alone.
func slotDocument() hclext.EncodeRequest {
	both := must.Must(schemaext.NewFacets(&widget{kind: "example.org/a/x"}, &widget{kind: "example.org/b/x"}))
	other := must.Must(schemaext.NewFacets(&widget{kind: "example.org/b/x"}))
	table := hclext.Table{Schema: "app", Name: "t"}
	return hclext.EncodeRequest{
		Tables: []hclext.TableFacets{{Table: table, Facets: both}},
		Columns: []hclext.SlotFacets{{Slot: hclext.Slot{Kind: hclext.SlotColumn, Table: table, Name: "c"}, Facets: both},
			{Slot: hclext.Slot{Kind: hclext.SlotColumn, Table: table, Name: "d"}, Facets: other}},
		Indexes: []hclext.SlotFacets{{Slot: hclext.Slot{Kind: hclext.SlotIndex, Table: table, Name: "i"}, Facets: both}},
		Keys:    []hclext.SlotFacets{{Slot: hclext.Slot{Kind: hclext.SlotKey, Table: table, Primary: true}, Facets: both}},
	}
}

// TestSet_Encode_HandsEachOwnerItsSlots hands each extension the writer's
// blocks that hold a facet of its kinds, with those facets alone, and returns
// what every extension adds to them, extension by extension. The writer
// writes digits and any SQL text, line breaks included, since it quotes it.
func TestSet_Encode_HandsEachOwnerItsSlots(t *testing.T) {
	c := qt.New(t)
	document := slotDocument()
	column, index := document.Columns[0].Slot, document.Indexes[0].Slot
	first := hclext.SlotAttributes{Slot: column, Attributes: []hclext.EncodedAttribute{
		{Name: "on_update", Value: hclext.Value{Kind: hclext.SQL, Text: "a\n\"b\" ${c}"}}}, Losses: []string{"lost"}}
	second := hclext.SlotAttributes{Slot: column, Attributes: []hclext.EncodedAttribute{{Name: "auto_increment", Value: hclext.Value{Kind: hclext.Number, Text: "007"}}}}
	third := hclext.SlotAttributes{Slot: index, Losses: []string{"hint"}}
	var requests []hclext.EncodeRequest
	set := must.Must(hclext.NewSet(
		encodingExtension("example.org/a", "example.org/a/x", slotEncoder{slots: []hclext.SlotAttributes{first}, requests: &requests}),
		encodingExtension("example.org/b", "example.org/b/x", slotEncoder{slots: []hclext.SlotAttributes{second, third}})))

	encoded, err := set.Encode(t.Context(), document)

	c.Assert(err, qt.IsNil)
	c.Assert(encoded.Attributes, qt.DeepEquals, []hclext.SlotAttributes{first, second, third})
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Tables, qt.HasLen, 1)
	for _, slots := range [][]hclext.SlotFacets{requests[0].Columns, requests[0].Indexes, requests[0].Keys} {
		c.Assert(slots, qt.HasLen, 1)
		c.Assert(slots[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{"example.org/a/x"})
	}
	c.Assert(requests[0].Columns[0].Slot, qt.Equals, column)
	c.Assert(requests[0].Indexes[0].Slot, qt.Equals, index)
	c.Assert(requests[0].Keys[0].Slot, qt.Equals, document.Keys[0].Slot)
}

// TestSet_Encode_RefusesSlotAttributes_FailurePath refuses attributes for a
// block the extension was not handed or wrote twice, and an attribute the
// writer cannot write there.
func TestSet_Encode_RefusesSlotAttributes_FailurePath(t *testing.T) {
	table := hclext.Table{Schema: "app", Name: "t"}
	column := hclext.Slot{Kind: hclext.SlotColumn, Table: table, Name: "c"}
	text := hclext.EncodedAttribute{Name: "charset", Value: hclext.Value{Kind: hclext.String, Text: "t"}}
	number := func(digits string) []hclext.EncodedAttribute {
		return []hclext.EncodedAttribute{{Name: "auto_increment", Value: hclext.Value{Kind: hclext.Number, Text: digits}}}
	}
	tests := []struct {
		name    string
		slots   []hclext.SlotAttributes
		wantErr string
	}{
		{name: "a column it was not handed", slots: []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: hclext.SlotColumn, Table: table, Name: "d"}}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attributes for column "d" of table "app.t", which it was not handed or wrote twice`},
		{name: "a column twice", slots: []hclext.SlotAttributes{{Slot: column}, {Slot: column}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attributes for column "c" of table "app.t", which it was not handed or wrote twice`},
		{name: "a table with a name", slots: []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table, Name: "c"}}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attributes for table "app.t", which it was not handed or wrote twice`},
		{name: "a table it was not handed", slots: []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: hclext.Table{Name: "u"}}}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attributes for table "u", which it was not handed or wrote twice`},
		{name: "a constraint it was not handed", slots: []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: hclext.SlotKey, Table: table, Name: "k"}}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attributes for key "k" of table "app.t", which it was not handed or wrote twice`},
		{name: "a primary key it was not handed", slots: []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: hclext.SlotKey, Table: hclext.Table{Name: "u"}, Primary: true}}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attributes for the primary key of table "u", which it was not handed or wrote twice`},
		{name: "an unknown kind of block", slots: []hclext.SlotAttributes{{Slot: hclext.Slot{Kind: "view", Table: table, Name: "c"}}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attributes for view "c" of table "app.t", which it was not handed or wrote twice`},
		{name: "an attribute without a name", slots: []hclext.SlotAttributes{{Slot: column, Attributes: []hclext.EncodedAttribute{{Name: " ", Value: text.Value}}}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attribute " " of column "c" of table "app.t" unnamed or twice`},
		{name: "an attribute twice", slots: []hclext.SlotAttributes{{Slot: column, Attributes: []hclext.EncodedAttribute{text, text}}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attribute "charset" of column "c" of table "app.t" unnamed or twice`},
		{name: "a number with a sign", slots: []hclext.SlotAttributes{{Slot: column, Attributes: number("-1")}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attribute "auto_increment" of column "c" of table "app.t" as "number", which the writer cannot write`},
		{name: "a number with an exponent", slots: []hclext.SlotAttributes{{Slot: column, Attributes: number("1e3")}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attribute "auto_increment" of column "c" of table "app.t" as "number", which the writer cannot write`},
		{name: "an empty number", slots: []hclext.SlotAttributes{{Slot: column, Attributes: number("")}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attribute "auto_increment" of column "c" of table "app.t" as "number", which the writer cannot write`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := must.Must(hclext.NewSet(encodingExtension("example.org/a", "example.org/a/x", slotEncoder{slots: test.slots})))

			encoded, err := set.Encode(t.Context(), slotDocument())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(encoded, qt.DeepEquals, hclext.EncodeResult{})
		})
	}
}

// TestSet_Encode_RefusesAnAttributeTwoOwnersWrite refuses two extensions
// writing one attribute of one block, whichever registered first, while each
// may write its own attributes there.
func TestSet_Encode_RefusesAnAttributeTwoOwnersWrite(t *testing.T) {
	c := qt.New(t)
	column := slotDocument().Columns[0].Slot
	write := func(name string) hclext.Encoder {
		return slotEncoder{slots: []hclext.SlotAttributes{{Slot: column, Attributes: []hclext.EncodedAttribute{
			{Name: name, Value: hclext.Value{Kind: hclext.String, Text: "t"}}}}}}
	}
	set := must.Must(hclext.NewSet(encodingExtension("example.org/a", "example.org/a/x", write("charset")),
		encodingExtension("example.org/b", "example.org/b/x", write("charset"))))

	encoded, err := set.Encode(t.Context(), slotDocument())

	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(err, qt.ErrorMatches, `duplicate feature identity: HCL encoders of example.org/a and example.org/b both write attribute "charset" of column "c" of table "app.t"`)
	c.Assert(encoded, qt.DeepEquals, hclext.EncodeResult{})
}

// TestSet_Encode_KeepsEachOwnersAttributesOfOneBlock is the control: two
// extensions writing different attributes of one block are both returned.
func TestSet_Encode_KeepsEachOwnersAttributesOfOneBlock(t *testing.T) {
	c := qt.New(t)
	column := slotDocument().Columns[0].Slot
	write := func(name string) hclext.Encoder {
		return slotEncoder{slots: []hclext.SlotAttributes{{Slot: column, Attributes: []hclext.EncodedAttribute{
			{Name: name, Value: hclext.Value{Kind: hclext.String, Text: "t"}}}}}}
	}
	set := must.Must(hclext.NewSet(encodingExtension("example.org/a", "example.org/a/x", write("charset")),
		encodingExtension("example.org/b", "example.org/b/x", write("collation"))))

	encoded, err := set.Encode(t.Context(), slotDocument())

	c.Assert(err, qt.IsNil)
	c.Assert(encoded.Attributes, qt.HasLen, 2)
}
