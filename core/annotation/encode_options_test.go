package annotation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
)

var rooms = annotation.Table{Name: "rooms", Struct: "Room"}

// optioned is a levels extension whose attributes of the index directive
// spell options and read the index's type, and whose encoder writes result.
func optioned(owner string, handed *[]annotation.EncodeRequest, result annotation.EncodeResult) annotation.Extension {
	extension := encoding(owner, "ptah:schema:"+owner, []schemaext.Kind{levelKind}, handed, result)
	extension.Attributes = []annotation.DirectiveAttributes{{Directive: "ptah:schema:index",
		Attributes: []annotation.Attribute{{Name: "fill"}}, Reads: []string{"type"},
		Decode: func(map[string]string) (schemaext.Facets, error) { return schemaext.Facets{}, nil },
		Parameters: func(values map[string]string) (map[string]string, error) {
			return map[string]string{"fill": values["fill"]}, nil
		}}}
	return extension
}

// TestSet_Encode_HandsOptionsToTheOwnersThatReadThem pins that a
// declaration's options and the attributes of its own an owner reads go to
// an owner whose attributes of the directive spell options, even where the
// declaration holds none of the owner's facets, and to no other owner. Every
// owner is handed every field.
func TestSet_Encode_HandsOptionsToTheOwnersThatReadThem(t *testing.T) {
	c := qt.New(t)
	var reading, other []annotation.EncodeRequest
	index := annotation.Slot{Directive: "ptah:schema:index", Table: rooms, Name: "by_size"}
	field := annotation.FieldDeclaration{Slot: annotation.Slot{Directive: "ptah:schema:field", Table: rooms, Name: "size"}, Type: "Uint64", Key: true}
	set := must.Must(annotation.NewSet(
		optioned("example.org/levels", &reading, annotation.EncodeResult{}),
		encoding("example.org/marks", "ptah:schema:mark", []schemaext.Kind{markKind}, &other, annotation.EncodeResult{}),
	))

	_, err := set.Encode(c.Context(), annotation.EncodeRequest{Tables: annotation.Tables{rooms}, Fields: []annotation.FieldDeclaration{field},
		Slots: []annotation.SlotFacets{{Slot: index, Options: map[string]string{"fill": "70"}, Reads: map[string]string{"type": "btree", "name": "by_size"}}}})

	c.Assert(err, qt.IsNil)
	c.Assert(reading, qt.DeepEquals, []annotation.EncodeRequest{{Tables: annotation.Tables{rooms}, Fields: []annotation.FieldDeclaration{field},
		Slots: []annotation.SlotFacets{{Slot: index, Options: map[string]string{"fill": "70"}, Reads: map[string]string{"type": "btree"}}}}})
	c.Assert(other, qt.DeepEquals, []annotation.EncodeRequest{{Tables: annotation.Tables{rooms}, Fields: []annotation.FieldDeclaration{field}}})
}

// TestSet_Encode_ReturnsClaimsAndFieldTypes pins that an owner's option
// claims and field types come back with the attributes they belong to.
func TestSet_Encode_ReturnsClaimsAndFieldTypes(t *testing.T) {
	c := qt.New(t)
	var handed []annotation.EncodeRequest
	index := annotation.Slot{Directive: "ptah:schema:index", Table: rooms, Name: "by_size"}
	field := annotation.Slot{Directive: "ptah:schema:field", Table: rooms, Name: "size"}
	written := annotation.EncodeResult{
		Attributes: []annotation.SlotAttributes{{Slot: index, Attributes: []annotation.EncodedAttribute{{Name: "fill", Value: "70"}}, Options: []string{"fill"}}},
		FieldTypes: []annotation.FieldType{{Slot: field, Name: "uint64"}},
	}
	set := must.Must(annotation.NewSet(optioned("example.org/levels", &handed, written)))

	result, err := set.Encode(c.Context(), annotation.EncodeRequest{Tables: annotation.Tables{rooms},
		Fields: []annotation.FieldDeclaration{{Slot: field, Type: "Uint64"}},
		Slots:  []annotation.SlotFacets{{Slot: index, Options: map[string]string{"fill": "70"}}}})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Attributes, qt.DeepEquals, written.Attributes)
	c.Assert(result.FieldTypes, qt.DeepEquals, written.FieldTypes)
}

func TestSet_Encode_ClaimsAndFieldTypes_FailurePath(t *testing.T) {
	index := annotation.Slot{Directive: "ptah:schema:index", Table: rooms, Name: "by_size"}
	field := annotation.Slot{Directive: "ptah:schema:field", Table: rooms, Name: "size"}
	stranger := annotation.Slot{Directive: "ptah:schema:field", Table: rooms, Name: "color"}
	claim := func(names ...string) annotation.EncodeResult {
		return annotation.EncodeResult{Attributes: []annotation.SlotAttributes{{Slot: index,
			Attributes: []annotation.EncodedAttribute{{Name: "fill", Value: "70"}}, Options: names}}}
	}
	tests := []struct {
		name          string
		first, second annotation.EncodeResult
		wantIs        error
		wantErr       string
	}{
		{name: "an option the declaration does not hold", first: claim("depth"), wantIs: schemaext.ErrInvalidValue,
			wantErr: `.*encoder of example.org/levels claimed option "depth" of index "by_size" of table "rooms", which it was not handed`},
		{name: "an option two owners claim", first: claim("fill"), second: annotation.EncodeResult{Attributes: []annotation.SlotAttributes{{Slot: index,
			Attributes: []annotation.EncodedAttribute{{Name: "depth", Value: "2"}}, Options: []string{"fill"}}}}, wantIs: schemaext.ErrDuplicate,
			wantErr: `.*encoders of example.org/levels and example.org/shades both claim option "fill" of index "by_size" of table "rooms"`},
		{name: "a field the request does not hold", first: annotation.EncodeResult{FieldTypes: []annotation.FieldType{{Slot: stranger, Name: "string"}}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*encoder of example.org/levels typed field "color" of table "rooms", which it was not handed, or gave it no type`},
		{name: "a field typed without a name", first: annotation.EncodeResult{FieldTypes: []annotation.FieldType{{Slot: field, Name: " "}}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*encoder of example.org/levels typed field "size" of table "rooms", which it was not handed, or gave it no type`},
		{name: "a field two owners type", first: annotation.EncodeResult{FieldTypes: []annotation.FieldType{{Slot: field, Name: "uint64"}}},
			second: annotation.EncodeResult{FieldTypes: []annotation.FieldType{{Slot: field, Name: "int64"}}}, wantIs: schemaext.ErrDuplicate,
			wantErr: `.*encoders of example.org/levels and example.org/shades both type field "size" of table "rooms"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var handed []annotation.EncodeRequest
			shades := optioned("example.org/shades", &handed, test.second)
			shades.Kinds, shades.Encodes = []schemaext.Kind{markKind}, []schemaext.Kind{markKind}
			shades.Attributes[0].Attributes = []annotation.Attribute{{Name: "depth"}}
			set := must.Must(annotation.NewSet(optioned("example.org/levels", &handed, test.first), shades))

			result, err := set.Encode(c.Context(), annotation.EncodeRequest{Tables: annotation.Tables{rooms},
				Fields: []annotation.FieldDeclaration{{Slot: field, Type: "Uint64"}},
				Slots:  []annotation.SlotFacets{{Slot: index, Options: map[string]string{"fill": "70"}}}})

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
		})
	}
}
