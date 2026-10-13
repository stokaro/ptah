package hclext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
)

// shapeBlock is a top-level block that declares a type, takes a type, a SQL
// default and a note, and nests `member` children that each take a type.
func shapeBlock() hclext.Block {
	return hclext.Block{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName, DeclaresType: true,
		Attributes: []hclext.Attribute{{Name: "base", Kind: hclext.TypeName}, {Name: "default", Kind: hclext.SQL}, {Name: "note", Kind: hclext.Text}},
		Children:   []hclext.Child{{Type: "member", Attributes: []hclext.Attribute{{Name: "kind", Kind: hclext.TypeName}, {Name: "note", Kind: hclext.String}}}}}
}

// TestNewSet_KeepsChildrenAndTypeDeclarations keeps a block's children and
// its type declaration, and copies the children's attributes, so a caller
// that changes its shape after NewSet does not change the set.
func TestNewSet_KeepsChildrenAndTypeDeclarations(t *testing.T) {
	c := qt.New(t)
	shape := shapeBlock()
	set := must.Must(hclext.NewSet(extension("example.org/a", "example.org/a/x", answeringDecoder{}, shape)))

	shape.Children[0].Attributes[0].Name = "changed"
	found, ok := set.Lookup(hclext.TopLevel, "shape")

	c.Assert(ok, qt.IsTrue)
	c.Assert(found.Block, qt.DeepEquals, shapeBlock())
}

// TestNewSet_RefusesChildShapes refuses children and a type declaration where
// the site does not take them, and a child the frontend could not read
// as its owner means it.
func TestNewSet_RefusesChildShapes(t *testing.T) {
	decoder := answeringDecoder{}
	child := func(attributes ...hclext.Attribute) hclext.Block {
		return hclext.Block{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{{Name: "note", Kind: hclext.Text}},
			Children:   []hclext.Child{{Type: "member", Attributes: attributes}}}
	}
	tests := []struct {
		name    string
		block   hclext.Block
		wantErr string
	}{
		{name: "children on index attributes", block: hclext.Block{Site: hclext.IndexAttributes, Type: "index", Children: []hclext.Child{{Type: "member"}}},
			wantErr: `HCL index-attributes block "index" of example.org/a declares a type outside top level or children outside top level and in-table sites`},
		{name: "a type declared in a table", block: hclext.Block{Site: hclext.InTable, Type: "shape", DeclaresType: true},
			wantErr: `HCL in-table block "shape" of example.org/a declares a type outside top level or children outside top level and in-table sites`},
		{name: "a type declared by index attributes", block: hclext.Block{Site: hclext.IndexAttributes, Type: "index", DeclaresType: true},
			wantErr: `HCL index-attributes block "index" of example.org/a declares a type outside top level or children outside top level and in-table sites`},
		{name: "a child without a type", block: hclext.Block{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName,
			Children: []hclext.Child{{Type: " "}}},
			wantErr: `HCL block "shape" of example.org/a declares a child without a type`},
		{name: "a child twice", block: hclext.Block{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName,
			Children: []hclext.Child{{Type: "member"}, {Type: "member"}}},
			wantErr: `duplicate.*: HCL block "shape" of example.org/a declares child "member" twice or under an attribute's name`},
		{name: "a child under an attribute's name", block: hclext.Block{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{{Name: "member", Kind: hclext.Text}}, Children: []hclext.Child{{Type: "member"}}},
			wantErr: `duplicate.*: HCL block "shape" of example.org/a declares child "member" twice or under an attribute's name`},
		{name: "a child under the naming's schema", block: hclext.Block{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName,
			Children: []hclext.Child{{Type: "schema"}}},
			wantErr: `duplicate.*: HCL block "shape" of example.org/a declares child "schema" twice or under an attribute's name`},
		{name: "a child's attribute without a name", block: child(hclext.Attribute{Name: " ", Kind: hclext.Text}),
			wantErr: `HCL block "shape member" of example.org/a declares an attribute without a name`},
		{name: "a child's attribute twice", block: child(hclext.Attribute{Name: "kind", Kind: hclext.Text}, hclext.Attribute{Name: "kind", Kind: hclext.String}),
			wantErr: `duplicate.*: HCL block "shape member" of example.org/a declares attribute "kind" twice`},
		{name: "a child's reference", block: child(hclext.Attribute{Name: "on", Kind: hclext.Ref, Refers: "table"}),
			wantErr: `HCL attribute "on" of example.org/a block "shape member" is a reference, which a child does not take`},
		{name: "a child's list of references", block: child(hclext.Attribute{Name: "to", Kind: hclext.Refs, Refers: "role"}),
			wantErr: `HCL attribute "to" of example.org/a block "shape member" is a references, which a child does not take`},
		{name: "a child's attribute of an unknown kind", block: child(hclext.Attribute{Name: "kind", Kind: "expression"}),
			wantErr: `HCL attribute "kind" of example.org/a block "shape member" names unknown kind "expression"`},
		{name: "a child's number", block: child(hclext.Attribute{Name: "size", Kind: hclext.Number}),
			wantErr: `HCL attribute "size" of example.org/a block "shape member" is a number, which only the writer takes; a reader reads digits as text`},
		{name: "a block's number", block: hclext.Block{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{{Name: "size", Kind: hclext.Number}}},
			wantErr: `HCL attribute "size" of example.org/a block "shape" is a number, which only the writer takes; a reader reads digits as text`},
		{name: "a type name that refers to something", block: hclext.Block{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{{Name: "base", Kind: hclext.TypeName, Refers: "table"}}},
			wantErr: `HCL attribute "base" of example.org/a block "shape" is a type-name and refers to nothing`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := hclext.NewSet(extension("example.org/a", "example.org/a/x", decoder, test.block))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

// TestSet_Encode_KeepsChildrenInTheOwnersOrder passes a block's children
// through as the owner wrote them, in its order rather than sorted, at the
// placement among the types.
func TestSet_Encode_KeepsChildrenInTheOwnersOrder(t *testing.T) {
	c := qt.New(t)
	sql := func(text string) []hclext.EncodedAttribute {
		return []hclext.EncodedAttribute{{Name: "kind", Value: hclext.Value{Kind: hclext.SQL, Text: text}}}
	}
	written := hclext.EncodedBlock{Type: "shape", Placement: hclext.UserTypes, Name: "s", Children: []hclext.EncodedChild{
		{Type: "member", Name: "zeta", Attributes: sql("int4")}, {Type: "member", Name: "alpha", Attributes: sql("text")}, {Type: "member", Name: "mid"}}}
	set := must.Must(hclext.NewSet(encodingExtension("example.org/a", "example.org/a/x",
		answeringEncoder{result: hclext.EncodeResult{Blocks: []hclext.EncodedBlock{written}}})))

	encoded, err := set.Encode(t.Context(), hclext.EncodeRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(encoded.Blocks, qt.DeepEquals, []hclext.EncodedBlock{written})
}

// TestSet_Encode_RefusesChildrenTheWriterCannotWrite refuses children of a
// child with conflicting label settings, a child the writer could not label, one written twice
// for one block, and a child's attribute refused as a block's is. A type is
// written as SQL: the reader's TypeName is refused on a block and a child.
func TestSet_Encode_RefusesChildrenTheWriterCannotWrite(t *testing.T) {
	text := func(name string) hclext.EncodedAttribute {
		return hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.String, Text: "t"}}
	}
	shape := func(children ...hclext.EncodedChild) hclext.EncodedBlock {
		return hclext.EncodedBlock{Type: "shape", Placement: hclext.UserTypes, Name: "s", Attributes: []hclext.EncodedAttribute{text("note")},
			Children: children}
	}
	tests := []struct {
		name    string
		block   hclext.EncodedBlock
		wantErr string
	}{
		{name: "unlabeled child with a name", block: shape(hclext.EncodedChild{Type: "member", Name: "a", Unlabeled: true}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote unlabeled child "member" with name "a"`},
		{name: "a child without a type", block: shape(hclext.EncodedChild{Type: " ", Name: "a"}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote block "shape" with child " " "a" without a type or a name, or under an attribute's name`},
		{name: "a child without a name", block: shape(hclext.EncodedChild{Type: "member"}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote block "shape" with child "member" "" without a type or a name, or under an attribute's name`},
		{name: "a child under an attribute's name", block: shape(hclext.EncodedChild{Type: "note", Name: "a"}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote block "shape" with child "note" "a" without a type or a name, or under an attribute's name`},
		{name: "a child twice", block: shape(hclext.EncodedChild{Type: "member", Name: "a"}, hclext.EncodedChild{Type: "member", Name: "a"}),
			wantErr: `duplicate.*: HCL encoder of example.org/a wrote member "a" of block "shape" twice`},
		{name: "a child's attribute without a name", block: shape(hclext.EncodedChild{Type: "member", Name: "a", Attributes: []hclext.EncodedAttribute{text(" ")}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote member "a" of block "shape" with attribute " " unnamed or twice`},
		{name: "a child's attribute twice", block: shape(hclext.EncodedChild{Type: "member", Name: "a",
			Attributes: []hclext.EncodedAttribute{text("kind"), text("kind")}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote member "a" of block "shape" with attribute "kind" unnamed or twice`},
		{name: "a child's type name", block: shape(hclext.EncodedChild{Type: "member", Name: "a",
			Attributes: []hclext.EncodedAttribute{{Name: "kind", Value: hclext.Value{Kind: hclext.TypeName, Text: "int4"}}}}),
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attribute "kind" of member "a" of block "shape" as "type-name", which the writer cannot write`},
		{name: "a block's type name", block: hclext.EncodedBlock{Type: "shape", Placement: hclext.UserTypes, Name: "s",
			Attributes: []hclext.EncodedAttribute{{Name: "base", Value: hclext.Value{Kind: hclext.TypeName, Text: "int4"}}}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote attribute "base" of block "shape" as "type-name", which the writer cannot write`},
		{name: "a type block in a table", block: hclext.EncodedBlock{Type: "shape", Placement: hclext.UserTypes, Name: "s", Table: &hclext.Table{Name: "t"}},
			wantErr: `invalid feature value: HCL encoder of example.org/a wrote top-level block "shape" with a table`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := must.Must(hclext.NewSet(encodingExtension("example.org/a", "example.org/a/x",
				answeringEncoder{result: hclext.EncodeResult{Blocks: []hclext.EncodedBlock{test.block}}})))

			encoded, err := set.Encode(t.Context(), hclext.EncodeRequest{})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(encoded, qt.DeepEquals, hclext.EncodeResult{})
		})
	}
}

// TestNewSet_TableChildrenSnapshot checks that child labels and reference
// metadata survive registration without retaining mutable caller slices.
func TestNewSet_TableChildrenSnapshot(t *testing.T) {
	c := qt.New(t)
	block := hclext.Block{Site: hclext.InTable, Type: "layout", Children: []hclext.Child{{Type: "by", Unlabeled: true,
		Attributes: []hclext.Attribute{{Name: "column", Kind: hclext.Ref, Refers: "column", Optional: true}}}}}
	set := must.Must(hclext.NewSet(extension("example.org/a", "example.org/a/x", answeringDecoder{}, block)))
	block.Children[0].Unlabeled = false
	block.Children[0].Attributes[0].Refers = "table"
	shape, ok := set.Lookup(hclext.InTable, "layout")
	c.Assert(ok, qt.IsTrue)
	c.Assert(shape.Block.Children[0].Unlabeled, qt.IsTrue)
	c.Assert(shape.Block.Children[0].Attributes[0], qt.DeepEquals, hclext.Attribute{Name: "column", Kind: hclext.Ref, Refers: "column", Optional: true})
}

func TestSet_Encode_RefusesQualifiedColumnReferences(t *testing.T) {
	for _, value := range []hclext.Value{
		{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "column", Schema: "app", Name: "x"}},
		{Kind: hclext.Refs, References: []hclext.Reference{{Kind: "column", Schema: "app", Name: "x"}}},
	} {
		t.Run(string(value.Kind), func(t *testing.T) {
			c := qt.New(t)
			block := hclext.EncodedBlock{Type: "layout", Placement: hclext.WithinTable, Table: &hclext.Table{Name: "t"},
				Attributes: []hclext.EncodedAttribute{{Name: "columns", Value: value}}}
			set := must.Must(hclext.NewSet(encodingExtension("example.org/a", "example.org/a/x",
				answeringEncoder{result: hclext.EncodeResult{Blocks: []hclext.EncodedBlock{block}}})))
			result, err := set.Encode(t.Context(), hclext.EncodeRequest{})
			c.Assert(err, qt.ErrorMatches, `invalid feature value: .*which the writer cannot write`)
			c.Assert(result, qt.DeepEquals, hclext.EncodeResult{})
		})
	}
}
