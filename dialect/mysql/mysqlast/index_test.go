package mysqlast_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlast"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/modelast"
)

func replacement() *mysqlast.ReplaceIndex {
	return &mysqlast.ReplaceIndex{Index: mysqlast.Index{Name: "k", Unique: true, Type: "FULLTEXT", Parser: "ngram", KeyBlockSize: 8,
		Comment: "lookup", Invisible: true, Parts: []mysqlast.IndexPart{{Column: "a", Prefix: "7", Descending: true}, {Expression: "lower(b)"}}},
		TableCopy: true}
}

// The codec writes the operation's own fields, leaving out what is false,
// empty or zero, and decodes them back.
func TestCodecs_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name  string
		value *mysqlast.ReplaceIndex
		want  string
	}{
		{"every field", replacement(), `{"index":{"name":"k","unique":true,"type":"FULLTEXT","parts":[{"column":"a","prefix":"7","descending":true},` +
			`{"expression":"lower(b)"}],"parser":"ngram","key_block_size":8,"comment":"lookup","invisible":true},"table_copy":true}`},
		{"the least", &mysqlast.ReplaceIndex{Index: mysqlast.Index{Name: "k", Parts: []mysqlast.IndexPart{{Column: "a"}}}},
			`{"index":{"name":"k","parts":[{"column":"a"}]}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := mysqlast.Codecs()[0]

			encoded, err := codec.Encode(test.value)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want)
			c.Assert(must.Must(codec.Decode(encoded)), qt.DeepEquals, schemaext.Payload(test.value))
		})
	}
}

// A decoder accepts only what the encoder writes, and the operation needs a
// name and key parts that each name one column or expression.
func TestCodecs_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, data, wantErr string
	}{
		{"no index", `{"table_copy":true}`, `.*index.*`},
		{"a false spelled out", `{"index":{"name":"k","parts":[{"column":"a"}]},"table_copy":false}`, `.*table_copy.*`},
		{"an unknown key", `{"index":{"name":"k","parts":[{"column":"a"}],"using":"BTREE"}}`, `.*using.*`},
		{"an unknown part key", `{"index":{"name":"k","parts":[{"column":"a","length":7}]}}`, `.*length.*`},
		{"no name", `{"index":{"name":"","parts":[{"column":"a"}]}}`, `.*a MySQL index needs a name.*`},
		{"no part", `{"index":{"name":"k","parts":[]}}`, `.*has no key part.*`},
		{"a part naming both", `{"index":{"name":"k","parts":[{"column":"a","expression":"a+1"}]}}`, `.*names neither or both.*`},
		{"a hint above the limit", `{"index":{"name":"k","parts":[{"column":"a"}],"key_block_size":4294967296}}`, `.*exceeds the mysql limit.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := mysqlast.Codecs()[0].Decode(json.RawMessage(test.data))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// IndexFromModel reads a declared index as the common planner lowers it, so
// the owner's replacement writes the definition the common one would. The
// rows cover key parts from fields and from parts, and the owner's options.
func TestIndexFromModel_AgreesWithTheCommonLowering(t *testing.T) {
	options := must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndex{Parser: "ngram"}, &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8}))
	for _, test := range []struct {
		name  string
		index schemamodel.Index
	}{
		{"fields", schemamodel.Index{Name: "k", TableName: "t", Fields: []string{"a", "b"}, Unique: true, Comment: "c", Invisible: true}},
		{"parts", schemamodel.Index{Name: "k", TableName: "t", Fields: []string{"a"}, Type: "HASH",
			Parts: []schemamodel.IndexPart{{Name: "a", Prefix: "7", Desc: true}, {Expr: "lower(b)"}}}},
		{"options", schemamodel.Index{Name: "ft", TableName: "t", Fields: []string{"body"}, Type: "FULLTEXT", Facets: options}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			index, err := mysqlast.IndexFromModel(test.index)

			c.Assert(err, qt.IsNil)
			c.Assert(index, qt.DeepEquals, must.Must(mysqlast.IndexFromNode(modelast.FromIndex(test.index))))
		})
	}
}

// A facet of another owner on the index is refused rather than left out of
// the definition the replacement writes.
func TestIndexFromNode_FailurePath(t *testing.T) {
	c := qt.New(t)
	node := &ast.IndexNode{Name: "k", Columns: []string{"a"}, Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredTable{Engine: "InnoDB"}))}

	index, err := mysqlast.IndexFromNode(node)

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*MySQL index facet "ptah.run/mysql/table" is not supported`)
	c.Assert(index, qt.DeepEquals, mysqlast.Index{})
}

// The effect names the table copy when the replacement asks for one.
func TestReplaceIndex_Effect(t *testing.T) {
	c := qt.New(t)
	inPlace := replacement()
	inPlace.TableCopy = false

	c.Assert(replacement().Effect().Reason, qt.Contains, "ALGORITHM=COPY rebuilds the whole table")
	c.Assert(inPlace.Effect().Reason, qt.Not(qt.Contains), "ALGORITHM=COPY")
	c.Assert(replacement().SchemaChange(), qt.Equals, ast.ExtensionChange{Action: ast.ExtensionModify, Name: "k"})
}
