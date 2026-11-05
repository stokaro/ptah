package annotation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
)

// treeWidget reads the index declarations of type "tree" through its file
// decoder, which files receives.
func treeWidget(owner string, files *[]*recordingFile) annotation.Extension {
	extension := perFileWidget(files, nil, perFile("ptah:schema:index", "fanout"))
	extension.Owner = owner
	extension.ReadsIndex = func(indexType string) bool { return indexType == "tree" }
	return extension
}

// An owner that reads an index type reads the whole declaration of it, every
// attribute included; a declaration of another type stays the frontend's.
func TestReader_DecodeIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	var files []*recordingFile
	set := must.Must(annotation.NewSet(treeWidget("example.org/widget", &files)))
	declaration := annotation.Declaration{Directive: "ptah:schema:index", Struct: "Record",
		Subject:    annotation.Subject{Kind: annotation.SubjectIndex, Struct: "Record", Name: "by_id"},
		Attributes: map[string]string{"name": "by_id", "fields": "id", "type": "tree", "fanout": "4"}}

	owner, owned, err := set.IndexOwner("tree")
	other, otherOwned, otherErr := set.IndexOwner("btree")
	contributions, decodeErr := set.Reader().DecodeIndex(declaration)

	c.Assert(err, qt.IsNil)
	c.Assert(owned, qt.IsTrue)
	c.Assert(owner, qt.Equals, "example.org/widget")
	c.Assert(otherErr, qt.IsNil)
	c.Assert(otherOwned, qt.IsFalse)
	c.Assert(other, qt.Equals, "")
	c.Assert(decodeErr, qt.IsNil)
	c.Assert(contributions, qt.HasLen, 0)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].decoded, qt.DeepEquals, []annotation.Declaration{declaration})
}

func TestReader_DecodeIndex_FailurePath(t *testing.T) {
	t.Run("a type no owner reads", func(t *testing.T) {
		c := qt.New(t)
		var files []*recordingFile
		set := must.Must(annotation.NewSet(treeWidget("example.org/widget", &files)))

		contributions, err := set.Reader().DecodeIndex(annotation.Declaration{Directive: "ptah:schema:index", Attributes: map[string]string{"type": "btree"}})

		c.Assert(err, qt.ErrorMatches, `no selected owner reads an index of type "btree"`)
		c.Assert(contributions, qt.IsNil)
		c.Assert(files, qt.HasLen, 0)
	})
	t.Run("a type two owners read", func(t *testing.T) {
		c := qt.New(t)
		var files []*recordingFile
		other := treeWidget("example.org/other", &files)
		other.Directives[0].Name, other.Kinds, other.Attributes = "ptah:schema:other", []schemaext.Kind{markKind}, nil
		set := must.Must(annotation.NewSet(treeWidget("example.org/widget", &files), other))

		owner, owned, err := set.IndexOwner("tree")

		c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
		c.Assert(err, qt.ErrorMatches, `.*an index of type "tree" is read by example.org/widget and example.org/other`)
		c.Assert(owned, qt.IsFalse)
		c.Assert(owner, qt.Equals, "")
	})
}

// An owner reads index declarations through its file decoder alone.
func TestNewSet_RefusesAnIndexReaderWithoutAFileDecoder(t *testing.T) {
	c := qt.New(t)
	extension := widget("example.org/widget", "ptah:schema:widget")
	extension.ReadsIndex = func(string) bool { return true }

	set, err := annotation.NewSet(extension)

	c.Assert(err, qt.ErrorMatches, `annotation extension of example.org/widget reads index declarations and has no file decoder`)
	c.Assert(set.Selected(), qt.IsFalse)
}

// treeEncoding is the tree widget with an encoder that writes indexes.
func treeEncoding(handed *[]annotation.EncodeRequest, indexes ...annotation.EncodedIndex) annotation.Extension {
	var files []*recordingFile
	extension := treeWidget("example.org/widget", &files)
	extension.Encoder = recorder{handed: handed, result: annotation.EncodeResult{Indexes: indexes}}
	extension.Encodes = extension.Kinds
	return extension
}

func TestSet_Encode_WritesAnOwnersIndex(t *testing.T) {
	c := qt.New(t)
	table := annotation.Table{Name: "records", Struct: "Record"}
	index := annotation.EncodedIndex{Table: table, Name: "by_id", Type: "tree", Columns: []string{"id"},
		Attributes: []annotation.EncodedAttribute{{Name: "fanout", Value: "4"}}}
	var handed []annotation.EncodeRequest
	set := must.Must(annotation.NewSet(treeEncoding(&handed, index)))

	result, err := set.Encode(c.Context(), annotation.EncodeRequest{Tables: annotation.Tables{table}})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Indexes, qt.DeepEquals, []annotation.EncodedIndex{index})
}

func TestSet_Encode_RefusesAnOwnersIndex(t *testing.T) {
	table := annotation.Table{Name: "records", Struct: "Record"}
	tree := func(change func(*annotation.EncodedIndex)) []annotation.EncodedIndex {
		index := annotation.EncodedIndex{Table: table, Name: "by_id", Type: "tree", Columns: []string{"id"}}
		change(&index)
		return []annotation.EncodedIndex{index}
	}
	for _, test := range []struct {
		name    string
		indexes []annotation.EncodedIndex
		want    string
	}{
		{"a table the source does not write", tree(func(index *annotation.EncodedIndex) { index.Table = annotation.Table{Name: "halls"} }),
			`.*wrote index "by_id" of type "tree" at a table the source does not write, without a column, twice or of a type it does not read`},
		{"no column", tree(func(index *annotation.EncodedIndex) { index.Columns = nil }),
			`.*wrote index "by_id" of type "tree" at a table the source does not write, without a column, twice or of a type it does not read`},
		{"a type the owner does not read", tree(func(index *annotation.EncodedIndex) { index.Type = "btree" }),
			`.*wrote index "by_id" of type "btree" at a table the source does not write, without a column, twice or of a type it does not read`},
		{"one index twice", append(tree(func(*annotation.EncodedIndex) {}), tree(func(*annotation.EncodedIndex) {})...),
			`.*wrote index "by_id" of type "tree" at a table the source does not write, without a column, twice or of a type it does not read`},
		{"an attribute the owner does not add", tree(func(index *annotation.EncodedIndex) {
			index.Attributes = []annotation.EncodedAttribute{{Name: "depth", Value: "2"}}
		}), `.*wrote attribute "depth" of index "by_id" unnamed, twice or as one it does not add`},
		{"an attribute twice", tree(func(index *annotation.EncodedIndex) {
			index.Attributes = []annotation.EncodedAttribute{{Name: "fanout", Value: "2"}, {Name: "fanout", Value: "3"}}
		}), `.*wrote attribute "fanout" of index "by_id" unnamed, twice or as one it does not add`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var handed []annotation.EncodeRequest
			set := must.Must(annotation.NewSet(treeEncoding(&handed, test.indexes...)))

			result, err := set.Encode(c.Context(), annotation.EncodeRequest{Tables: annotation.Tables{table}})

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result.Indexes, qt.IsNil)
		})
	}
}
