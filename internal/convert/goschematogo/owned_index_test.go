package goschematogo_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/convert/goschematogo"
)

// indexWriter writes the indexes it was given.
type indexWriter []annotation.EncodedIndex

func (w indexWriter) EncodeAnnotations(context.Context, annotation.EncodeRequest) (annotation.EncodeResult, error) {
	return annotation.EncodeResult{Indexes: w}, nil
}

// silentFile declares nothing for any declaration.
type silentFile struct{}

func (silentFile) Decode(annotation.Declaration) ([]annotation.Contribution, error) { return nil, nil }
func (silentFile) Finish(annotation.Tables) ([]annotation.Contribution, error)      { return nil, nil }

// treeProvider is the paint owner reading the index declarations of type
// "tree" in place of the frontend, with a fanout attribute of the index
// directive, and writing indexes.
func treeProvider(indexes ...annotation.EncodedIndex) engine.Provider {
	painter := paintProvider("example.org/paint", indexWriter(indexes))
	extension := &painter.Annotations[0]
	extension.Decode, extension.Directives = nil, nil
	extension.File = func() annotation.FileDecoder { return silentFile{} }
	extension.ReadsIndex = func(indexType string) bool { return indexType == "tree" }
	extension.Attributes = []annotation.DirectiveAttributes{{Directive: "ptah:schema:index", PerFile: true,
		Attributes: []annotation.Attribute{{Name: "fanout", Description: "fanout.", Value: "string"}}}}
	return painter
}

// treeSchema is a table with an index of its own.
func treeSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables:  []schemamodel.Table{{StructName: "Room", Name: "rooms"}},
		Fields:  []schemamodel.Field{{StructName: "Room", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true}},
		Indexes: []schemamodel.Index{{StructName: "Room", Name: "plain", Fields: []string{"id"}}},
	}
}

// TestRender_WritesAnOwnersIndex writes an index an owner writes whole as an
// index directive of its table's struct, after the table's own indexes, with
// the frontend's attributes and then the owner's.
func TestRender_WritesAnOwnersIndex(t *testing.T) {
	c := qt.New(t)
	table := annotation.Table{Name: "rooms", Struct: "Room"}
	runtime := must.Must(engine.New(treeProvider(annotation.EncodedIndex{Table: table, Name: "by_id", Type: "tree", Columns: []string{"id"},
		Include: []string{"id"}, Unique: true, Comment: "ids", Ops: "int_ops", Attributes: []annotation.EncodedAttribute{{Name: "fanout", Value: "4"}}})))

	files, err := goschematogo.Render(c.Context(), treeSchema(), goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Contains, "\t//ptah:schema:index name=\"plain\" fields=\"id\"\n\t_ struct{}\n"+
		"\t//ptah:schema:index name=\"by_id\" fields=\"id\" include=\"id\" unique=\"true\" type=\"tree\" ops=\"int_ops\" comment=\"ids\" fanout=\"4\"\n\t_ struct{}\n}\n")
}

// TestRender_RefusesAnOwnersIndexOfATakenName refuses an index an owner
// writes under the name of one of the table's own: read back, the source
// would declare one index twice.
func TestRender_RefusesAnOwnersIndexOfATakenName(t *testing.T) {
	c := qt.New(t)
	table := annotation.Table{Name: "rooms", Struct: "Room"}
	runtime := must.Must(engine.New(treeProvider(annotation.EncodedIndex{Table: table, Name: "plain", Type: "tree", Columns: []string{"id"}})))

	files, err := goschematogo.Render(c.Context(), treeSchema(), goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})

	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(err, qt.ErrorMatches, `.*an owner wrote index "plain" of table "rooms", which the table declares already`)
	c.Assert(files, qt.IsNil)
}
