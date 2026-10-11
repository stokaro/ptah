package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// optionsSchema is a table with one index of the given type and WITH options.
func optionsSchema(indexType string, options map[string]string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Doc", FieldName: "Body", Name: "body", Type: "TEXT"},
		},
		Indexes: []schemamodel.Index{{StructName: "Doc", Name: "docs_body", Fields: []string{"body"}, Type: indexType, StorageParams: options}},
	}
}

// TestRender_IndexOptions_FailurePath pins that an index's WITH options the
// index directive cannot write are refused rather than left out. Left out,
// the source applied back declares the index without them, and the
// comparator plans a rebuild that drops them, as its "storage parameter
// removed" case in the PostgreSQL index semantics tests shows.
func TestRender_IndexOptions_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		indexType string
		options   map[string]string
		wantErr   string
	}{
		{name: "a PostgreSQL fillfactor", options: map[string]string{"fillfactor": "70"},
			wantErr: `.*index "docs_body" has WITH options fillfactor, which a Go annotation cannot represent`},
		{name: "several options of a GIN index", indexType: "gin", options: map[string]string{"gin_pending_list_limit": "128", "fastupdate": "off"},
			wantErr: `.*index "docs_body" has WITH options fastupdate, gin_pending_list_limit, which a Go annotation cannot represent`},
		{name: "options of a YDB vector index", indexType: "vector_kmeans_tree", options: map[string]string{"levels": "2"},
			wantErr: `.*index "docs_body" has WITH options levels, which a Go annotation cannot represent`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := goschematogo.Render(c.Context(), optionsSchema(test.indexType, test.options),
				goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(files, qt.IsNil)
		})
	}
}

// TestRender_IndexOptions_RefusesAFullTextOptionItCannotWrite pins that the
// options a YDB full-text index may carry are checked before they are
// written: the directive writes only the options it knows, so an unknown one
// would be left out rather than refused.
func TestRender_IndexOptions_RefusesAFullTextOptionItCannotWrite(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(c.Context(), optionsSchema("fulltext_plain", map[string]string{"tokenizer": "standard", "colour": "red"}),
		goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

	c.Assert(err, qt.ErrorMatches, `index "docs_body": unknown YDB full-text index option "colour"`)
	c.Assert(files, qt.IsNil)
}

// TestRender_IndexOptions_HappyPath is the control on the refusal: the
// options of a YDB full-text index, which the directive writes, and an index
// without options are still written, and read back the same.
func TestRender_IndexOptions_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		indexType string
		options   map[string]string
	}{
		{name: "a YDB full-text index", indexType: "fulltext_plain", options: map[string]string{"tokenizer": "standard", "use_filter_lowercase": "true"}},
		{name: "an index without options", indexType: "gin"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := goschematogo.Render(c.Context(), optionsSchema(test.indexType, test.options),
				goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})
			c.Assert(err, qt.IsNil)
			parsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", files[0].Data)

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Indexes, qt.HasLen, 1)
			c.Assert(parsed.Indexes[0].StorageParams, qt.DeepEquals, test.options)
		})
	}
}
