package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// A full-text declaration crosses both human-authored formats and Go export
// without losing an explicitly disabled filter or an integer option.
func TestRender_FullTextIndexFromYAML(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse(t.Context(), builtintest.Runtime().YAML(), []byte(`
tables:
  docs:
    columns:
      id: {type: Uint64, primary: true}
      body: {type: text}
    indexes:
      ft:
        fields: [body]
        type: fulltext_relevance
        tokenizer: standard
        use_filter_lowercase: true
        use_filter_stopwords: false
        use_filter_ngram: true
        filter_ngram_min_length: 3
        filter_ngram_max_length: 4
`))
	c.Assert(err, qt.IsNil)
	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", string(files[0].Data))
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Indexes, qt.HasLen, 1)
	c.Assert(parsed.Indexes[0].StorageParams, qt.DeepEquals, map[string]string{
		"tokenizer": "standard", "use_filter_lowercase": "true", "use_filter_stopwords": "false",
		"use_filter_ngram": "true", "filter_ngram_min_length": "3", "filter_ngram_max_length": "4",
	})
	c.Assert(parsed.Indexes[0].Type, qt.Equals, "fulltext_relevance")
}

// TestRender_RefusesAnInvalidFullTextIndex pins that the export refuses a
// full-text index whose options YDB refuses, the one without any among them,
// rather than write an index nothing can read back.
func TestRender_RefusesAnInvalidFullTextIndex(t *testing.T) {
	for _, test := range []struct {
		name    string
		options map[string]string
		wantErr string
	}{
		{"a full-text index without options", nil,
			`index "by_body": a YDB full-text index requires analyzer settings in WITH`},
		{"a full-text option YDB does not know", map[string]string{"tokenizer": "standard", "stemmer": "porter"},
			`index "by_body": unknown YDB full-text index option "stemmer"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
				Fields: []schemamodel.Field{{StructName: "Doc", FieldName: "Body", Name: "body", Type: "String"}},
				Indexes: []schemamodel.Index{{StructName: "Doc", Name: "by_body", Fields: []string{"body"}, Type: "fulltext_plain",
					TableName: "docs", StorageParams: test.options}},
			}

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(files, qt.IsNil)
		})
	}
}
