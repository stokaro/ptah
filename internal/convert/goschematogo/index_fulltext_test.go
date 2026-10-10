package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// A full-text declaration crosses both human-authored formats and Go export
// without losing an explicitly disabled filter or an integer option.
func TestRender_FullTextIndexFromYAML(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse([]byte(`
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
	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})
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
