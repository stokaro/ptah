package ydbindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbindex"
)

func TestFullTextOptions(t *testing.T) {
	c := qt.New(t)
	options, err := ydbindex.ResolveFullText(map[string]string{
		"tokenizer": "standard", "use_filter_lowercase": "true", "filter_ngram_min_length": "03", "language": "en'glish",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(ydbindex.FullTextClause(options), qt.Equals,
		"WITH (filter_ngram_min_length=3, language='en\\'glish', tokenizer=standard, use_filter_lowercase=true)")
}

func TestFullTextOptionsRefuseUnknownOrMalformedValues(t *testing.T) {
	for _, test := range []struct {
		name    string
		options map[string]string
		want    string
	}{
		{name: "no settings", want: "a YDB full-text index requires analyzer settings in WITH"},
		{name: "newer analyzer syntax", options: map[string]string{"analyzer": "standard"}, want: `unknown YDB full-text index option "analyzer"`},
		{name: "missing tokenizer", options: map[string]string{"use_filter_lowercase": "true"}, want: "a YDB full-text index requires tokenizer"},
		{name: "missing ngram bounds", options: map[string]string{"tokenizer": "standard", "use_filter_ngram": "true"}, want: "a YDB n-gram filter requires filter_ngram_min_length"},
		{name: "unknown tokenizer", options: map[string]string{"tokenizer": "other"}, want: `YDB full-text index option tokenizer: unknown tokenizer "other"`},
		{name: "invalid boolean", options: map[string]string{"use_filter_lowercase": "yes"}, want: `YDB full-text index option use_filter_lowercase: "yes" is not true or false`},
		{name: "SQL in number", options: map[string]string{"filter_ngram_min_length": "3); DROP TABLE t"}, want: `YDB full-text index option filter_ngram_min_length: .* is not a nonnegative 32-bit integer`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := ydbindex.ResolveFullText(test.options)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

func TestFullTextEqual(t *testing.T) {
	for _, test := range []struct {
		name        string
		left, right map[string]string
		want        bool
	}{
		{name: "omitted false", left: map[string]string{"tokenizer": "standard"}, right: map[string]string{"tokenizer": "standard", "use_filter_lowercase": "false"}, want: true},
		{name: "enabled filter", left: map[string]string{"tokenizer": "standard"}, right: map[string]string{"tokenizer": "standard", "use_filter_lowercase": "true"}},
		{name: "unknown option", left: map[string]string{"unknown": "true"}, right: map[string]string{"unknown": "true"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbindex.FullTextEqual(test.left, test.right), qt.Equals, test.want)
		})
	}
}
