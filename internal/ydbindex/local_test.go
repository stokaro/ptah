package ydbindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbindex"
)

// YDB 26.2.1.14 aborts instead of rejecting an n-gram size of 9. Its
// constructor permits sizes 3 through 8 and at most 8 probability-derived
// hashes. Validate both before a render can send DDL to the server.
func TestResolveLocal_NgramBounds(t *testing.T) {
	for _, test := range []struct{ name, key, value, want string }{
		{name: "lower size", key: "ngram_size", value: "2", want: "ngram_size must be between 3 and 8"},
		{name: "upper size", key: "ngram_size", value: "9", want: "ngram_size must be between 3 and 8"},
		{name: "too many hashes", key: "false_positive_probability", value: "0.001", want: "false_positive_probability must produce at most 8 n-gram hashes.*"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.ResolveLocal(ydbindex.LocalNgram, map[string]string{test.key: test.value})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestResolveLocal_NgramAcceptedBounds(t *testing.T) {
	for _, test := range []struct{ key, value string }{{"ngram_size", "3"}, {"ngram_size", "8"}, {"false_positive_probability", "0.003"}} {
		t.Run(test.key+test.value, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.ResolveLocal(ydbindex.LocalNgram, map[string]string{test.key: test.value})
			c.Assert(err, qt.IsNil)
			c.Assert(got[test.key], qt.Equals, test.value)
		})
	}
}
