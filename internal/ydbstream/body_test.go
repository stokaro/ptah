package ydbstream_test

import (
	qt "github.com/frankban/quicktest"
	"ptah.run/internal/ydbstream"
	"testing"
)

func TestSameBody_CommentsDoNotResetState(t *testing.T) {
	for _, test := range []struct {
		name, left, right string
		same              bool
	}{
		{"trailing comment", "SELECT 1; /* changed */", "\nSELECT 1; \n", true},
		{"line comment", "SELECT -- note\n 1;", "SELECT 1;", true},
		{"literal whitespace", "SELECT 'a b';", "SELECT 'ab';", false},
		{"literal comment", "SELECT '/* a */';", "SELECT '/* b */';", false},
		{"raw literal", "SELECT @@-- a@@;", "SELECT @@-- b@@;", false},
		{"operator", "SELECT 1 + 2;", "SELECT 1 - 2;", false},
		{"literal suffix", "SELECT 'a'u;", "SELECT 'a';", false},
		{"identifier", "SELECT `a b`;", "SELECT `ab`;", false},
		{"statement", "SELECT 1;", "SELECT 2;", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbstream.SameBody(test.left, test.right), qt.Equals, test.same)
		})
	}
}
