package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/internal/atlashcl"
)

func TestOwnerTextList_PreservesOrderAndEvaluatesValues(t *testing.T) {
	tests := []struct {
		name, expression string
		want             []string
	}{
		{name: "literal identifiers", expression: `[last, "first", last]`, want: []string{"last", "first", "last"}},
		{name: "scalar", expression: `"last"`, want: []string{"last"}},
		{name: "variable list", expression: `var.words`, want: []string{"last", "first", "last"}},
		{name: "function", expression: `[upper("last"), "first"]`, want: []string{"LAST", "first"}},
		{name: "empty list", expression: `[]`, want: make([]string, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request
			set := must.Must(hclext.NewSet(hclext.Extension{Owner: "example.org/list", Decoder: shapeDecoder{requests: &requests},
				Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "words", Naming: hclext.Unlabeled,
					Attributes: []hclext.Attribute{{Name: "values", Kind: hclext.TextList}}}}}))
			text := "variable \"words\" {\n type = list(string)\n default = [\"last\", \"first\", \"last\"]\n}\nwords {\n values = " + test.expression + "\n}\n"
			_, err := atlashcl.ParseWithOptions([]byte(text), "words.hcl", atlashcl.Options{HCL: set})
			c.Assert(err, qt.IsNil)
			c.Assert(requests, qt.HasLen, 1)
			c.Assert(requests[0].Claims, qt.HasLen, 1)
			c.Assert(requests[0].Claims[0].Attributes["values"], qt.DeepEquals, hclext.Value{Kind: hclext.TextList, Texts: test.want})
		})
	}
}
