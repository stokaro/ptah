package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestKind_Valid(t *testing.T) {
	tests := []struct {
		kind schemaext.Kind
		want bool
	}{
		{kind: "example.org/feature", want: true},
		{kind: "example.org/feature/operation-v2", want: true},
		{kind: "example.org/Feature", want: true},
		{kind: ""}, {kind: "feature"}, {kind: "local/feature"},
		{kind: "example.org//feature"}, {kind: "example.org/../feature"},
		{kind: "example.org/feature\\value"}, {kind: "example.org/with space"},
		{kind: "example.org/with\u00a0space"}, {kind: "example.org/with\x00control"},
	}
	for _, test := range tests {
		t.Run(string(test.kind), func(t *testing.T) {
			qt.New(t).Assert(test.kind.Valid(), qt.Equals, test.want)
		})
	}
}
