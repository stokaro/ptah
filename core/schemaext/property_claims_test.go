package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestPropertyDefinition_Claims(t *testing.T) {
	definition := schemaext.PropertyDefinition{Kind: "example.com/feature", Keys: []string{"mode", "level"}, Prefixes: []string{"ttl"}}
	tests := []struct {
		key  string
		want bool
	}{
		{key: "mode", want: true},
		{key: "ttl", want: true},
		{key: "ttl_anything", want: true},
		{key: "TTL_ANYTHING", want: true},
		{key: "Mode", want: false},
		{key: "modes", want: false},
		{key: "attl", want: false},
	}
	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(definition.Claims(test.key), qt.Equals, test.want)
		})
	}
}
