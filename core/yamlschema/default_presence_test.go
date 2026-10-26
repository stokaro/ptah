package yamlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
)

func TestParsePreservesColumnDefaultPresence(t *testing.T) {
	for _, test := range []struct {
		name, source, value string
		present             bool
	}{
		{name: "absent"},
		{name: "empty literal", source: ", default: ''", present: true},
		{name: "null", source: ", default: null"},
		{name: "literal", source: ", default: text", value: "text", present: true},
		{name: "zero", source: ", default: 0", value: "0", present: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			model, err := yamlschema.Parse(t.Context(), noOwners, []byte("tables:\n  records:\n    columns:\n      label: {type: TEXT"+test.source+"}\n"))
			c.Assert(err, qt.IsNil)
			c.Assert(model.Fields, qt.HasLen, 1)
			c.Assert(model.Fields[0].Default, qt.Equals, test.value)
			c.Assert(model.Fields[0].DefaultSet, qt.Equals, test.present)
		})
	}
}

func TestEmptyYAMLDefaultReachesRenderer(t *testing.T) {
	c := qt.New(t)
	model, err := yamlschema.Parse(t.Context(), noOwners, []byte("tables:\n  records:\n    columns:\n      label: {type: TEXT, default: ''}\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(renderStatements(c, model, "sqlite"), "\n"), qt.Contains, "DEFAULT ''")
}
