package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

// TestParse_KeepsTheGeneratedKindOfAColumn reads the generated kind each
// column declares. A generated_kind stands without generated, as ClickHouse's
// EPHEMERAL does, and the render decides whether the target takes it; stored
// without generated declares nothing.
func TestParse_KeepsTheGeneratedKindOfAColumn(t *testing.T) {
	tests := []struct {
		name, column   string
		kind, computed string
	}{
		{name: "a kind with an expression", column: "{type: Int64, generated: a + 1, generated_kind: alias}", kind: "ALIAS", computed: "a + 1"},
		{name: "stored with an expression", column: "{type: Int64, generated: a + 1, stored: true}", kind: "STORED", computed: "a + 1"},
		{name: "EPHEMERAL alone", column: "{type: Int64, generated_kind: ephemeral}", kind: "EPHEMERAL", computed: ""},
		{name: "STORED alone", column: "{type: Int64, generated_kind: STORED}", kind: "STORED", computed: ""},
		{name: "stored alone", column: "{type: Int64, stored: true}", kind: "", computed: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(t.Context(), yamlext.None(), []byte("tables:\n  t:\n    columns:\n      c: "+test.column+"\n"))

			c.Assert(err, qt.IsNil)
			c.Assert(db.Fields, qt.HasLen, 1)
			c.Assert(db.Fields[0].GeneratedKind, qt.Equals, test.kind)
			c.Assert(db.Fields[0].GeneratedExpression, qt.Equals, test.computed)
		})
	}
}
