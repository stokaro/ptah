package difftypes_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestConstraintIdentity_IsOmittedUntilAProducerResolvesIt pins the wire shape.
//
// The identity is a second view of a record that already carries a name and a
// table, so a producer that has not resolved one must not put an empty object in
// the document a consumer reads -- an `identity` of three empty strings says
// "this constraint is in no schema, on no table" rather than "nobody answered".
func TestConstraintIdentity_IsOmittedUntilAProducerResolvesIt(t *testing.T) {
	c := qt.New(t)

	encoded, err := json.Marshal(difftypes.ConstraintRemovalInfo{
		Name: "uq_widget_scope", TableName: "public.widget", Type: "UNIQUE",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals,
		`{"name":"uq_widget_scope","table_name":"public.widget","type":"UNIQUE"}`)
}

// TestConstraintIdentity_KeepsItsPartsApart pins that the folded components
// travel separately, so no consumer has to parse a joined string back out --
// which is the mistake that makes a table whose own name contains a dot
// unreadable.
func TestConstraintIdentity_KeepsItsPartsApart(t *testing.T) {
	c := qt.New(t)

	encoded, err := json.Marshal(difftypes.ConstraintRemovalInfo{
		Name: "uq_widget_scope", TableName: "public.widget", Type: "UNIQUE",
		Identity: difftypes.ConstraintIdentity{
			Schema: "public", Table: "widget", Name: "uq_widget_scope",
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals,
		`{"name":"uq_widget_scope","table_name":"public.widget","type":"UNIQUE",`+
			`"identity":{"schema":"public","table":"widget","name":"uq_widget_scope"}}`)

	var decoded difftypes.ConstraintRemovalInfo
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	c.Assert(decoded.Identity.Table, qt.Equals, "widget")
	c.Assert(decoded.Identity.Schema, qt.Equals, "public")
}
