package chast_test

import (
	"encoding/json"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func rowPolicyOperation() *chast.RowPolicy {
	return &chast.RowPolicy{Database: "app", Table: "orders", Name: "tenant", Change: *chdiff.NewRowPolicy(
		&chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{Names: []string{"alice"}}},
		&chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{Names: []string{"alice"}}})}
}

// The operation travels through the registry with its identity, both
// operands and the assessment, and a clone shares no role list with it.
func TestRowPolicyOperationCodecPreservesTheChange(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(engine.New(engine.Provider{ID: "example.org/row-policy", Codecs: chast.Codecs()})).Codecs()
	op := rowPolicyOperation()

	data, err := registry.Marshal(t.Context(), schemaext.Operation, []schemaext.Payload{op})
	c.Assert(err, qt.IsNil)
	decoded, err := registry.Unmarshal(t.Context(), data)

	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{op})
	cloned, err := ast.CloneExtensionPayload(op)
	c.Assert(err, qt.IsNil)
	cloned.(*chast.RowPolicy).Change.After.Roles.Names[0] = "mutated"
	c.Assert(op.Change.After.Roles.Names, qt.DeepEquals, []string{"alice"})
	c.Assert(op.AccessEffect().Access, qt.Equals, schemaext.AccessNarrows)
	c.Assert(op.Effect().Impact, qt.Equals, schemaext.Behavioral)
	c.Assert(op.Subject(), qt.DeepEquals, chschema.RowPolicyRef("app", "orders", "tenant"))
	c.Assert(op.SchemaChange(), qt.DeepEquals, ast.ExtensionChange{Action: ast.ExtensionModify, Name: "tenant ON app.orders"})
}

// The operation names one table policy and carries a change the owner could
// plan, so a document cannot smuggle in a database-wide policy or a change
// without its assessment.
func TestRowPolicyOperationCodecRefusesIncompleteOperations(t *testing.T) {
	codecs := chast.Codecs()
	codec := codecs[slices.IndexFunc(codecs, func(candidate schemaext.Codec) bool { return candidate.Prototype.Kind() == chast.RowPolicyKind })]
	change := `{"before":null,"after":{},"access":{"access":"unknown","reason":"r"}}`
	for _, data := range []string{
		`null`, `{}`,
		`{"database":"app","table":"orders","name":"tenant"}`,
		`{"database":"app","table":"","name":"tenant","change":` + change + `}`,
		`{"database":"app","table":"orders","name":"","change":` + change + `}`,
		`{"database":"app","table":"orders","name":"tenant","change":{"before":null,"after":{}}}`,
		`{"database":"app","table":"orders","name":"tenant","change":` + change + `,"extra":true}`,
		`{"database":null,"table":"orders","name":"tenant","change":` + change + `}`,
	} {
		t.Run(data, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := codec.Decode(json.RawMessage(data))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}
