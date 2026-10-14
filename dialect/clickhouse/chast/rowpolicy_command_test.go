package chast_test

import (
	"encoding/json"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chschema"
)

func commandCodec() schemaext.Codec {
	codecs := chast.Codecs()
	return codecs[slices.IndexFunc(codecs, func(c schemaext.Codec) bool { return c.Prototype.Kind() == chast.RowPolicyCommandKind })]
}

func TestRowPolicyCommandCodecPreservesPartialIntentAndCopies(t *testing.T) {
	c := qt.New(t)
	codec := commandCodec()
	original := &chast.RowPolicyCommand{Action: "ALTER", Table: "orders", Name: "tenant", RolesSet: true,
		Values: &chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}}
	encoded := must.Must(codec.Encode(original))
	decoded := must.Must(codec.Decode(encoded)).(*chast.RowPolicyCommand)
	c.Assert(decoded, qt.DeepEquals, original)
	decoded.Values.Roles.Except[0] = "changed"
	c.Assert(original.Values.Roles.Except, qt.DeepEquals, []string{"admin"})
	copied := original.Copy()
	copied.Values.Roles.Except[0] = "copied"
	c.Assert(original.Values.Roles.Except, qt.DeepEquals, []string{"admin"})
	drop := &chast.RowPolicyCommand{Action: "DROP", Table: "orders", Name: "tenant"}
	c.Assert(must.Must(codec.Decode(must.Must(codec.Encode(drop)))), qt.DeepEquals, drop)
	c.Assert(drop.Effect().Impact, qt.Equals, schemaext.Destructive)
	c.Assert(drop.AccessEffect().Access, qt.Equals, schemaext.AccessUnknown)
}

func TestRowPolicyCommandCodecRefusesLostOrInventedClauses(t *testing.T) {
	for _, text := range []string{
		`{"action":"ALTER","database":"","table":"orders","name":"p","values":{},"filter_set":false,"roles_set":false}`,
		`{"action":"ALTER","database":"","table":"orders","name":"p","values":{"filter":"id = 1"},"filter_set":false,"roles_set":true}`,
		`{"action":"DROP","database":"","table":"orders","name":"p","values":{},"filter_set":false,"roles_set":false}`,
		`{"action":"CREATE","database":"","table":"orders","name":"p","values":{"extra":true},"filter_set":false,"roles_set":false}`,
		`{"action":"CREATE","database":"","table":"orders","name":"p","values":{},"filter_set":false,"roles_set":null}`,
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			value, err := commandCodec().Decode(json.RawMessage(text))
			c.Assert(err, qt.IsNotNil)
			c.Assert(value, qt.IsNil)
		})
	}
}
