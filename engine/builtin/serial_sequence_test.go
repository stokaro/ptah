package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// serialSchema declares table orders whose BIGSERIAL key starts at 100 and
// steps by 5.
func serialSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{
			StructName: "Order", Name: "id", Type: "BIGSERIAL", Primary: true, AutoInc: true,
			IdentityGeneration: "BY_DEFAULT", IdentityStart: "100", IdentityIncrement: "5",
		}},
	}
}

// TestRenderSQL_AlterSerialSequence_FailurePath refuses the node on every
// target without capability.SerialSequenceOptions, before the dialect's
// renderer sees it.
func TestRenderSQL_AlterSerialSequence_FailurePath(t *testing.T) {
	node := &ast.AlterSerialSequenceNode{Table: "orders", Column: "id",
		Path: "/local/orders/_serial_column_id", Start: 100, Increment: 1}
	for _, test := range []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		node    *ast.AlterSerialSequenceNode
		wantErr string
	}{
		{name: "postgres", dialect: platform.Postgres, caps: capability.Postgres17(), node: node,
			wantErr: `changing the sequence of Serial column "id" of table "orders", which requires target capability ` +
				`serial_sequence_options, unavailable on this postgres target`},
		{name: "mysql", dialect: platform.MySQL, caps: capability.MySQL84(), node: node,
			wantErr: `changing the sequence of Serial column "id" of table "orders", which requires target capability ` +
				`serial_sequence_options, unavailable on this mysql target`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, test.node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
