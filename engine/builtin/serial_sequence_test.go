package builtin_test

import (
	"strings"
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

// TestGetOrderedCreateStatementsReportingOmissions_YDBReportsASerialsSettings
// pins what a render without a database does with a Serial's start and
// increment on YDB: CREATE TABLE has no clause for them, and the ALTER
// SEQUENCE that sets them names the sequence by an absolute path that includes
// the database, so the render writes the table and reports both as dropped. A
// plan against a database writes that statement instead.
func TestGetOrderedCreateStatementsReportingOmissions_YDBReportsASerialsSettings(t *testing.T) {
	c := qt.New(t)

	statements, omissions, err := builtin.GetOrderedCreateStatementsReportingOmissions(
		serialSchema(), platform.YDB, capability.YDB262())

	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(rendered, qt.Contains, "`id` BigSerial NOT NULL")
	c.Assert(rendered, qt.Not(qt.Contains), "SEQUENCE")
	properties := make([]string, 0, len(omissions))
	for _, omission := range omissions {
		c.Assert(omission.Name, qt.Equals, "orders.id")
		properties = append(properties, omission.Property)
	}
	c.Assert(properties, qt.DeepEquals, []string{"identity increment", "identity start"})
}

// TestRenderSQL_AlterSerialSequence_HappyPath writes the statement that gives
// a Serial's sequence its settings: both settings always, and RESTART only
// where the node asks for it, with the start spelled out.
func TestRenderSQL_AlterSerialSequence_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		node *ast.AlterSerialSequenceNode
		want string
	}{
		{
			name: "without a restart",
			node: &ast.AlterSerialSequenceNode{Table: "app.orders", Column: "id",
				Path: "/local/app/orders/_serial_column_id", Start: 1, Increment: 5},
			want: "ALTER SEQUENCE `/local/app/orders/_serial_column_id` START WITH 1 INCREMENT BY 5;\n",
		},
		{
			name: "with a restart",
			node: &ast.AlterSerialSequenceNode{Table: "orders", Column: "id",
				Path: "/local/orders/_serial_column_id", Start: 100, Increment: 1, Restart: true},
			want: "ALTER SEQUENCE `/local/orders/_serial_column_id` START WITH 100 INCREMENT BY 1 RESTART WITH 100;\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestRenderSQL_AlterSerialSequence_FailurePath refuses the node on every
// target without capability.SerialSequenceOptions, before the dialect's
// renderer sees it, and refuses a path YDB would not resolve: measured, a
// relative one answers `Path does not exist`.
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
		{name: "ydb without the key", dialect: platform.YDB,
			caps: capability.YDB262().With(capability.SerialSequenceOptions, false), node: node,
			wantErr: `changing the sequence of Serial column "id" of table "orders", which requires target capability ` +
				`serial_sequence_options, unavailable on this ydb target`},
		{name: "a relative path", dialect: platform.YDB, caps: capability.YDB262(),
			node: &ast.AlterSerialSequenceNode{Table: "orders", Column: "id", Path: "orders/_serial_column_id", Start: 1, Increment: 1},
			wantErr: `the sequence of Serial column "id" of table "orders": YDB's ALTER SEQUENCE takes a sequence only by ` +
				`its absolute path, and "orders/_serial_column_id" is not one`},
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
