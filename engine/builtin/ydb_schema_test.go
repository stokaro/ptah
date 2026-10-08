package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// TestGetOrderedCreateStatements_YDBRefusesAnEnumByName pins the path an enum
// takes to the YDB renderer. YDB has no enum column type, no CREATE TYPE and
// no CHECK to emulate one with, so the declaration is refused by the enum's
// own name and key. Lowered into the column instead, it would be refused as
// the column's type and the message would name a key the target never had a
// use for.
func TestGetOrderedCreateStatements_YDBRefusesAnEnumByName(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Ticket", Name: "tickets"}},
		Fields: []schemamodel.Field{
			{StructName: "Ticket", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Ticket", Name: "mood", Type: "mood", Nullable: true},
		},
		Enums: []schemamodel.Enum{{Name: "mood", Values: []string{"calm", "angry"}}},
	}
	schemamodel.Finalize(db)

	statements, err := builtin.GetOrderedCreateStatements(db, "ydb")

	c.Assert(statements, qt.IsNil)
	var refusal *ptaherr.CapabilityError
	c.Assert(err, qt.ErrorAs, &refusal)
	c.Assert(refusal.Feature, qt.Equals, string(capability.EnumCustomType))
	c.Assert(err, qt.ErrorMatches, `(?s).*enum mood, which requires target capability enum_custom_type.*`)
}

// TestRenderSQLReportingOmissions_YDBReportsADroppedTypeModifier pins the
// report of what a YDB type does not keep. A Utf8 has no length and a
// Timestamp keeps microseconds whatever precision was declared, so the
// statement renders without either, and the omission is how a caller finds out
// the declared limit is not enforced.
func TestRenderSQLReportingOmissions_YDBReportsADroppedTypeModifier(t *testing.T) {
	c := qt.New(t)
	table := &ast.CreateTableNode{
		Name: "accounts",
		Columns: []*ast.ColumnNode{
			ast.NewColumn("id", "BIGINT").SetPrimary(),
			{Name: "email", Type: "VARCHAR(255)", Nullable: true},
			{Name: "seen", Type: "TIMESTAMP(3)", Nullable: true},
			{Name: "note", Type: "TEXT", Nullable: true},
		},
	}

	sql, omissions, err := builtin.RenderSQLReportingOmissions("ydb", capability.YDB262(), table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "`email` Utf8,")
	c.Assert(omissions, qt.HasLen, 2)
	c.Assert([]string{omissions[0].Name, omissions[0].Property, omissions[0].Detail}, qt.DeepEquals,
		[]string{"accounts.email", "type modifier", "VARCHAR(255): length 255"})
	c.Assert([]string{omissions[1].Name, omissions[1].Property, omissions[1].Detail}, qt.DeepEquals,
		[]string{"accounts.seen", "type modifier", "TIMESTAMP(3): precision 3 (YDB keeps microseconds)"})
}

// ydbIndexedSchema declares one table with an index over the named column.
func ydbIndexedSchema(indexed string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Reading", Name: "readings"}},
		Fields: []schemamodel.Field{
			{StructName: "Reading", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Reading", Name: "value", Type: "DOUBLE", Nullable: true},
			{StructName: "Reading", Name: "label", Type: "TEXT", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "Reading", Name: "idx_readings", Fields: []string{indexed}}},
	}
	schemamodel.Finalize(db)
	return db
}

// TestValidateSchema_YDBRefusesTheIndexTheRenderRefuses pins validation to the
// render on YDB. An index over a Double column is refused by the table's
// render; a plan adding it to a table that exists sees the index alone and
// cannot, so validation, which runs before a live comparison plans anything,
// is where it is refused.
func TestValidateSchema_YDBRefusesTheIndexTheRenderRefuses(t *testing.T) {
	c := qt.New(t)

	err := builtin.ValidateSchemaWithCapabilities(ydbIndexedSchema("value"), "ydb", capability.YDB262())

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*index "idx_readings" on table "readings": column "value" is Double, which YDB refuses as an index key.*`)
}

// TestValidateSchema_YDBAcceptsAnIndexTheRenderAccepts is the control: the
// same table indexed on a text column validates.
func TestValidateSchema_YDBAcceptsAnIndexTheRenderAccepts(t *testing.T) {
	c := qt.New(t)

	err := builtin.ValidateSchemaWithCapabilities(ydbIndexedSchema("label"), "ydb", capability.YDB262())

	c.Assert(err, qt.IsNil)
}

// TestGetOrderedCreateStatements_YDBQuotesATableNameOnce pins the path of a
// table name YDB has to escape. A name holding a backtick and a dot reaches
// the renderer in the canonical spelling, and the renderer writes it as one
// backticked path with the backtick escaped once.
func TestGetOrderedCreateStatements_YDBQuotesATableNameOnce(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		table  string
		want   string
	}{
		{name: "a backtick and dots at the root", table: "tick`name.with.dot",
			want: "CREATE TABLE `tick\\`name.with.dot` ("},
		{name: "the same name in a directory", schema: "app/sub", table: "tick`name.with.dot",
			want: "CREATE TABLE `app/sub/tick\\`name.with.dot` ("},
		{name: "a backslash", table: `back\slash`,
			want: "CREATE TABLE `back\\\\slash` ("},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: test.table, Schema: test.schema}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
			}
			schemamodel.Finalize(db)

			statements, err := builtin.GetOrderedCreateStatements(db, "ydb")

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, test.want)
		})
	}
}
