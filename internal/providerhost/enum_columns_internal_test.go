package providerhost

// White-box testing required: the schema the host hands an engine after it
// builds the engine's enum columns is private adapter state, shared by the
// comparison's lowering, the validation and the render. The subprocess tests
// in integration/providercli cover the engine.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
)

// enumTarget is a target whose engine states columns, with the feature
// listed by the provider and the engine.
func enumTarget(columns *protocol.EnumColumns) *target {
	features := []string{provider.EnumColumnsFeature}
	return &target{host: &Host{description: &protocol.Description{Features: features}},
		engine: &protocol.Engine{Id: "fixture", Features: features, EnumColumns: columns}}
}

// enumSchema is a table with an inline enum column and a column typed with a
// declared enum, as the Go and YAML sources write them.
func enumSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Item", Name: "kind", Type: "enum_item_kind", Enum: []string{"a", "b"}},
			{StructName: "Item", Name: "mood", Type: "mood", Check: "mood <> ''"},
		},
		Enums: []schemamodel.Enum{{Name: "enum_item_kind", Values: []string{"a", "b"}}, {Name: "mood", Values: []string{"up", "down"}}},
	}
}

// Each form builds both columns and leaves no enum type in the schema, and
// the comparison's lowering builds the same columns; the declaration stays
// as it was.
func TestEnumColumns_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name      string
		columns   *protocol.EnumColumns
		wantKind  schemamodel.Field
		wantMood  schemamodel.Field
		wantEnums []schemamodel.Enum
	}{
		{name: "a text column under a CHECK", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_CHECK, CheckType: "TEXT"},
			wantKind: schemamodel.Field{StructName: "Item", Name: "kind", Type: "TEXT", Enum: []string{"a", "b"}, Check: "kind IN ('a', 'b')"},
			wantMood: schemamodel.Field{StructName: "Item", Name: "mood", Type: "TEXT", Enum: []string{"up", "down"}, Check: "(mood <> '') AND mood IN ('up', 'down')"}},
		{name: "a type that lists the values", columns: &protocol.EnumColumns{Form: protocol.EnumColumns_VALUES_TYPE},
			wantKind: schemamodel.Field{StructName: "Item", Name: "kind", Type: "enum('a','b')", Enum: []string{"a", "b"}},
			wantMood: schemamodel.Field{StructName: "Item", Name: "mood", Type: "enum('up','down')", Enum: []string{"up", "down"}, Check: "mood <> ''"}},
		{name: "no form", columns: nil,
			wantKind:  schemamodel.Field{StructName: "Item", Name: "kind", Type: "enum_item_kind", Enum: []string{"a", "b"}},
			wantMood:  schemamodel.Field{StructName: "Item", Name: "mood", Type: "mood", Check: "mood <> ''"},
			wantEnums: enumSchema().Enums},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			declared := enumSchema()

			built, err := enumTarget(row.columns).enumColumns(declared)
			c.Assert(err, qt.IsNil)
			lowered, err := providerLowering{target: enumTarget(row.columns)}.LowerDesired(c.Context(), schemapreparation.LoweringRequest{Desired: declared})
			c.Assert(err, qt.IsNil)

			c.Assert(built.Fields[1], qt.DeepEquals, row.wantKind)
			c.Assert(built.Fields[2], qt.DeepEquals, row.wantMood)
			c.Assert(built.Enums, qt.DeepEquals, row.wantEnums)
			c.Assert(lowered, qt.DeepEquals, built)
			c.Assert(declared, qt.DeepEquals, enumSchema())
		})
	}
	// A host that negotiated the feature away keeps the enum a type of its
	// own, whatever the engine's description still holds.
	t.Run("the feature not negotiated", func(t *testing.T) {
		c := qt.New(t)
		target := enumTarget(&protocol.EnumColumns{Form: protocol.EnumColumns_REFUSED})
		target.host.description.Features = nil
		built, err := target.enumColumns(enumSchema())
		c.Assert(err, qt.IsNil)
		c.Assert(built, qt.DeepEquals, enumSchema())
	})
	t.Run("an engine without enums and a schema without one", func(t *testing.T) {
		c := qt.New(t)
		declared := &schemamodel.Database{Tables: enumSchema().Tables, Fields: enumSchema().Fields[:1]}
		built, err := enumTarget(&protocol.EnumColumns{Form: protocol.EnumColumns_REFUSED}).enumColumns(declared)
		c.Assert(err, qt.IsNil)
		c.Assert(built, qt.Equals, declared)
	})
}

// An engine that builds no enum refuses an enum column by its table and
// column, and an enum no column uses by its name.
func TestEnumColumns_FailurePath(t *testing.T) {
	refused := &protocol.EnumColumns{Form: protocol.EnumColumns_REFUSED}
	for _, row := range []struct {
		name   string
		schema *schemamodel.Database
		want   string
	}{
		{name: "an enum column", schema: enumSchema(),
			want: `.*column "kind" of table "items" is an enum column with the values \("a", "b"\), and the fixture engine builds no enum: type the column with a type fixture has`},
		{name: "a declared enum alone", schema: &schemamodel.Database{Tables: enumSchema().Tables, Fields: enumSchema().Fields[:1], Enums: enumSchema().Enums[1:]},
			want: `.*the schema declares the enum "mood", and the fixture engine builds no enum`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			built, err := enumTarget(refused).enumColumns(row.schema)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(built, qt.IsNil)
		})
	}
}
