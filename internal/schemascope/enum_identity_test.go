package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemascope"
)

// scopedEnumSchema declares one enum of the given name and one qualified table
// whose column is typed with it. The table carries schema="public" on purpose:
// scoping an unqualified table with --schema refuses for either spelling, so a
// fixture without the qualifier coincides instead of discriminating.
func scopedEnumSchema(enumName string) *goschema.Database {
	return &goschema.Database{
		Enums:  []goschema.Enum{{Name: enumName, Values: []string{"active", "archived"}}},
		Tables: []goschema.Table{{StructName: "User", Schema: "public", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "User", Name: "s", Type: enumName},
		},
	}
}

// TestFilterGeneratedWithDefaultSchema_KeepsAnEnumWhateverItIsCalled pins that a
// scoped desired schema keeps the enum a kept column references.
//
// keepReferencedGeneratedEnums collected referenced types only when the type name
// began with "enum_", so a declared enum named e.g. "status_kind" was dropped
// from the scoped schema while the column that names it stayed. The CREATE TYPE
// was then never emitted and the run died, rather than merely under-generating:
//
//	ptah schema test --root-dir . --dir tests --schema public --db-url postgres://...
//	error: apply desired schema: ERROR: type "status_kind" does not exist
//	(SQLSTATE 42704)   TRUE_EXIT=2
//
// The two rows are the discriminating pair: identical schemas whose enum names
// differ only by the historical prefix (stokaro/ptah#931 item 1).
func TestFilterGeneratedWithDefaultSchema_KeepsAnEnumWhateverItIsCalled(t *testing.T) {

	tests := []struct {
		name string
		enum string
	}{
		{name: "no prefix", enum: "status_kind"},
		{name: "historical prefix", enum: "enum_status"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := schemascope.FilterGeneratedWithDefaultSchema(scopedEnumSchema(test.enum), []string{"public"}, "public")

			c.Assert(got.Enums, qt.HasLen, 1)
			c.Assert(got.Enums[0].Name, qt.Equals, test.enum)
		})
	}
}

// TestFilterGeneratedWithDefaultSchema_DropsAnUnreferencedEnum is the inverse
// control. Without it, "keep every enum" would satisfy the test above while
// undoing the scoping this function exists to perform.
func TestFilterGeneratedWithDefaultSchema_DropsAnUnreferencedEnum(t *testing.T) {
	c := qt.New(t)

	db := scopedEnumSchema("status_kind")
	db.Enums = append(db.Enums, goschema.Enum{Name: "unused_kind", Values: []string{"a"}})

	got := schemascope.FilterGeneratedWithDefaultSchema(db, []string{"public"}, "public")

	c.Assert(got.Enums, qt.HasLen, 1)
	c.Assert(got.Enums[0].Name, qt.Equals, "status_kind")
}
