package fromschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// A type that is already the target's own is not put through the portable
// mapping -- stokaro/ptah#2147.
//
// The mapping turns a portable spelling into what this target means by it. A
// type the author wrote with Atlas HCL's sql() escape hatch, or one the
// target's own catalog reported, is not a portable spelling waiting to be
// interpreted: it is the answer.
//
// Mapping it changed the string, and because the string changed the marker was
// then correctly dropped, so nothing downstream could recover the fact. The
// symptom hid behind a length: `VARCHAR(50)` misses the bare-name switch and
// survived, `TEXT` did not. Measured on SQL Server 2025, a `TEXT` column read
// from the target replayed as `nvarchar(-1)` beside a `VARCHAR(50)` that
// replayed as `varchar/50`.
func TestFromField_ATypeAlreadyTheTargetsOwnSkipsThePortableMapping(t *testing.T) {
	tests := []struct {
		name  string
		field schemamodel.Field
		want  string
	}{
		{
			name:  "a native TEXT named with sql()",
			field: schemamodel.Field{Name: "c", Type: "TEXT", TypeRawSQL: true},
			want:  "TEXT",
		},
		{
			name:  "a native TEXT read from the catalog",
			field: schemamodel.Field{Name: "c", Type: "TEXT", TypeIsDeclaredText: true},
			want:  "TEXT",
		},
		{
			name:  "a native bare VARCHAR named with sql()",
			field: schemamodel.Field{Name: "c", Type: "VARCHAR", TypeRawSQL: true},
			want:  "VARCHAR",
		},
		{
			// The controls. A portable declaration still means what a schema
			// written for several engines means by it, and deciding that
			// differently is what a too-wide fix breaks.
			name:  "a portable TEXT",
			field: schemamodel.Field{Name: "c", Type: "TEXT"},
			want:  "NVARCHAR(MAX)",
		},
		{
			name:  "a portable bare VARCHAR",
			field: schemamodel.Field{Name: "c", Type: "VARCHAR"},
			want:  "NVARCHAR(MAX)",
		},
		{
			name:  "a portable SERIAL",
			field: schemamodel.Field{Name: "c", Type: "SERIAL"},
			want:  "INT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			column := fromschema.FromField(tt.field, nil, "sqlserver")

			c.Assert(column.Type, qt.Equals, tt.want)
		})
	}
}

// The marker survives the conversion when the type does, which is what lets the
// renderer leave the spelling alone. Without this the test above would pass on
// a converter that kept the type and dropped the fact.
func TestFromField_TheMarkerSurvivesWithTheType(t *testing.T) {
	c := qt.New(t)

	raw := fromschema.FromField(
		schemamodel.Field{Name: "c", Type: "TEXT", TypeRawSQL: true}, nil, "sqlserver")
	native := fromschema.FromField(
		schemamodel.Field{Name: "c", Type: "TEXT", TypeIsDeclaredText: true}, nil, "sqlserver")

	c.Assert(raw.TypeRawSQL, qt.IsTrue)
	c.Assert(native.TypeIsDeclaredText, qt.IsTrue)
}
