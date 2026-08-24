package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestTableColumns_FloatSpellingsConverge is the round trip that never ended.
//
// The catalog answers `float8` and `float4`; the renderer writes `double
// precision` and `real` into the document it produces from that read. So
// `schema inspect > out.hcl` followed by `apply --to file://out.hcl` planned
// `ALTER COLUMN "x" TYPE double precision` for a column nobody touched -- on
// PostgreSQL a full table rewrite under an ACCESS EXCLUSIVE lock -- and planned
// it again on the next run, forever (stokaro/ptah#2027).
//
// The controls are the point of the table: the two float types fold
// SEPARATELY, so a real change between them is still reported.
func TestTableColumns_FloatSpellingsConverge(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		catalog  string
		want     string
	}{
		{name: "the document's spelling of what the catalog calls float8",
			declared: "double precision", catalog: "float8"},
		{name: "the document's spelling of what the catalog calls float4",
			declared: "real", catalog: "float4"},
		{name: "both sides already agree",
			declared: "double precision", catalog: "double precision"},
		{
			// The control that makes the fold narrow rather than blanket: 8
			// bytes to 4 loses precision, and one token for both would call it
			// no change at all.
			name: "narrowed to four bytes", declared: "real", catalog: "float8",
			want: "double precision -> real",
		},
		{
			name: "widened to eight bytes", declared: "double precision", catalog: "float4",
			want: "real -> double precision",
		},
		{
			// An array is not its element type.
			name:     "a scalar column declared as an array",
			declared: "double precision[]", catalog: "float8",
			want: "double precision -> double precision[]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			genTable := goschema.Table{StructName: "Reading", Name: "readings"}
			dbTable := types.DBTable{
				Name:    "readings",
				Columns: []types.DBColumn{{Name: "x", DataType: test.catalog, IsNullable: "YES"}},
			}
			generated := &goschema.Database{Fields: []goschema.Field{
				{StructName: "Reading", Name: "x", Type: test.declared, Nullable: true},
			}}

			result := compare.TableColumns(genTable, dbTable, generated)

			c.Assert(floatTypeChanges(result.ColumnsModified), qt.DeepEquals, wantedChanges(test.want))
		})
	}
}

// floatTypeChanges names the type changes a comparison reported, in a form an
// assertion can read.
func floatTypeChanges(columns []difftypes.ColumnDiff) []string {
	changes := make([]string, 0, len(columns)+1)
	for _, column := range columns {
		changes = append(changes, presentChange(column.Changes["type"])...)
	}
	return changes
}

func presentChange(change string) []string {
	if change == "" {
		return nil
	}
	return []string{change}
}

// wantedChanges turns the row's single expectation into the list form the
// assertion compares against, so a row expecting silence says so with an empty
// list rather than with an absent substring.
func wantedChanges(want string) []string {
	return append(make([]string, 0, 1), presentChange(want)...)
}
