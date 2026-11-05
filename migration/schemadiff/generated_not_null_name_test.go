package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// TestCompareReportingUndecidedAdditionsTakesNoNameForTheGeneratedOne
// compares a PostgreSQL 18 read as the desired side, which names every NOT
// NULL, with a document as the current side, the shape `schema diff --from
// <file> --to <database>` compares. A document that leaves the name out leaves
// it to the server, which gives it `<table>_<column>_not_null`, so the two
// describe one constraint. A name the document writes is compared, and so is
// the same pair on a server that gives a NOT NULL no name.
func TestCompareReportingUndecidedAdditionsTakesNoNameForTheGeneratedOne(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
		desired string
		current string
		want    []*difftypes.NotNullConstraintNameChange
	}{
		{
			name:    "the generated name beside no name",
			dialect: "postgres",
			schema:  "public",
			desired: "t1_id_not_null",
		},
		{
			name:    "the generated name beside a written one",
			dialect: "postgres",
			schema:  "public",
			desired: "t1_id_not_null",
			current: "t1_id_nn",
			want:    []*difftypes.NotNullConstraintNameChange{{Current: "t1_id_nn", Desired: "t1_id_not_null"}},
		},
		{
			name:    "a written name beside no name",
			dialect: "postgres",
			schema:  "public",
			desired: "t1_id_nn",
			want:    []*difftypes.NotNullConstraintNameChange{{Desired: "t1_id_nn"}},
		},
		{
			name:    "a numbered name beside no name",
			dialect: "postgres",
			schema:  "public",
			desired: "t1_id_not_null1",
			want:    []*difftypes.NotNullConstraintNameChange{{Desired: "t1_id_not_null1"}},
		},
		{
			name:    "the same name on a server that names no NOT NULL",
			dialect: "mysql",
			desired: "t1_id_not_null",
			want:    []*difftypes.NotNullConstraintNameChange{{Desired: "t1_id_not_null"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T1", Name: "t1"}},
				Fields: []schemamodel.Field{{
					StructName: "T1", Name: "id", Type: "integer", NotNullConstraintName: test.desired,
				}},
			}
			current := &catalog.Database{Tables: []catalog.Table{{Schema: test.schema, Name: "t1", Columns: []catalog.Column{{
				Name: "id", DataType: "integer", UDTName: "int4", ColumnType: "integer", IsNullable: "NO",
				NotNullConstraintName: test.current,
			}}}}}
			opts := config.DefaultCompareOptions()
			opts.Dialect = test.dialect

			diff, _, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), desired, current, opts, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(notNullNameChanges(diff), qt.DeepEquals, test.want)
		})
	}
}

// notNullNameChanges collects the NOT NULL name change of every modified
// column of diff.
func notNullNameChanges(diff *difftypes.SchemaDiff) []*difftypes.NotNullConstraintNameChange {
	var changes []*difftypes.NotNullConstraintNameChange
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsModified {
			changes = append(changes, column.NotNullConstraintNameChange)
		}
	}
	return changes
}
