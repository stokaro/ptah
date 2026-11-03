package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// TestCompareWithDatabaseInfo_SerialColumnIsNotNull compares a column declared
// without NOT NULL with the column PostgreSQL built from it, which it holds
// NOT NULL. A serial column is NOT NULL whatever its definition says, so it
// differs in nothing; a plain integer declared so is a column that lost its
// NOT NULL, and the difference stays.
func TestCompareWithDatabaseInfo_SerialColumnIsNotNull(t *testing.T) {
	tests := []struct {
		name         string
		declaredType string
		want         string
	}{
		{name: "a bigserial column", declaredType: "BIGSERIAL", want: ""},
		{name: "a smallserial column", declaredType: "smallserial", want: ""},
		{name: "control: a bigint column", declaredType: "BIGINT", want: "false -> true"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: test.declaredType, Nullable: true}},
			}
			built := &catalog.Database{Tables: []catalog.Table{{
				Name: "t", Type: "BASE TABLE",
				Columns: []catalog.Column{{Name: "id", DataType: "bigint", ColumnType: "bigint", UDTName: "int8", IsNullable: "NO"}},
			}}}

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, built,
				catalog.ServerInfo{Dialect: "postgres"}, nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			changes := make(map[string]string)
			for _, table := range diff.TablesModified {
				for _, column := range table.ColumnsModified {
					changes = column.Changes
				}
			}
			c.Assert(changes["nullable"], qt.Equals, test.want)
		})
	}
}
