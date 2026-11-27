package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// Offline, a SQL Server name with a non-ASCII rune has no class, so it may be
// any other name of its kind in its schema. Names in two schemas are two
// objects under every collation, so a declaration is refused only for a pair
// in one schema, or in two whose names may be one (stokaro/ptah#6747).

// tablesIn declares one table for each schema-qualified name.
func tablesIn(names ...[2]string) *schemamodel.Database {
	desired := &schemamodel.Database{}
	for _, name := range names {
		desired.Tables = append(desired.Tables, schemamodel.Table{StructName: name[1], Schema: name[0], Name: name[1]})
	}
	return desired
}

// TestValidateDesiredSchema_SQLServerOfflineNonASCIINameInAnotherSchema_HappyPath
// declares, without a connection, a name with a non-ASCII rune beside a name
// in another schema, in either order.
func TestValidateDesiredSchema_SQLServerOfflineNonASCIINameInAnotherSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
	}{
		{name: "views, the non-ASCII name first", desired: nonASCIIViews("sales.Größe", "dbo.v2")},
		{name: "views, the non-ASCII name second", desired: nonASCIIViews("dbo.v2", "sales.Größe")},
		{name: "a view in the default schema", desired: nonASCIIViews("[sales].[Größe]", "v2")},
		{name: "tables, the non-ASCII name first", desired: tablesIn([2]string{"sales", "Größe"}, [2]string{"dbo", "t2"})},
		{name: "tables, the non-ASCII name second", desired: tablesIn([2]string{"dbo", "t2"}, [2]string{"sales", "Größe"})},
		{
			// A schema with no class may be dbo, and the two names are
			// still two.
			name:    "a non-ASCII schema beside another table name",
			desired: tablesIn([2]string{"Größe", "t1"}, [2]string{"dbo", "t2"}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemadiff.ValidateDesiredSchema(c.Context(), must.Must(builtin.New()), test.desired,
				catalog.ServerInfo{Dialect: platform.SQLServer})

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestValidateDesiredSchema_SQLServerOfflineNonASCIINameInItsSchema_FailurePath
// is the control: in one schema, or in a schema that may be the other one,
// the two names may be one.
func TestValidateDesiredSchema_SQLServerOfflineNonASCIINameInItsSchema_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    string
	}{
		{
			name:    "two views in one schema",
			desired: nonASCIIViews("sales.Größe", "sales.v2"),
			want:    `without a connection Ptah cannot tell whether target views sales\.Größe and sales\.v2 are one name under the database collation`,
		},
		{
			name:    "two tables in one schema",
			desired: tablesIn([2]string{"sales", "t2"}, [2]string{"sales", "Größe"}),
			want:    `without a connection Ptah cannot tell whether target tables sales\.t2 and sales\.Größe are one name under the database collation`,
		},
		{
			name:    "one table name in a non-ASCII schema and in dbo",
			desired: tablesIn([2]string{"dbo", "t1"}, [2]string{"Größe", "t1"}),
			want:    `without a connection Ptah cannot tell whether target tables dbo\.t1 and Größe\.t1 are one name under the database collation`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemadiff.ValidateDesiredSchema(c.Context(), must.Must(builtin.New()), test.desired,
				catalog.ServerInfo{Dialect: platform.SQLServer})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*`+test.want+`.*`)
		})
	}
}

// TestCompareWithDatabaseInfo_SQLServerOfflineNonASCIINameInAnotherSchema_HappyPath
// plans the two examples of stokaro/ptah#6747 offline: each object is created.
func TestCompareWithDatabaseInfo_SQLServerOfflineNonASCIINameInAnotherSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    []string
	}{
		{name: "views", desired: nonASCIIViews("sales.Größe", "dbo.v2"), want: []string{"view added dbo.v2", "view added sales.Größe"}},
		{
			name:    "tables",
			desired: tablesIn([2]string{"sales", "Größe"}, [2]string{"dbo", "t2"}),
			want:    []string{"table added dbo.t2", "table added sales.Größe"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), test.desired, &catalog.Database{},
				catalog.ServerInfo{Dialect: platform.SQLServer}, nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(namedObjectChanges(diff), qt.DeepEquals, test.want)
		})
	}
}
