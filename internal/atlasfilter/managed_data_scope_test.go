package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/atlasfilter"
)

// managedDataTables spells what a projection kept of a desired state's declared
// rows.
func managedDataTables(db *schemamodel.Database) []string {
	tables := make([]string, 0, len(db.ManagedData))
	for _, data := range db.ManagedData {
		tables = append(tables, schemamodel.QualifyTableName(data.Schema, data.Table))
	}
	return tables
}

// TestExcludeGeneratedScopeReport_DeclaredRowsLeaveWithTheirTable covers the rows
// a table takes with it. The table has left the live side too, so a row kept here
// compares with nothing and reads as an insert. The last rows name a declaration
// on a table the desired state does not define: the live table leaves with the
// same selector, so the declaration has to be asked as well.
func TestExcludeGeneratedScopeReport_DeclaredRowsLeaveWithTheirTable(t *testing.T) {
	tests := []struct {
		name  string
		scope atlasfilter.Scope
		want  []string
	}{
		{
			name:  "a bare name, on a run limited to the default schema",
			scope: atlasfilter.Scope{Exclude: []string{"countries"}, DefaultSchema: "public"},
			want:  []string{"reporting.regions", "plans", "undefined"},
		},
		{
			name:  "a qualified name, on a realm-scoped run",
			scope: atlasfilter.Scope{Exclude: []string{"public.countries"}, DefaultSchema: "public", RealmRelativePatterns: true},
			want:  []string{"reporting.regions", "plans", "undefined"},
		},
		{
			name:  "the schema a declaration names",
			scope: atlasfilter.Scope{Exclude: []string{"reporting.regions"}, DefaultSchema: "public", RealmRelativePatterns: true},
			want:  []string{"countries", "plans", "undefined"},
		},
		{
			name:  "a whole schema",
			scope: atlasfilter.Scope{Exclude: []string{"reporting[type=schema]"}, DefaultSchema: "public", RealmRelativePatterns: true},
			want:  []string{"countries", "plans", "undefined"},
		},
		{
			name:  "every table",
			scope: atlasfilter.Scope{Exclude: []string{"*[type=table]"}, DefaultSchema: "public"},
			want:  make([]string, 0),
		},
		{
			name:  "a table the desired state does not define",
			scope: atlasfilter.Scope{Exclude: []string{"undefined"}, DefaultSchema: "public"},
			want:  []string{"countries", "reporting.regions", "plans"},
		},
		{
			name:  "a selector of another kind keeps every declaration",
			scope: atlasfilter.Scope{Exclude: []string{"countries[type=view]"}, DefaultSchema: "public"},
			want:  []string{"countries", "reporting.regions", "plans", "undefined"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := test.scope
			scope.Selections = selectext.None()
			declared := &schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "reporting"}},
				Tables: []schemamodel.Table{
					{StructName: "Country", Name: "countries"},
					{StructName: "Region", Name: "regions", Schema: "reporting"},
					{StructName: "Plan", Name: "plans"},
				},
				ManagedData: []schemamodel.ManagedData{
					{StructName: "Country", Table: "countries", Keys: []string{"code"}},
					{StructName: "Region", Table: "regions", Schema: "reporting", Keys: []string{"code"}},
					{StructName: "Plan", Table: "plans", Keys: []string{"id"}},
					{Table: "undefined", Keys: []string{"id"}},
				},
			}

			got, _, err := atlasfilter.ExcludeGeneratedScopeReport(declared, scope)

			c.Assert(err, qt.IsNil)
			c.Assert(managedDataTables(got), qt.DeepEquals, test.want)
			c.Assert(declared.ManagedData, qt.HasLen, 4, qt.Commentf("the filter does not write through the caller's state"))
		})
	}
}
