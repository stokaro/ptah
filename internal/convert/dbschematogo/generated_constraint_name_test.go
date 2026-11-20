package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvert_GeneratedConstraintName describes the two-column UNIQUE and the
// CHECK of an Oracle table, under the names the server chose or a name
// somebody chose. A name the server chose is left out, so the description
// declares an unnamed key and an unnamed CHECK, which the comparison pairs with
// the server's by columns and by condition. Written out, SYS_C008800 creates a
// constraint of that name in another account, where the server may later
// choose the same name for one of its own and refuse it with ORA-02264. A
// foreign key keeps its name either way, because nothing pairs an unnamed one.
func TestConvert_GeneratedConstraintName(t *testing.T) {
	tests := []struct {
		name      string
		generated bool
		want      []string
	}{
		{name: "the server chose the names", generated: true, want: []string{"", "", "SYS_C008802"}},
		{name: "somebody chose the names", want: []string{"SYS_C008800", "SYS_C008801", "SYS_C008802"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			read := &catalog.Database{
				StoredNames: true,
				Tables: []catalog.Table{
					{Schema: "APP", Name: "PAIRS", Type: "BASE TABLE", Columns: []catalog.Column{
						{Name: "ID", DataType: "NUMBER", IsNullable: "NO", IsPrimaryKey: true},
						{Name: "A", DataType: "VARCHAR2", IsNullable: "YES"},
						{Name: "B", DataType: "VARCHAR2", IsNullable: "YES"},
					}},
					{Schema: "APP", Name: "OTHERS", Type: "BASE TABLE", Columns: []catalog.Column{
						{Name: "A", DataType: "VARCHAR2", IsNullable: "YES"},
						{Name: "B", DataType: "VARCHAR2", IsNullable: "YES"},
					}},
				},
				Constraints: []catalog.Constraint{
					{
						Schema: "APP", TableName: "PAIRS", Name: "SYS_C008800", Type: "UNIQUE",
						ColumnNames: []string{"A", "B"}, GeneratedName: test.generated,
					},
					{
						Schema: "APP", TableName: "PAIRS", Name: "SYS_C008801", Type: "CHECK",
						CheckClause: new("a <> b"), GeneratedName: test.generated,
					},
					{
						Schema: "APP", TableName: "PAIRS", Name: "SYS_C008802", Type: "FOREIGN KEY",
						ColumnNames: []string{"A", "B"}, ForeignSchema: "APP", ForeignTable: new("OTHERS"),
						ForeignColumns: []string{"A", "B"}, GeneratedName: test.generated,
					},
				},
			}

			database := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), read, "oracle", must.Must(builtin.New())))

			names := make([]string, 0, len(database.Constraints))
			for _, constraint := range database.Constraints {
				names = append(names, constraint.Name)
			}
			c.Assert(names, qt.DeepEquals, test.want)
		})
	}
}
