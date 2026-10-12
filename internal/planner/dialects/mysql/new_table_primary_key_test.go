package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestPlanner_GenerateMigrationAST_CreatesANewTableWithItsListedPrimaryKey
// plans a table whose primary key is a PRIMARY KEY entry of the constraint
// list, with an AUTO_INCREMENT column. The key goes inside the CREATE TABLE,
// once, because MySQL and MariaDB refuse an AUTO_INCREMENT column no key of
// the same statement covers. The rows declare the key as the entry alone, and
// on the column as well.
func TestPlanner_GenerateMigrationAST_CreatesANewTableWithItsListedPrimaryKey(t *testing.T) {
	tests := []struct {
		name    string
		primary bool
	}{
		{name: "the entry alone", primary: false},
		{name: "the entry and the column", primary: true},
	}
	for _, dialect := range mysqlFamilyDialects {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				desired := &schemamodel.Database{
					Tables: []schemamodel.Table{{StructName: "HK", Name: "hk"}},
					Fields: []schemamodel.Field{
						{StructName: "HK", Name: "id", Type: "INT", Primary: test.primary, AutoInc: true},
						{StructName: "HK", Name: "a", Type: "INT", Nullable: true},
					},
					Constraints: []schemamodel.Constraint{
						{StructName: "HK", Table: "hk", Name: "PRIMARY", Type: "PRIMARY KEY", Columns: []string{"id"}, Comment: "lookup"},
					},
				}
				diff := &difftypes.SchemaDiff{
					TablesAdded:      difftypes.TableCreationsFor(desired, identifier.ForDialect("mysql"), "hk"),
					ConstraintsAdded: difftypes.ConstraintAdditionsFor(desired, "PRIMARY"),
				}

				sql := renderMySQLFamily(c, dialect, diff, desired)

				c.Assert(sql, qt.Matches, `(?s).*CREATE TABLE hk \(\n  id INT NOT NULL AUTO_INCREMENT,\n.*  PRIMARY KEY \(id\) COMMENT 'lookup'\n\);.*`)
				c.Assert(strings.Count(sql, "PRIMARY KEY"), qt.Equals, 1, qt.Commentf("%s", sql))
			})
		}
	}
}
