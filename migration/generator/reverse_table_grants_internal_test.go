package generator

// White-box testing required: downMigrationSQL is package-local, and the
// property is which privileges the rollback of a table removal grants, which
// only the rendered rollback shows.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// grantedTableDatabase is a database holding the table docs, with the
// privileges its owner ptah_user holds on it and the ones it granted to the
// role r: two on the table and one on a column.
func grantedTableDatabase(roles ...catalog.Role) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "docs", Type: "BASE TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "bigint", FormattedType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "member_id", DataType: "bigint", FormattedType: "bigint", IsNullable: "YES"},
		}}},
		Indexes:     []catalog.Index{{Name: "docs_pkey", TableName: "docs", Columns: []string{"id"}, IsUnique: true, IsPrimary: true}},
		Constraints: []catalog.Constraint{{Name: "docs_pkey", TableName: "docs", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}}},
		Roles:       roles,
		Grants: []catalog.Grant{
			{Role: "ptah_user", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "docs", WithOption: true, GrantedBy: "ptah_user"},
			{Role: "ptah_user", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "docs", WithOption: true, GrantedBy: "ptah_user"},
			{Role: "r", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "docs", GrantedBy: "ptah_user"},
			{Role: "r", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "docs", GrantedBy: "ptah_user"},
			{Role: "r", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "docs", GrantedBy: "ptah_user", Column: "member_id"},
		},
	}
}

// grantLines is the GRANT statements of a rendered migration, in order.
func grantLines(sql string) []string {
	var grants []string
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.HasPrefix(line, "GRANT ") {
			grants = append(grants, line)
		}
	}
	return grants
}

// TestGenerateDownMigrationSQL_GrantsBackWhatARemovedTableHeld removes the
// table docs and plans the rollback, which creates docs again and grants r
// what r held on it, once each, and grants the owner nothing.
//
// Where the schema declares r, the forward direction revokes the privileges
// before it drops the table, and the rollback grants them back. Where it does
// not, the forward direction leaves them to the drop, and the rollback created
// the table without them.
func TestGenerateDownMigrationSQL_GrantsBackWhatARemovedTableHeld(t *testing.T) {
	tests := []struct {
		name     string
		database *catalog.Database
		roles    []schemamodel.Role
	}{
		{name: "a role the schema does not declare", database: grantedTableDatabase()},
		{name: "a role the schema declares", database: grantedTableDatabase(catalog.Role{Name: "r"}), roles: []schemamodel.Role{{Name: "r"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Other", Name: "other"}},
				Fields: []schemamodel.Field{{StructName: "Other", Name: "id", Type: "BIGINT", Primary: true}},
				Roles:  test.roles,
			}
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, test.database, platform.Postgres, runtime))
			c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, []string{"docs"})

			down, err := downMigrationSQL(t.Context(), runtime, diff, desired, test.database, platform.Postgres, capability.Postgres17())

			c.Assert(err, qt.IsNil)
			c.Assert(grantLines(down), qt.DeepEquals, []string{
				`GRANT INSERT ON TABLE "docs" TO "r";`,
				`GRANT SELECT ON TABLE "docs" TO "r";`,
				`GRANT UPDATE ("member_id") ON TABLE "docs" TO "r";`,
			}, qt.Commentf("%s", down))
			c.Assert(strings.Index(down, `CREATE TABLE "docs"`) < strings.Index(down, "GRANT "), qt.IsTrue,
				qt.Commentf("the grants follow the table they name:\n%s", down))
		})
	}
}
