package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// nameComesBack is a diff that creates t2 and adds secret to t1, while the
// role holds privileges on both names the declaration does not give it: a
// table privilege kept from a t2 the database dropped, and a column privilege
// kept from a secret t1 dropped. Both apply again to the object created under
// the name.
func nameComesBack() *difftypes.SchemaDiff {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "T1", Name: "t1", PrimaryKey: []string{"id"}},
			{StructName: "T2", Name: "t2", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "T1", Name: "id", Type: "INT", Primary: true},
			{StructName: "T1", Name: "secret", Type: "VARCHAR(50)", Nullable: true},
			{StructName: "T2", Name: "id", Type: "INT", Primary: true},
		},
	}
	held := []difftypes.GrantRef{
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t2"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Column: "secret"},
	}
	return withDeclaredObjects(&difftypes.SchemaDiff{
		TablesAdded: difftypes.TableCreationsFor(desired, identifier.ForDialect("mysql"), "t2"),
		TablesModified: []difftypes.TableDiff{{TableName: "t1", ColumnsAdded: difftypes.ColumnChanges{
			{StructName: "T1", Name: "secret", Type: "VARCHAR(50)", Nullable: true},
		}}},
		GrantsRemoved: held,
		CurrentGrants: append(held, difftypes.GrantRef{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Column: "id"}),
	}, desired)
}

// MySQL and MariaDB keep a table or column privilege after DROP TABLE and DROP
// COLUMN, and REVOKE succeeds on a name nothing holds. So the plan revokes
// before it creates anything: a revoke that ran after the CREATE TABLE or the
// ADD COLUMN gives the role the object for as long as the statements between
// them take, and for good when one of them fails.
func TestPlan_MySQLFamilyRevokesBeforeANameComesBack(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), nameComesBack(), dialect,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"REVOKE SELECT ON `t2` FROM `reader`",
				"REVOKE SELECT (`secret`) ON `t1` FROM `reader`",
				"CREATE TABLE `t2` (\n  `id` INT PRIMARY KEY\n)",
				"-- Modify table: t1\nALTER TABLE `t1` ADD COLUMN `secret` VARCHAR(50)",
			})
		})
	}
}
