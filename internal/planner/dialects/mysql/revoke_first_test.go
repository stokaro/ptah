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

// columnPrivilegeOnANameThatComesBack is a diff that creates t2 and the view
// v1, adds secret to t1, and revokes SELECT on all of t1, t2 and v1, while the
// role keeps the column privileges the catalog holds on t1's id, t1's secret,
// t2's id and v1's id. secret, t2 and v1 are the ones the database dropped,
// and the server kept the privileges on.
func columnPrivilegeOnANameThatComesBack() *difftypes.SchemaDiff {
	diff := nameComesBack()
	diff.ViewsAdded = difftypes.ViewChanges{{Name: "v1", Body: "SELECT id FROM t2"}}
	diff.GrantsRemoved = []difftypes.GrantRef{
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t2"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "v1"},
	}
	diff.CurrentGrants = []difftypes.GrantRef{
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Column: "id"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Column: "Secret"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t2"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t2", Column: "id"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "v1"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "v1", Column: "id"},
	}
	return diff
}

// A column privilege the role keeps and a table revoke takes comes back right
// after the revokes where its column exists, and after the CREATE TABLE,
// CREATE VIEW or ADD COLUMN that brings it back where it does not: the server
// answers a GRANT on a missing column with ERROR 1054, and on a missing table
// or view with 1146. The column is matched without case, as the server
// matches it.
func TestPlan_MySQLFamilyGrantsBackAColumnPrivilegeOnceItsColumnExists(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), columnPrivilegeOnANameThatComesBack(), dialect,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"REVOKE SELECT ON `t1` FROM `reader`",
				"REVOKE SELECT ON `t2` FROM `reader`",
				"REVOKE SELECT ON `v1` FROM `reader`",
				"GRANT SELECT (`id`) ON `t1` TO `reader`",
				"CREATE TABLE `t2` (\n  `id` INT PRIMARY KEY\n)",
				"-- Modify table: t1\nALTER TABLE `t1` ADD COLUMN `secret` VARCHAR(50)",
				"CREATE VIEW `v1` AS\nSELECT id FROM t2",
				"GRANT SELECT (`Secret`) ON `t1` TO `reader`",
				"GRANT SELECT (`id`) ON `t2` TO `reader`",
				"GRANT SELECT (`id`) ON `v1` TO `reader`",
			})
		})
	}
}

// columnPrivilegeOnAReplacedView is a diff that replaces the view v1 with one
// that has an extra column, and revokes SELECT on all of v1, while the role
// keeps the privilege the catalog holds on extra: the server kept it from a v1
// that had the column before it was replaced without it.
func columnPrivilegeOnAReplacedView() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		ViewsModified: []difftypes.ViewDiff{{
			ViewName: "v1",
			Changes:  map[string]string{"body": "SELECT id FROM t1 -> SELECT id, secret AS extra FROM t1"},
			Desired:  schemamodel.View{Name: "v1", Body: "SELECT id, secret AS extra FROM t1"},
		}},
		GrantsRemoved: []difftypes.GrantRef{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "v1"}},
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "v1"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "v1", Column: "extra"},
		},
	}
}

// respelledColumnPrivilege is a diff that replaces SELECT on the Name column
// of t1 with SELECT on name, beside the schema change change makes to it.
func respelledColumnPrivilege(change func(*difftypes.SchemaDiff, *schemamodel.Database)) func() *difftypes.SchemaDiff {
	return func() *difftypes.SchemaDiff {
		held := difftypes.GrantRef{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Column: "Name"}
		desired := &schemamodel.Database{
			Tables: []schemamodel.Table{
				{StructName: "T1", Name: "t1", PrimaryKey: []string{"id"}},
				{StructName: "T2", Name: "t2", PrimaryKey: []string{"id"}},
			},
			Fields: []schemamodel.Field{
				{StructName: "T1", Name: "id", Type: "INT", Primary: true},
				{StructName: "T1", Name: "name", Type: "VARCHAR(50)", Nullable: true},
				{StructName: "T2", Name: "id", Type: "INT", Primary: true},
			},
		}
		diff := &difftypes.SchemaDiff{
			GrantsAdded:   []difftypes.GrantRef{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Column: "name"}},
			GrantsRemoved: []difftypes.GrantRef{held},
			CurrentGrants: []difftypes.GrantRef{held},
		}
		change(diff, desired)
		return withDeclaredObjects(diff, desired)
	}
}

// The view a plan replaces is the view the column privilege comes back on:
// CREATE OR REPLACE VIEW is what gives v1 its extra column, and a GRANT on
// the column before it fails with ERROR 1054, measured on MySQL 8.4 and 26.10
// and MariaDB 11.8 and 12.3.
func TestPlan_MySQLFamilyGrantsBackAColumnPrivilegeOnceItsViewIsReplaced(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), columnPrivilegeOnAReplacedView(), dialect,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"REVOKE SELECT ON `v1` FROM `reader`",
				"CREATE OR REPLACE VIEW `v1` AS\nSELECT id, secret AS extra FROM t1",
				"GRANT SELECT (`extra`) ON `v1` TO `reader`",
			})
		})
	}
}

// On MySQL a privilege the declaration spells another way is revoked at the
// start of the plan and granted again under the declared spelling right after
// the revokes, where the column exists: placed after the tables, a statement
// failing in between leaves the role without the column. On a column the plan
// adds, the GRANT waits for the ADD COLUMN.
func TestPlan_MySQLGrantsARespelledColumnPrivilegeOnceItsColumnExists(t *testing.T) {
	tests := []struct {
		name string
		diff func() *difftypes.SchemaDiff
		want []string
	}{
		{
			name: "beside a created table",
			diff: respelledColumnPrivilege(func(diff *difftypes.SchemaDiff, desired *schemamodel.Database) {
				diff.TablesAdded = difftypes.TableCreationsFor(desired, identifier.ForDialect("mysql"), "t2")
			}),
			want: []string{
				"REVOKE SELECT (`Name`) ON `t1` FROM `reader`",
				"GRANT SELECT (`name`) ON `t1` TO `reader`",
				"CREATE TABLE `t2` (\n  `id` INT PRIMARY KEY\n)",
			},
		},
		{
			// The control: a privilege revoked on another column is not the
			// declared one spelled otherwise, so the GRANT waits for the tables.
			name: "beside a revoke on another column",
			diff: respelledColumnPrivilege(func(diff *difftypes.SchemaDiff, desired *schemamodel.Database) {
				diff.TablesAdded = difftypes.TableCreationsFor(desired, identifier.ForDialect("mysql"), "t2")
				held := difftypes.GrantRef{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Column: "secret"}
				diff.GrantsRemoved = []difftypes.GrantRef{held}
				diff.CurrentGrants = []difftypes.GrantRef{held}
			}),
			want: []string{
				"REVOKE SELECT (`secret`) ON `t1` FROM `reader`",
				"CREATE TABLE `t2` (\n  `id` INT PRIMARY KEY\n)",
				"GRANT SELECT (`name`) ON `t1` TO `reader`",
			},
		},
		{
			name: "on a column the plan adds",
			diff: respelledColumnPrivilege(func(diff *difftypes.SchemaDiff, _ *schemamodel.Database) {
				diff.TablesModified = []difftypes.TableDiff{{TableName: "t1", ColumnsAdded: difftypes.ColumnChanges{
					{StructName: "T1", Name: "name", Type: "VARCHAR(50)", Nullable: true},
				}}}
			}),
			want: []string{
				"REVOKE SELECT (`Name`) ON `t1` FROM `reader`",
				"-- Modify table: t1\nALTER TABLE `t1` ADD COLUMN `name` VARCHAR(50)",
				"GRANT SELECT (`name`) ON `t1` TO `reader`",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), test.diff(), "mysql",
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}
