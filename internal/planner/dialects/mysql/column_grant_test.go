package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// columnGrantsChanged is a diff that grants SELECT on one column, revokes
// INSERT on another, adds the grant option to UPDATE on a third, and takes the
// grant option away from two column privileges of a second table.
func columnGrantsChanged() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsAdded: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "name"},
		},
		GrantOptionsAdded: []difftypes.GrantRef{
			{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id", WithOption: true},
		},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "secret"},
		},
		GrantOptionsRevoked: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t2", Column: "a", WithOption: true},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t2", Column: "b", WithOption: true},
		},
	}
}

// tableGrantNarrowed is a diff that replaces SELECT on the whole of t1 with
// SELECT on its name column, while the role keeps SELECT and INSERT on the id
// column, INSERT with the grant option, and SELECT on a column of t2. The
// grant option comes off t1 and stays on t2.
func tableGrantNarrowed() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsAdded: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Column: "name"},
		},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1"},
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "app.t1"},
		},
		GrantOptionsRevoked: []difftypes.GrantRef{
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id", WithOption: true},
		},
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1"},
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "app.t1"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id"},
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id", WithOption: true},
			{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t2", Column: "a", WithOption: true},
			{Role: "other", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id"},
		},
	}
}

// columnGrantWidened is a diff that replaces SELECT on the name column of t1,
// held with the grant option, with SELECT on the whole of t1 without it.
func columnGrantWidened() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsAdded: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1"},
		},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "name", WithOption: true},
		},
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "name", WithOption: true},
		},
	}
}

// tableGrantRevokedAll is a diff that revokes ALL on t1 from a role that
// keeps UPDATE on a column of t1 with the grant option.
func tableGrantRevokedAll() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "ALL", ObjectType: "TABLE", ObjectName: "app.t1"},
		},
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id", WithOption: true},
		},
	}
}

// tableAndColumnRevoked is a diff that revokes SELECT on the whole of t1 and
// SELECT on its secret column from a role that keeps SELECT on its id column.
func tableAndColumnRevoked() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "secret"},
		},
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "secret"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id"},
		},
	}
}

// columnSpelledOtherwise is a diff that grants UPDATE on the name column of t1
// to a role the catalog holds SELECT on it for, spelled Name.
func columnSpelledOtherwise() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsAdded: []difftypes.GrantRef{
			{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "app.t1", Column: "name"},
		},
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "Name"},
		},
	}
}

// columnRespelled is a diff that replaces SELECT on the Name column of t1 with
// SELECT on name, as a comparison that holds the two spellings apart finds it.
func columnRespelled() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsAdded: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "name"},
		},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "Name"},
		},
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "Name"},
		},
	}
}

// twoSpellingsHeldByAnother is [twoSpellingsHeld] with a diff that grants
// another role SELECT on the id column of t1 instead.
func twoSpellingsHeldByAnother() *difftypes.SchemaDiff {
	diff := twoSpellingsHeld("t1")
	diff.GrantsRemoved = nil
	diff.GrantsAdded = []difftypes.GrantRef{
		{Role: "writer", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id"},
	}
	return diff
}

// twoSpellingsHeld is a catalog holding, for one role, SELECT on Name and
// UPDATE on name of t1 and SELECT on Name of t2, with a diff that revokes the
// role's SELECT on Name of table.
func twoSpellingsHeld(table string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app." + table, Column: "Name"},
		},
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "Name"},
			{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "app.t1", Column: "name"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t2", Column: "Name"},
		},
	}
}

// A grant limited to a column is planned on that column and no wider, and the
// revokes go first: on MySQL and MariaDB a revoke on a table takes the column
// privileges of the same kind, and a revoke of the grant option takes it from
// every privilege on the table. The grant option of a column privilege is the
// grantee's at the table, so the two columns of t2 losing it plan one
// statement for t2, and a revoked privilege that carried it takes it along
// where the role keeps no privilege with it. A column privilege the role keeps
// comes back right after the table revoke that takes it, before the plan's own
// grants, with its grant option unless the diff revokes the option on that
// table; a privilege the table revoke does not take, or another role holds, is
// left alone. A column the catalog spells two ways for one role does not hold
// up a change on another table, or a change of another role's privileges on
// the same one.
func TestPlan_MySQLFamilyColumnGrants_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff func() *difftypes.SchemaDiff
		want []string
	}{
		{
			name: "column grants changed",
			diff: columnGrantsChanged,
			want: []string{
				"REVOKE GRANT OPTION ON `app`.`t2` FROM `reader`",
				"REVOKE INSERT (`secret`) ON `app`.`t1` FROM `reader`",
				"GRANT SELECT (`name`) ON `app`.`t1` TO `reader`",
				"GRANT UPDATE (`id`) ON `app`.`t1` TO `reader` WITH GRANT OPTION",
			},
		},
		{
			name: "a table grant narrowed to a column",
			diff: tableGrantNarrowed,
			want: []string{
				"REVOKE GRANT OPTION ON `app`.`t1` FROM `reader`",
				"REVOKE SELECT ON `app`.`t1` FROM `reader`",
				"REVOKE INSERT ON `app`.`t1` FROM `reader`",
				"GRANT SELECT (`id`) ON `app`.`t1` TO `reader`",
				"GRANT INSERT (`id`) ON `app`.`t1` TO `reader`",
				"GRANT SELECT (`name`) ON `t1` TO `reader`",
			},
		},
		{
			name: "a column grant with the option widened to the table without it",
			diff: columnGrantWidened,
			want: []string{
				"REVOKE GRANT OPTION ON `app`.`t1` FROM `reader`",
				"REVOKE SELECT (`name`) ON `app`.`t1` FROM `reader`",
				"GRANT SELECT ON `t1` TO `reader`",
			},
		},
		{
			name: "ALL revoked from a table",
			diff: tableGrantRevokedAll,
			want: []string{
				"REVOKE ALL ON `app`.`t1` FROM `reader`",
				"GRANT UPDATE (`id`) ON `app`.`t1` TO `reader` WITH GRANT OPTION",
			},
		},
		{
			// The table revoke takes SELECT (secret) with it, and the server
			// refuses a revoke of a privilege nothing holds with ERROR 1147, so
			// the column revoke is not planned and SELECT (secret) does not
			// come back with SELECT (id).
			name: "the whole table and a column it takes",
			diff: tableAndColumnRevoked,
			want: []string{
				"REVOKE SELECT ON `app`.`t1` FROM `reader`",
				"GRANT SELECT (`id`) ON `app`.`t1` TO `reader`",
			},
		},
		{
			name: "two spellings held on another table",
			diff: func() *difftypes.SchemaDiff { return twoSpellingsHeld("t2") },
			want: []string{"REVOKE SELECT (`Name`) ON `app`.`t2` FROM `reader`"},
		},
		{
			name: "two spellings held by another role",
			diff: twoSpellingsHeldByAnother,
			want: []string{"GRANT SELECT (`id`) ON `app`.`t1` TO `writer`"},
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				statements, err := planner.GenerateSchemaDiffSQLStatements(
					context.Background(), must.Must(builtin.New()), test.diff(), dialect,
				)

				c.Assert(err, qt.IsNil)
				c.Assert(statements, qt.DeepEquals, test.want)
			})
		}
	}
}

// A column is named as the declaration spells it on MySQL, where a column
// privilege held through a role serves only the spelling the GRANT used, and
// as the catalog spells it on MariaDB, which serves both. On MySQL a privilege
// moved from one spelling to the other is revoked under the old one first, so
// the catalog never holds the column under both.
func TestPlan_MySQLFamilyColumnSpelledOtherwise_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		diff    func() *difftypes.SchemaDiff
		want    []string
	}{
		{
			name:    "a privilege added",
			dialect: "mysql",
			diff:    columnSpelledOtherwise,
			want:    []string{"GRANT UPDATE (`name`) ON `app`.`t1` TO `reader`"},
		},
		{
			name:    "a privilege respelled",
			dialect: "mysql",
			diff:    columnRespelled,
			want: []string{
				"REVOKE SELECT (`Name`) ON `app`.`t1` FROM `reader`",
				"GRANT SELECT (`name`) ON `app`.`t1` TO `reader`",
			},
		},
		{
			name:    "a privilege added",
			dialect: "mariadb",
			diff:    columnSpelledOtherwise,
			want:    []string{"GRANT UPDATE (`Name`) ON `app`.`t1` TO `reader`"},
		},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), test.diff(), test.dialect,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// Oracle shares this planner and has no column grant spelling here, so a plan
// holding one is refused by name rather than granted on the whole table.
func TestPlan_MySQLFamilyColumnGrants_FailurePath(t *testing.T) {
	tests := []struct {
		dialect string
		wantErr string
	}{
		{dialect: "oracle", wantErr: `(?s).*column privileges are modeled for PostgreSQL, MySQL, MariaDB and SQL Server only, not oracle.*`},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{GrantsAdded: []difftypes.GrantRef{
				{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Column: "name"},
			}}

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), diff, test.dialect,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A change to a role's privileges on a table where the catalog holds one
// column under two spellings for it is refused: the server's in-memory copy
// keeps one of them, the table both, and a GRANT or REVOKE under either
// changes one and not the other.
func TestPlan_MySQLFamilyColumnSpellings_FailurePath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), twoSpellingsHeld("t1"), dialect,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches,
				`(?s).*role "reader" holds privileges on one column of app.t1 as "Name" and as "name".*revoke the privileges under both spellings by hand.*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// grantsAllOnT1 is t1 and a role, with the grants given: a declared state
// rather than a catalog read, so a GRANT ALL stays one grant of ALL.
func grantsAllOnT1(grants ...schemamodel.Grant) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T1", Name: "t1"}},
		Fields: []schemamodel.Field{
			{StructName: "T1", Name: "id", Type: "INT", Primary: true},
			{StructName: "T1", Name: "name", Type: "VARCHAR(9)", Nullable: true},
		},
		Roles:  []schemamodel.Role{{Name: "reader"}},
		Grants: grants,
	}
	schemamodel.Finalize(db)
	return db
}

// A comparison of two declared states keeps the ALL a state grants, so a plan
// from one holding ALL on t1 and SELECT on its id column revokes ALL, which
// takes the column privilege with it on MySQL and MariaDB. Kept, the column
// privilege comes back after the revoke; removed, it is not revoked a second
// time, which the server answers with ERROR 1147.
func TestPlan_MySQLFamilyRevokeAllBetweenTwoStates_HappyPath(t *testing.T) {
	all := schemamodel.Grant{Role: "reader", Privileges: []string{"ALL"}, OnTable: "t1"}
	id := schemamodel.Grant{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t1", Columns: []string{"id"}}
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    []string
	}{
		{
			name:    "the column privilege kept",
			desired: grantsAllOnT1(id),
			want:    []string{"REVOKE ALL ON `t1` FROM `reader`", "GRANT SELECT (`id`) ON `t1` TO `reader`"},
		},
		{
			name:    "the column privilege removed",
			desired: grantsAllOnT1(),
			want:    []string{"REVOKE ALL ON `t1` FROM `reader`"},
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				runtime := must.Must(builtin.New())
				diff, err := schemadiff.CompareSchemas(c.Context(), test.desired, grantsAllOnT1(all, id), dialect, runtime)
				c.Assert(err, qt.IsNil)

				statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), runtime, diff, dialect)

				c.Assert(err, qt.IsNil)
				c.Assert(statements, qt.DeepEquals, test.want)
			})
		}
	}
}
