package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// keptBeside is a catalog holding, for one role, SELECT, UPDATE and INSERT on
// Name and SELECT on id of t1, and SELECT on Name of t2, with a diff that
// removes and adds the grants given.
func keptBeside(removed, added []difftypes.GrantRef) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsRemoved: removed,
		GrantsAdded:   added,
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "Name"},
			{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "app.t1", Column: "Name"},
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "Name"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t2", Column: "Name"},
		},
	}
}

// columnRef is the role's privilege on one column of one table.
func columnRef(privilege, table, column string) difftypes.GrantRef {
	return difftypes.GrantRef{Role: "reader", Privilege: privilege, ObjectType: "TABLE", ObjectName: "app." + table, Column: column}
}

// A plan that leaves a role holding one column under a second spelling is
// refused before it runs. On MySQL a grant takes the declared spelling, so
// SELECT (name) granted beside an UPDATE and an INSERT the role keeps on Name
// gives the catalog both spellings, and the next plan on the table is refused
// for that.
func TestPlan_MySQLSecondColumnSpelling_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{
			name: "a respelled privilege beside kept ones",
			diff: keptBeside(
				[]difftypes.GrantRef{columnRef("SELECT", "t1", "Name")},
				[]difftypes.GrantRef{columnRef("SELECT", "t1", "name")},
			),
			wantErr: `unsupported feature: role "reader" keeps INSERT, UPDATE on one column of app.t1 as "Name", ` +
				`and the plan grants SELECT on it as "name", the declared spelling; MYSQL would then hold the column ` +
				`under two spellings for the role, and a later plan on app.t1 is refused for that -- declare the ` +
				`role's privileges on the column as "name" too, or revoke them as "Name"`,
		},
		{
			name:    "a new privilege beside kept ones",
			diff:    keptBeside(nil, []difftypes.GrantRef{columnRef("REFERENCES", "t1", "name")}),
			wantErr: `unsupported feature: role "reader" keeps INSERT, SELECT, UPDATE on one column of app.t1 as "Name", and the plan grants REFERENCES on it as "name".*`,
		},
		{
			name:    "a privilege added beside one held otherwise",
			diff:    columnSpelledOtherwise(),
			wantErr: `unsupported feature: role "reader" keeps SELECT on one column of app.t1 as "Name", and the plan grants UPDATE on it as "name".*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), test.diff, "mysql",
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A grant under the declared spelling is planned where the role keeps nothing
// else on the column under another: every privilege on it respelled, a grant
// on another column, and one on another table. MariaDB grants under the
// catalog's spelling, so a grant there never adds a second one.
func TestPlan_MySQLSecondColumnSpelling_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		diff    *difftypes.SchemaDiff
		want    []string
	}{
		{
			name:    "every privilege on the column respelled",
			dialect: "mysql",
			diff: keptBeside(
				[]difftypes.GrantRef{
					columnRef("SELECT", "t1", "Name"), columnRef("UPDATE", "t1", "Name"), columnRef("INSERT", "t1", "Name"),
				},
				[]difftypes.GrantRef{columnRef("SELECT", "t1", "name")},
			),
			want: []string{
				"REVOKE SELECT (`Name`) ON `app`.`t1` FROM `reader`",
				"REVOKE UPDATE (`Name`) ON `app`.`t1` FROM `reader`",
				"REVOKE INSERT (`Name`) ON `app`.`t1` FROM `reader`",
				"GRANT SELECT (`name`) ON `app`.`t1` TO `reader`",
			},
		},
		{
			name:    "a grant on another column",
			dialect: "mysql",
			diff:    keptBeside(nil, []difftypes.GrantRef{columnRef("UPDATE", "t1", "id")}),
			want:    []string{"GRANT UPDATE (`id`) ON `app`.`t1` TO `reader`"},
		},
		{
			name:    "a grant on another table",
			dialect: "mysql",
			diff: keptBeside(
				[]difftypes.GrantRef{columnRef("SELECT", "t2", "Name")},
				[]difftypes.GrantRef{columnRef("SELECT", "t2", "name")},
			),
			want: []string{
				"REVOKE SELECT (`Name`) ON `app`.`t2` FROM `reader`",
				"GRANT SELECT (`name`) ON `app`.`t2` TO `reader`",
			},
		},
		{
			name:    "a new privilege beside kept ones",
			dialect: "mariadb",
			diff:    keptBeside(nil, []difftypes.GrantRef{columnRef("REFERENCES", "t1", "name")}),
			want:    []string{"GRANT REFERENCES (`Name`) ON `app`.`t1` TO `reader`"},
		},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), test.diff, test.dialect,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}
