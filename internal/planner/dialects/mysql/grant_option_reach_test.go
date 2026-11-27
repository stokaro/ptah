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

// q2Select is SELECT on dbo.t48 held by grantee from grantor.
func q2Select(grantee, grantor string, withOption bool) difftypes.GrantRef {
	return difftypes.GrantRef{
		Role: grantee, Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "dbo.t48", WithOption: withOption, GrantedBy: grantor,
	}
}

// A revoke of the grant option on SQL Server carries CASCADE, which takes the
// grants its grantee made with the option. Where q2 granted SELECT onward to
// q2v and the diff does not revoke that grant, the plan is refused and names
// q2v: for the option the diff revokes, and for the option a removal takes
// first.
func TestPlan_SQLServerRefusesAGrantOptionRevokeThatCascades(t *testing.T) {
	held := []difftypes.GrantRef{q2Select("q2", "dbo", true), q2Select("q2v", "dbo", false), q2Select("q2v", "q2", false)}
	for _, test := range []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{
			name: "the option revoked",
			diff: &difftypes.SchemaDiff{GrantOptionsRevoked: []difftypes.GrantRef{q2Select("q2", "dbo", true)}, CurrentGrants: held},
		},
		{
			name: "a grantable privilege removed",
			diff: &difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{q2Select("q2", "dbo", true)}, CurrentGrants: held},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), test.diff, "sqlserver")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*sqlserver: cannot revoke the grant option for SELECT on dbo\.t48 from "q2": `+
				`SQL Server revokes it with CASCADE, which also takes the SELECT that "q2" granted to "q2v", and the schema `+
				`does not revoke those grants; revoke them first, or declare their REVOKE in the schema`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// The controls: a grantee that granted nothing onward, a grant of the
// privilege from another grantor, and an onward grant the diff revokes itself
// are no reason to refuse, and the plan revokes the option with CASCADE.
func TestPlan_SQLServerRevokesAGrantOptionThatCascadesToNothingKept(t *testing.T) {
	for _, test := range []struct {
		name    string
		removed []difftypes.GrantRef
		held    []difftypes.GrantRef
		want    []string
	}{
		{
			name: "no onward grant",
			held: []difftypes.GrantRef{q2Select("q2", "dbo", true), q2Select("q2v", "dbo", false)},
			want: []string{"REVOKE GRANT OPTION FOR SELECT ON [dbo].[t48] FROM [q2] CASCADE AS [dbo]"},
		},
		{
			name:    "an onward grant the diff revokes",
			removed: []difftypes.GrantRef{q2Select("q2v", "q2", false)},
			held:    []difftypes.GrantRef{q2Select("q2", "dbo", true), q2Select("q2v", "q2", false)},
			want: []string{
				"REVOKE GRANT OPTION FOR SELECT ON [dbo].[t48] FROM [q2] CASCADE AS [dbo]",
				"REVOKE SELECT ON [dbo].[t48] FROM [q2v] AS [q2]",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				GrantOptionsRevoked: []difftypes.GrantRef{q2Select("q2", "dbo", true)},
				GrantsRemoved:       test.removed,
				CurrentGrants:       test.held,
			}

			statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, "sqlserver")

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// t1Privilege is privilege on t1 held by reader with the grant option, on
// column when it is not empty.
func t1Privilege(privilege, column string) difftypes.GrantRef {
	return difftypes.GrantRef{Role: "reader", Privilege: privilege, ObjectType: "TABLE", ObjectName: "t1", Column: column, WithOption: true}
}

// On MySQL and MariaDB the grant option belongs to the grantee at the whole
// object. A diff that takes it for SELECT while the role holds INSERT, or
// UPDATE on a column, with the option and keeps that grantable asks for what
// the server cannot do, and the plan is refused and names the privilege.
func TestPlan_MySQLFamilyRefusesAGrantOptionRevokeThatTakesMore(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range []struct {
			name, wantErr string
			kept          difftypes.GrantRef
		}{
			{name: "another privilege", kept: t1Privilege("INSERT", ""), wantErr: "INSERT"},
			{name: "a column privilege", kept: t1Privilege("UPDATE", "id"), wantErr: `UPDATE \(id\)`},
		} {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				diff := &difftypes.SchemaDiff{
					GrantOptionsRevoked: []difftypes.GrantRef{t1Privilege("SELECT", "")},
					CurrentGrants:       []difftypes.GrantRef{t1Privilege("SELECT", ""), test.kept},
				}

				statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, dialect)

				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err, qt.ErrorMatches, `(?s).*`+dialect+`: cannot revoke the grant option for SELECT on t1 from "reader": `+
					`the server keeps the grant option at the whole object, not at one privilege, so the revoke also takes it from `+
					test.wantErr+`, which the schema keeps grantable; revoke the grant option for every privilege "reader" `+
					`holds on t1, or for none of them`)
				c.Assert(statements, qt.IsNil)
			})
		}
	}
}

// The controls: a diff that takes the option for every privilege the role
// holds with it, or removes the other privilege, takes it in one statement.
func TestPlan_MySQLFamilyRevokesAGrantOptionItTakesWhole(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range []struct {
			name             string
			revoked, removed []difftypes.GrantRef
			want             []string
		}{
			{
				name:    "the option for every privilege",
				revoked: []difftypes.GrantRef{t1Privilege("SELECT", ""), t1Privilege("INSERT", "")},
				want:    []string{"REVOKE GRANT OPTION ON `t1` FROM `reader`"},
			},
			{
				name:    "the other privilege removed",
				revoked: []difftypes.GrantRef{t1Privilege("SELECT", "")},
				removed: []difftypes.GrantRef{t1Privilege("INSERT", "")},
				want:    []string{"REVOKE GRANT OPTION ON `t1` FROM `reader`", "REVOKE INSERT ON `t1` FROM `reader`"},
			},
		} {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				diff := &difftypes.SchemaDiff{
					GrantOptionsRevoked: test.revoked,
					GrantsRemoved:       test.removed,
					CurrentGrants:       []difftypes.GrantRef{t1Privilege("SELECT", ""), t1Privilege("INSERT", "")},
				}

				statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, dialect)

				c.Assert(err, qt.IsNil)
				c.Assert(statements, qt.DeepEquals, test.want)
			})
		}
	}
}
