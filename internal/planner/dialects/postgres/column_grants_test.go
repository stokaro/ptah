package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
)

func columnPrivilege(role, privilege, column string, withOption bool) difftypes.GrantRef {
	return difftypes.GrantRef{
		Role: role, Privilege: privilege, ObjectType: "TABLE", ObjectName: "public.items",
		Column: column, WithOption: withOption,
	}
}

// A table revoke takes the column privileges of the same kind with it. The
// plan restores each column privilege it keeps, before its later statements.
func TestPlanner_TableRevokeKeepsColumnPrivileges(t *testing.T) {
	tableSelect := columnPrivilege("reader", "SELECT", "", false)
	columnSelect := columnPrivilege("reader", "SELECT", "a", true)
	ownerColumn := columnSelect
	ownerColumn.GrantedBy, ownerColumn.ObjectOwner = "owner", "owner"
	otherGrantorColumn := columnSelect
	otherGrantorColumn.GrantedBy, otherGrantorColumn.ObjectOwner = "delegator", "owner"
	tests := []struct {
		name string
		diff difftypes.SchemaDiff
		want []string
	}{
		{
			name: "held column privilege and grant option",
			diff: difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{tableSelect}, CurrentGrants: []difftypes.GrantRef{columnSelect}},
			want: []string{
				"REVOKE SELECT ON TABLE public.items FROM reader;",
				"GRANT SELECT (a) ON TABLE public.items TO reader WITH GRANT OPTION;",
			},
		},
		{
			name: "ALL takes each column privilege",
			diff: difftypes.SchemaDiff{
				GrantsRemoved: []difftypes.GrantRef{columnPrivilege("reader", "ALL", "", false)},
				CurrentGrants: []difftypes.GrantRef{columnSelect, columnPrivilege("reader", "UPDATE", "b", false)},
			},
			want: []string{
				"REVOKE ALL ON TABLE public.items FROM reader;",
				"GRANT SELECT (a) ON TABLE public.items TO reader WITH GRANT OPTION;",
				"GRANT UPDATE (b) ON TABLE public.items TO reader;",
			},
		},
		{
			name: "removed column privilege stays removed",
			diff: difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{tableSelect, columnSelect}, CurrentGrants: []difftypes.GrantRef{columnSelect}},
			want: []string{
				"REVOKE SELECT ON TABLE public.items FROM reader;",
				"REVOKE SELECT (a) ON TABLE public.items FROM reader;",
			},
		},
		{
			name: "another role and another privilege are unchanged",
			diff: difftypes.SchemaDiff{
				GrantsRemoved: []difftypes.GrantRef{tableSelect},
				CurrentGrants: []difftypes.GrantRef{columnPrivilege("other", "SELECT", "a", false), columnPrivilege("reader", "UPDATE", "a", false)},
			},
			want: []string{"REVOKE SELECT ON TABLE public.items FROM reader;"},
		},
		{
			name: "a planned column grant is emitted once",
			diff: difftypes.SchemaDiff{
				GrantsRemoved: []difftypes.GrantRef{tableSelect}, CurrentGrants: []difftypes.GrantRef{columnSelect},
				GrantsAdded: []difftypes.GrantRef{columnSelect},
			},
			want: []string{
				"REVOKE SELECT ON TABLE public.items FROM reader;",
				"GRANT SELECT (a) ON TABLE public.items TO reader WITH GRANT OPTION;",
			},
		},
		{
			name: "another grantor keeps its independent column grant",
			diff: difftypes.SchemaDiff{
				ConnectedRole: "owner", GrantsRemoved: []difftypes.GrantRef{tableSelect},
				CurrentGrants: []difftypes.GrantRef{otherGrantorColumn},
			},
			want: []string{"REVOKE SELECT ON TABLE public.items FROM reader;"},
		},
		{
			name: "a superuser restores the owner column grant",
			diff: difftypes.SchemaDiff{
				ConnectedRole: "admin", ConnectedRoleSuperuser: true,
				GrantsRemoved: []difftypes.GrantRef{tableSelect}, CurrentGrants: []difftypes.GrantRef{ownerColumn, otherGrantorColumn},
			},
			want: []string{
				"REVOKE SELECT ON TABLE public.items FROM reader;",
				"GRANT SELECT (a) ON TABLE public.items TO reader WITH GRANT OPTION;",
			},
		},
		{
			name: "a planned column grant option is emitted once",
			diff: difftypes.SchemaDiff{
				GrantsRemoved: []difftypes.GrantRef{tableSelect}, CurrentGrants: []difftypes.GrantRef{columnSelect},
				GrantOptionsAdded: []difftypes.GrantRef{columnSelect},
			},
			want: []string{
				"REVOKE SELECT ON TABLE public.items FROM reader;",
				"GRANT SELECT (a) ON TABLE public.items TO reader WITH GRANT OPTION;",
			},
		},
		{
			name: "a column revoke leaves another column alone",
			diff: difftypes.SchemaDiff{
				GrantsRemoved: []difftypes.GrantRef{columnPrivilege("reader", "SELECT", "b", false)}, CurrentGrants: []difftypes.GrantRef{columnSelect},
			},
			want: []string{"REVOKE SELECT (b) ON TABLE public.items FROM reader;"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := postgres.New().GenerateMigrationAST(c.Context(), must.Must(builtin.New()), &test.diff)
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Split(strings.TrimSpace(legacyRenderedSQL(sql)), "\n"), qt.DeepEquals, test.want)
		})
	}
}
