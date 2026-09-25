package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// plannedColumnGrant is a planned grant as these rows compare it.
type plannedColumnGrant struct {
	Role, Privilege, ObjectName, Column string
}

func plannedColumnGrants(refs []difftypes.GrantRef) []plannedColumnGrant {
	out := make([]plannedColumnGrant, 0, len(refs))
	for _, ref := range refs {
		out = append(out, plannedColumnGrant{Role: ref.Role, Privilege: ref.Privilege, ObjectName: ref.ObjectName, Column: ref.Column})
	}
	return out
}

func columnRow(privilege, column string) catalog.Grant {
	return catalog.Grant{Role: "app", Privilege: privilege, ObjectType: "TABLE", Schema: "public", ObjectName: "t", Column: column}
}

// TestGrants_Columns pins the comparison of column privileges, which the
// catalog reports one row per privilege and column. A revoke of the table
// privilege reaches the column rows too, as PostgreSQL's REVOKE does, except
// the columns the schema grants it on.
func TestGrants_Columns(t *testing.T) {
	tests := []struct {
		name        string
		desired     schemamodel.Database
		database    []catalog.Grant
		wantAdded   []plannedColumnGrant
		wantRemoved []plannedColumnGrant
	}{
		{
			name: "a column grant the database holds plans nothing",
			desired: schemamodel.Database{Grants: []schemamodel.Grant{{
				Role: "app", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"state"},
			}}},
			database: []catalog.Grant{columnRow("UPDATE", "state")},
		},
		{
			name: "a column grant the database lacks is added for that column",
			desired: schemamodel.Database{Grants: []schemamodel.Grant{{
				Role: "app", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"state", "note"},
			}}},
			database:  []catalog.Grant{columnRow("UPDATE", "state")},
			wantAdded: []plannedColumnGrant{{Role: "app", Privilege: "UPDATE", ObjectName: "t", Column: "note"}},
		},
		{
			name: "a table grant is not satisfied by a column row",
			desired: schemamodel.Database{Grants: []schemamodel.Grant{{
				Role: "app", Privileges: []string{"UPDATE"}, OnTable: "t",
			}}},
			database:  []catalog.Grant{columnRow("UPDATE", "state")},
			wantAdded: []plannedColumnGrant{{Role: "app", Privilege: "UPDATE", ObjectName: "t"}},
		},
		{
			name: "a table revoke removes the table row and the undeclared column rows",
			desired: schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"state"}}},
				RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"UPDATE"}, OnTable: "t"}},
			},
			database: []catalog.Grant{columnRow("UPDATE", ""), columnRow("UPDATE", "state"), columnRow("UPDATE", "other")},
			wantRemoved: []plannedColumnGrant{
				{Role: "app", Privilege: "UPDATE", ObjectName: "public.t"},
				{Role: "app", Privilege: "UPDATE", ObjectName: "public.t", Column: "other"},
			},
		},
		{
			name: "a column revoke removes only that column",
			desired: schemamodel.Database{RevokedGrants: []schemamodel.Grant{{
				Role: "app", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"secret"},
			}}},
			database:    []catalog.Grant{columnRow("SELECT", "secret"), columnRow("SELECT", "label")},
			wantRemoved: []plannedColumnGrant{{Role: "app", Privilege: "SELECT", ObjectName: "public.t", Column: "secret"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithSemantics(&test.desired, &catalog.Database{Grants: test.database}, diff,
				identifier.ForDialect(platform.Postgres))

			c.Assert(plannedColumnGrants(diff.GrantsAdded), qt.DeepEquals, append(make([]plannedColumnGrant, 0), test.wantAdded...))
			c.Assert(plannedColumnGrants(diff.GrantsRemoved), qt.DeepEquals, append(make([]plannedColumnGrant, 0), test.wantRemoved...))
		})
	}
}
