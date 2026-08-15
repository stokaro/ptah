package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// grantTarget is the part of a planned grant these rows tell apart: what the
// grant is about. Everything else agrees on both sides of every row here, so
// comparing it would state an agreement the rows are not about.
//
// The fields are exported because qt.DeepEquals compares through go-cmp, which
// refuses to read unexported ones rather than guess at their meaning.
type grantTarget struct {
	ObjectType string
	ObjectName string
}

// grantIdentityCase is one pair of spellings for the same grant, or for two
// grants that only look alike, with what the comparison owes each.
//
// The expectation is data rather than a closure. Every row asks one question
// -- which grants were planned -- and a row answering it with its own
// assertions hides that the question is shared, while handing the checker to a
// field the tooling cannot follow. See AGENTS.md, "A Table Row Carries Data,
// Not A Checker".
//
// Stating the whole projected list rather than a length also says more than
// the closures did: a row that planned one grant used to check only that it
// planned one.
type grantIdentityCase struct {
	name             string
	generated        []goschema.Grant
	roles            []goschema.Role
	database         []types.DBGrant
	wantAdded        []grantTarget
	wantRemoved      []grantTarget
	wantOptionsAdded int
}

// grantTargets projects planned grants onto what the rows compare, answering
// nil for none so a row that plans nothing says so by omission.
func grantTargets(grants []difftypes.GrantRef) []grantTarget {
	if len(grants) == 0 {
		return nil
	}
	targets := make([]grantTarget, 0, len(grants))
	for _, grant := range grants {
		targets = append(targets, grantTarget{ObjectType: grant.ObjectType, ObjectName: grant.ObjectName})
	}
	return targets
}

// TestGrantsWithSemantics_QualifiedTargetIdentity pins that a grant is matched
// by the object it is about rather than by the string that object was written
// as.
//
// The two sides never spelled it the same. A grant read from the catalog goes
// through [types.DBGrant.QualifiedTarget], which qualifies the target with the
// schema the reader found -- `"public"."granted"`. A grant declared in Go
// annotations or HCL carries whatever the author wrote, normally the bare
// `granted`. Keyed raw, one grant became two, so an unchanged schema planned a
// GRANT for the declaration and a REVOKE for the database row on every run.
//
// This is stokaro/ptah#1232's defect in a comparator that builds its own key,
// collected as an instance of stokaro/ptah#1276.
func TestGrantsWithSemantics_QualifiedTargetIdentity(t *testing.T) {
	tests := []grantIdentityCase{
		{
			// The headline row: identical grant, two spellings.
			name: "a declared bare table matches the qualified row the reader reports",
			generated: []goschema.Grant{
				{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "granted"},
			},
			database: []types.DBGrant{
				{Role: "app_user", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "granted"},
			},
		},
		{
			// The same in the other direction: the author qualified it and the
			// reader reported the same schema.
			name: "a declared qualified table matches the same qualified row",
			generated: []goschema.Grant{
				{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "public.granted"},
			},
			database: []types.DBGrant{
				{Role: "app_user", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "granted"},
			},
		},
		{
			// The control that stops the fix from becoming "everything
			// matches". A different schema is a different table.
			name: "the same table name in another schema is another grant",
			generated: []goschema.Grant{
				{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "other.granted"},
			},
			roles: []goschema.Role{{Name: "app_user"}},
			database: []types.DBGrant{
				{Role: "app_user", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "granted"},
			},
			wantAdded:   []grantTarget{{ObjectType: "TABLE", ObjectName: "other.granted"}},
			wantRemoved: []grantTarget{{ObjectType: "TABLE", ObjectName: "public.granted"}},
		},
		{
			// A SCHEMA grant's target is a schema, so there is no owning schema
			// to resolve. It must not pick up the default one and start
			// matching a table of the same name.
			name: "a schema grant does not collide with a table of that name",
			generated: []goschema.Grant{
				{Role: "app_user", Privileges: []string{"USAGE"}, OnSchema: "app"},
			},
			roles: []goschema.Role{{Name: "app_user"}},
			database: []types.DBGrant{
				{Role: "app_user", Privilege: "USAGE", ObjectType: "TABLE", Schema: "public", ObjectName: "app"},
			},
			wantAdded:   []grantTarget{{ObjectType: "SCHEMA", ObjectName: "app"}},
			wantRemoved: []grantTarget{{ObjectType: "TABLE", ObjectName: "public.app"}},
		},
		{
			// A schema grant still round-trips against its own row, which is
			// the control for the row above: that assertion must not pass
			// merely because schema grants stopped matching anything.
			name: "a schema grant matches its own row",
			generated: []goschema.Grant{
				{Role: "app_user", Privileges: []string{"USAGE"}, OnSchema: "app"},
			},
			database: []types.DBGrant{
				{Role: "app_user", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "app"},
			},
		},
		{
			// A sequence grant travels the same qualification path as a table
			// grant, so it gets the same normalization and the same control.
			name: "a declared bare sequence matches the qualified row",
			generated: []goschema.Grant{
				{Role: "app_user", Privileges: []string{"USAGE"}, OnSequence: "order_seq"},
			},
			database: []types.DBGrant{
				{Role: "app_user", Privilege: "USAGE", ObjectType: "SEQUENCE", Schema: "public", ObjectName: "order_seq"},
			},
		},
		{
			// The grant-option paths key through the same identity, so they
			// were unreachable for a qualified target too: this row reached
			// neither GrantOptionsAdded nor a plain re-GRANT.
			name: "a grant option is detected across the two spellings",
			generated: []goschema.Grant{
				{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "granted", WithOption: true},
			},
			roles: []goschema.Role{{Name: "app_user"}},
			database: []types.DBGrant{
				{Role: "app_user", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "granted"},
			},
			wantOptionsAdded: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{Grants: test.generated, Roles: test.roles}
			database := &types.DBSchema{Grants: test.database}
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithSemantics(generated, database, diff, identifier.ForDialect(platform.Postgres))

			c.Assert(grantTargets(diff.GrantsAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(grantTargets(diff.GrantsRemoved), qt.DeepEquals, test.wantRemoved)
			c.Assert(diff.GrantOptionsAdded, qt.HasLen, test.wantOptionsAdded)
		})
	}
}
