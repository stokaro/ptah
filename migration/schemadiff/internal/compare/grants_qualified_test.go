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

// grantIdentityCase is one pair of spellings for the same grant, or for two
// grants that only look alike, with what the comparison owes each.
type grantIdentityCase struct {
	name      string
	generated []goschema.Grant
	roles     []goschema.Role
	database  []types.DBGrant
	assert    func(c *qt.C, diff *difftypes.SchemaDiff)
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
	c := qt.New(t)

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
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.GrantsAdded, qt.HasLen, 0)
				c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
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
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.GrantsAdded, qt.HasLen, 0)
				c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
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
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.GrantsAdded, qt.HasLen, 1)
				c.Assert(diff.GrantsAdded[0].ObjectName, qt.Equals, "other.granted")
				c.Assert(diff.GrantsRemoved, qt.HasLen, 1)
			},
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
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.GrantsAdded, qt.HasLen, 1)
				c.Assert(diff.GrantsAdded[0].ObjectType, qt.Equals, "SCHEMA")
				c.Assert(diff.GrantsRemoved, qt.HasLen, 1)
				c.Assert(diff.GrantsRemoved[0].ObjectType, qt.Equals, "TABLE")
			},
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
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.GrantsAdded, qt.HasLen, 0)
				c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
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
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.GrantsAdded, qt.HasLen, 0)
				c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
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
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.GrantsAdded, qt.HasLen, 0)
				c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
				c.Assert(diff.GrantOptionsAdded, qt.HasLen, 1)
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			generated := &goschema.Database{Grants: test.generated, Roles: test.roles}
			database := &types.DBSchema{Grants: test.database}
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithSemantics(generated, database, diff, identifier.ForDialect(platform.Postgres))

			test.assert(c, diff)
		})
	}
}
