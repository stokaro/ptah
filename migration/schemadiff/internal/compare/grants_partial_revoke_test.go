package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// The semantics argument is not incidental: the plain [compare.Grants] entry
// point resolves no default schema, so a declared bare `orders` and a catalog's
// `public.orders` are two grants there. Using it would have this test measuring
// stokaro/ptah#1232 rather than the partial revoke.
//
// TestGrants_APartialRevokeIsNotAGrantToRevoke pins that a row which SUBTRACTS
// a privilege is not compared as one that grants it.
//
// ClickHouse produces such a row for a partial revoke and SQL Server for a
// DENY. Entering it in the removal map turns it into a REVOKE of a privilege
// the role already does not hold -- which does not narrow anything, and on SQL
// Server removes the exception the DBA put there.
//
// internal/convert/dbschematogo has skipped these rows since ClickHouse's
// reader produced the first one. This comparator did not, and the gap stayed
// unreachable only because clickhouserbac.ValidateLive refuses a managed role
// carrying one before the comparison runs (stokaro/ptah#1698).
func TestGrants_APartialRevokeIsNotAGrantToRevoke(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Roles:  []goschema.Role{{Name: "reporting"}},
		Grants: []goschema.Grant{{Role: "reporting", Privileges: []string{"SELECT"}, OnTable: "orders"}},
	}
	database := &types.DBSchema{
		Roles: []types.DBRole{{Name: "reporting"}},
		Grants: []types.DBGrant{
			{Role: "reporting", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "orders"},
			{
				Role: "reporting", Privilege: "DELETE", ObjectType: "TABLE",
				Schema: "public", ObjectName: "orders", IsPartialRevoke: true,
			},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.GrantsWithSemantics(generated, database, diff, identifier.ForDialect("postgres"))

	c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
}

// TestGrants_APlainGrantIsStillRevoked is the control. A comparator that
// skipped every database row would satisfy the row above and would never
// revoke anything.
func TestGrants_APlainGrantIsStillRevoked(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{Roles: []goschema.Role{{Name: "reporting"}}}
	database := &types.DBSchema{
		Roles: []types.DBRole{{Name: "reporting"}},
		Grants: []types.DBGrant{
			{Role: "reporting", Privilege: "DELETE", ObjectType: "TABLE", Schema: "public", ObjectName: "orders"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.GrantsWithSemantics(generated, database, diff, identifier.ForDialect("postgres"))

	c.Assert(diff.GrantsRemoved, qt.HasLen, 1)
	c.Assert(diff.GrantsRemoved[0].Privilege, qt.Equals, "DELETE")
}

// TestGrants_APartialRevokeDoesNotSatisfyADeclaration is the other half: a row
// saying the role LOST a privilege must not make a declaration asking for that
// privilege look already satisfied.
func TestGrants_APartialRevokeDoesNotSatisfyADeclaration(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Roles:  []goschema.Role{{Name: "reporting"}},
		Grants: []goschema.Grant{{Role: "reporting", Privileges: []string{"DELETE"}, OnTable: "orders"}},
	}
	database := &types.DBSchema{
		Roles: []types.DBRole{{Name: "reporting"}},
		Grants: []types.DBGrant{{
			Role: "reporting", Privilege: "DELETE", ObjectType: "TABLE",
			Schema: "public", ObjectName: "orders", IsPartialRevoke: true,
		}},
	}
	diff := &difftypes.SchemaDiff{}

	compare.GrantsWithSemantics(generated, database, diff, identifier.ForDialect("postgres"))

	c.Assert(diff.GrantsAdded, qt.HasLen, 1)
	c.Assert(diff.GrantsAdded[0].Privilege, qt.Equals, "DELETE")
}
