package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemachange"
	"go.5x5.cz/ptah/internal/schemastate"
)

// grantCatalog is a database where role `reporting` holds SELECT on a table.
func grantCatalog(withOption bool) *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "orders", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
		},
		Roles: []dbschematypes.DBRole{{Name: "reporting"}},
		Grants: []dbschematypes.DBGrant{{
			Role: "reporting", Privilege: "SELECT", ObjectType: "TABLE",
			Schema: "public", ObjectName: "orders", WithOption: withOption,
		}},
	}
}

// grantDescription declares the table, the roles named, and the grants given.
func grantDescription(roles []string, grants []goschema.Grant) *goschema.Database {
	description := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Order", Name: "orders"}},
		Fields: []goschema.Field{
			{StructName: "Order", Name: "id", Type: "integer", Primary: true},
		},
		Grants: grants,
	}
	for _, role := range roles {
		description.Roles = append(description.Roles,
			goschema.Role{StructName: "Access", Name: role})
	}
	return description
}

func grantChanges(c *qt.C, description *goschema.Database, catalog *dbschematypes.DBSchema) []schemachange.Change {
	c.Helper()
	profile := postgresProfile()
	rawDesired, err := schemastate.FromDescription(description, profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	rawCurrent, err := schemastate.FromCatalog(catalog, profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	desired, err := schemastate.Normalize(rawDesired, profile)
	c.Assert(err, qt.IsNil)
	current, err := schemastate.Normalize(rawCurrent, profile)
	c.Assert(err, qt.IsNil)
	changes, err := schemachange.Compare(current, desired, profile)
	c.Assert(err, qt.IsNil)
	kept := make([]schemachange.Change, 0)
	for _, change := range changes {
		kept = appendGrantChange(kept, change)
	}
	return kept
}

func appendGrantChange(kept []schemachange.Change, change schemachange.Change) []schemachange.Change {
	appenders := map[bool]func() []schemachange.Change{
		true:  func() []schemachange.Change { return append(kept, change) },
		false: func() []schemachange.Change { return kept },
	}
	return appenders[change.ID.Kind == objectidentity.KindGrant]()
}

// TestGrantToAnUnmanagedRoleIsNotRevoked pins the rule that makes a grant
// removal different from a table removal: the privileges of a role Ptah does
// not manage are not Ptah's to take away.
//
// The description here declares no roles at all, so it never claimed to
// describe anyone's privileges, and reading its silence as "revoke everything
// the database grants" would cut off access nobody asked to cut off.
func TestGrantToAnUnmanagedRoleIsNotRevoked(t *testing.T) {
	c := qt.New(t)

	changes := grantChanges(c, grantDescription(nil, nil), grantCatalog(false))

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Undecidable)
	c.Assert(changes[0].Diagnostic, qt.Contains, "does not manage role")
	c.Assert(changes[0].Diagnostic, qt.Contains, "reporting")
}

// TestGrantToAManagedRoleIsRevoked is the control the row above cannot be. A
// model that withheld every revocation would satisfy it and would never revoke
// anything.
func TestGrantToAManagedRoleIsRevoked(t *testing.T) {
	c := qt.New(t)

	changes := grantChanges(c, grantDescription([]string{"reporting"}, nil), grantCatalog(false))

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
}

// TestGrantIsNotRevokedWhenTheRoleFamilyIsNotDescribed is the same silence one
// level up: the description names the role, and then records that it did not
// describe the role family. It did not look, so it is not asking.
func TestGrantIsNotRevokedWhenTheRoleFamilyIsNotDescribed(t *testing.T) {
	c := qt.New(t)
	description := grantDescription([]string{"reporting"}, nil)
	description.NotDescribed = coverage.Set{}.WithKind(coverage.Role)

	changes := grantChanges(c, description, grantCatalog(false))

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Undecidable)
	c.Assert(changes[0].Diagnostic, qt.Contains, "not-described")
}

// TestGrantDeclaredOnBothSidesIsNoChange pins that the two sources resolve one
// grant to one identity.
//
// They do not spell it the same way: the catalog qualifies the object with the
// schema it found it in, and the declaration writes the bare table name. Keyed
// raw, one grant becomes two -- a GRANT and a REVOKE on every run of an
// unchanged schema, which is stokaro/ptah#1232 in a comparator that builds its
// own key.
func TestGrantDeclaredOnBothSidesIsNoChange(t *testing.T) {
	c := qt.New(t)
	description := grantDescription([]string{"reporting"}, []goschema.Grant{{
		StructName: "Access", Role: "reporting", Privileges: []string{"SELECT"}, OnTable: "orders",
	}})

	changes := grantChanges(c, description, grantCatalog(false))

	c.Assert(changes, qt.HasLen, 0)
}

// TestGrantOptionChangeIsAModification pins that WITH GRANT OPTION is a
// property of a grant rather than part of its identity.
func TestGrantOptionChangeIsAModification(t *testing.T) {
	c := qt.New(t)
	description := grantDescription([]string{"reporting"}, []goschema.Grant{{
		StructName: "Access", Role: "reporting", Privileges: []string{"SELECT"},
		OnTable: "orders", WithOption: true,
	}})

	changes := grantChanges(c, description, grantCatalog(false))

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Modify)
	c.Assert(changes[0].Changed, qt.DeepEquals, []string{"with grant option"})
}

// TestGrantIdentityKeepsTwoPrivilegesApart pins that a declaration naming two
// privileges is two grants.
//
// That is what the target holds: `GRANT SELECT, INSERT` is two catalog rows,
// and revoking one leaves the other. A model carrying the pair as one object
// cannot express the state that follows.
func TestGrantIdentityKeepsTwoPrivilegesApart(t *testing.T) {
	c := qt.New(t)
	description := grantDescription([]string{"reporting"}, []goschema.Grant{{
		StructName: "Access", Role: "reporting",
		Privileges: []string{"SELECT", "INSERT"}, OnTable: "orders",
	}})

	changes := grantChanges(c, description, grantCatalog(false))

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Add)
	c.Assert(changes[0].ID.Signature, qt.Equals, "INSERT")
}

// TestSchemaGrantDoesNotCollideWithATableGrant pins the case that decides where
// a grant's object lives in the identity.
//
// `GRANT USAGE ON SCHEMA app` and `GRANT USAGE ON TABLE app` are two grants of
// one privilege to one role, and a model that folded the object into a single
// name slot would hold only one of them.
func TestSchemaGrantDoesNotCollideWithATableGrant(t *testing.T) {
	c := qt.New(t)
	profile := postgresProfile()
	description := grantDescription([]string{"reporting"}, []goschema.Grant{
		{StructName: "Access", Role: "reporting", Privileges: []string{"USAGE"}, OnSchema: "app"},
		{StructName: "Access", Role: "reporting", Privileges: []string{"USAGE"}, OnTable: "app"},
	})

	state, err := schemastate.FromDescription(description, profile.Dialect, profile.Semantics)

	c.Assert(err, qt.IsNil)
	c.Assert(state.OfKind(objectidentity.KindGrant), qt.HasLen, 2)
}

// TestPartialRevokeIsNotAGrant pins that a row which SUBTRACTS from a broader
// grant is not read as one.
//
// Only ClickHouse produces it, and treating it as a grant would plan a REVOKE
// of a privilege the role already does not hold.
func TestPartialRevokeIsNotAGrant(t *testing.T) {
	c := qt.New(t)
	profile := postgresProfile()
	catalog := grantCatalog(false)
	catalog.Grants[0].IsPartialRevoke = true

	state, err := schemastate.FromCatalog(catalog, profile.Dialect, profile.Semantics)

	c.Assert(err, qt.IsNil)
	c.Assert(state.OfKind(objectidentity.KindGrant), qt.HasLen, 0)
}
