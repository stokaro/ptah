package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// declaredDefaultPrivilege is one declaration, scoped to the dialects given and
// unscoped when none are.
func declaredDefaultPrivilege(dialects ...string) schemamodel.DefaultPrivilege {
	return schemamodel.DefaultPrivilege{
		StructName: "AccessControl",
		Grantor:    "app_owner",
		Schema:     "app",
		ObjectType: "TABLES",
		Grantee:    "app_reader",
		Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
		Dialects:   dialects,
	}
}

// describedDefaultPrivilege is the row a reader reports for that declaration.
func describedDefaultPrivilege() catalog.DefaultPrivilege {
	return catalog.DefaultPrivilege{
		Grantor:    "app_owner",
		Schema:     "app",
		ObjectType: "TABLES",
		Grantee:    "app_reader",
		Privilege:  "SELECT",
	}
}

// TestCompare_ADeclaredDefaultPrivilegeReachesTheDiff is the control for the
// wiring.
//
// Nothing reflects over the compare package and no test asserts that every
// comparator is invoked, so a comparator written and not called is dead code
// that compiles and passes its own unit tests while `ptah schema compare`
// reports a synced schema. This drives the public entry point instead.
//
// HasChanges is asserted beside the list for the same reason: it is a
// hand-written disjunction, so a family it does not read makes a filled diff
// answer false and every `--exit-code` pipeline pass.
func TestCompare_ADeclaredDefaultPrivilegeReachesTheDiff(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		DefaultPrivileges: []schemamodel.DefaultPrivilege{declaredDefaultPrivilege()},
	}

	diff := schemadiff.CompareWithDialect(desired, &catalog.Database{}, "postgres")

	c.Assert(diff.DefaultPrivilegesAdded, qt.HasLen, 1)
	c.Assert(diff.DefaultPrivilegesAdded[0].String(), qt.Equals,
		"SELECT on TABLES in app for app_owner to app_reader")
	c.Assert(diff.HasChanges(), qt.IsTrue)
}

// TestCompare_ADefaultPrivilegeMatchingItsRowIsNoChange is the control for the
// control. A comparator that planned every declaration would satisfy the test
// above and re-issue the same statement on every run.
func TestCompare_ADefaultPrivilegeMatchingItsRowIsNoChange(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Roles:             []schemamodel.Role{{Name: "app_owner"}},
		DefaultPrivileges: []schemamodel.DefaultPrivilege{declaredDefaultPrivilege()},
	}
	current := &catalog.Database{
		Roles:             []catalog.Role{{Name: "app_owner"}},
		DefaultPrivileges: []catalog.DefaultPrivilege{describedDefaultPrivilege()},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "postgres")

	c.Assert(diff.DefaultPrivilegesAdded, qt.HasLen, 0)
	c.Assert(diff.DefaultPrivilegesRemoved, qt.HasLen, 0)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// TestCompare_AScopedAwayDefaultPrivilegeIsNotPlannedForRemoval is the second
// half of the scope guarantee, for a family whose collected name is its whole
// identity rather than a bare role.
//
// The projection removes the declaration and leaves the database holding the
// object, so the comparison reads it as present in the target and absent from
// the declaration -- the shape of a revoke. The declared role is what makes the
// row removable at all here, which is what gives the suppression something to
// prevent.
func TestCompare_AScopedAwayDefaultPrivilegeIsNotPlannedForRemoval(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "a target the scope does not name", dialect: "postgres"},
		{name: "an accepted spelling of a target the scope does not name", dialect: "postgresql"},
		{name: "another target the scope does not name", dialect: "sqlite"},
		{name: "the named dialect, where both sides hold it", dialect: "mysql"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Roles:             []schemamodel.Role{{Name: "app_owner"}},
				DefaultPrivileges: []schemamodel.DefaultPrivilege{declaredDefaultPrivilege("mysql")},
			}
			current := &catalog.Database{
				Roles:             []catalog.Role{{Name: "app_owner"}},
				DefaultPrivileges: []catalog.DefaultPrivilege{describedDefaultPrivilege()},
			}

			diff := schemadiff.CompareWithDialect(desired, current, test.dialect)

			c.Assert(diff.DefaultPrivilegesRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompare_AnUndeclaredDefaultPrivilegeIsStillRemovedOnAScopedTarget keeps
// the guarantee above from being read as "never revoke anything". A suppression
// that emptied the described side wholesale would pass every row of it.
func TestCompare_AnUndeclaredDefaultPrivilegeIsStillRemovedOnAScopedTarget(t *testing.T) {
	c := qt.New(t)
	declaration := declaredDefaultPrivilege("mysql")
	declaration.Grantee = "somebody_else"
	desired := &schemamodel.Database{
		Roles:             []schemamodel.Role{{Name: "app_owner"}},
		DefaultPrivileges: []schemamodel.DefaultPrivilege{declaration},
	}
	current := &catalog.Database{
		Roles:             []catalog.Role{{Name: "app_owner"}},
		DefaultPrivileges: []catalog.DefaultPrivilege{describedDefaultPrivilege()},
	}

	diff := schemadiff.CompareWithDialect(desired, current, "postgres")

	c.Assert(diff.DefaultPrivilegesRemoved, qt.HasLen, 1)
	c.Assert(diff.DefaultPrivilegesRemoved[0].String(), qt.Equals,
		"SELECT on TABLES in app for app_owner to app_reader")
}
