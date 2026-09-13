package generator

// White-box testing required: cloneSchemaDiff is unexported, and aliasing is
// not observable from outside. The exported surface returns rendered SQL, which
// is identical whether the clone shares the caller's slice or copies it -- the
// damage shows up in the caller's own diff afterwards, which no rendered output
// reports.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/schemadiff/difftypes"
)

// TestCloneSchemaDiff_CopiesTheDefaultPrivilegeLists holds the clone against
// the way it is written.
//
// It opens with `clone := *diff`, so every slice it does not name is SHARED
// with the caller's diff rather than copied. Writing through the clone then
// edits the forward diff the caller is still holding, and the plan that diff
// renders afterwards is not the plan it was asked for.
//
// All four lists are written through, each with a different value, so a clone
// that copied one of them and aliased the next fails on the one it aliased
// rather than passing on the one it copied.
func TestCloneSchemaDiff_CopiesTheDefaultPrivilegeLists(t *testing.T) {
	c := qt.New(t)
	declared := difftypes.DefaultPrivilegeRef{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader", Privilege: "SELECT",
	}
	forward := []difftypes.DefaultPrivilegeRef{declared}
	diff := &difftypes.SchemaDiff{
		DefaultPrivilegesAdded:         []difftypes.DefaultPrivilegeRef{declared},
		DefaultPrivilegesRemoved:       []difftypes.DefaultPrivilegeRef{declared},
		DefaultPrivilegeOptionsAdded:   []difftypes.DefaultPrivilegeRef{declared},
		DefaultPrivilegeOptionsRevoked: []difftypes.DefaultPrivilegeRef{declared},
	}

	clone := cloneSchemaDiff(diff)
	clone.DefaultPrivilegesAdded[0].Grantee = "added_elsewhere"
	clone.DefaultPrivilegesRemoved[0].Grantee = "removed_elsewhere"
	clone.DefaultPrivilegeOptionsAdded[0].Grantee = "granted_elsewhere"
	clone.DefaultPrivilegeOptionsRevoked[0].Grantee = "revoked_elsewhere"

	c.Assert(diff.DefaultPrivilegesAdded, qt.DeepEquals, forward)
	c.Assert(diff.DefaultPrivilegesRemoved, qt.DeepEquals, forward)
	c.Assert(diff.DefaultPrivilegeOptionsAdded, qt.DeepEquals, forward)
	c.Assert(diff.DefaultPrivilegeOptionsRevoked, qt.DeepEquals, forward)
}

// TestReverseSchemaDiff_SwapsTheDefaultPrivilegeDirections pins the rollback.
//
// A default privilege reverses like a grant: what the up direction granted, the
// down direction revokes, and a grant option added comes back as one revoked.
// The reverse builder is a struct literal, and a literal is silent about the
// fields it omits -- a family left out rolls back to nothing at all.
func TestReverseSchemaDiff_SwapsTheDefaultPrivilegeDirections(t *testing.T) {
	c := qt.New(t)
	granted := difftypes.DefaultPrivilegeRef{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader", Privilege: "SELECT",
	}
	revoked := difftypes.DefaultPrivilegeRef{
		Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES",
		Grantee: "app_writer", Privilege: "USAGE",
	}

	reversed := reverseSchemaDiffWithSchema(&difftypes.SchemaDiff{
		DefaultPrivilegesAdded:       []difftypes.DefaultPrivilegeRef{granted},
		DefaultPrivilegeOptionsAdded: []difftypes.DefaultPrivilegeRef{revoked},
	}, nil, nil)

	c.Assert(reversed.DefaultPrivilegesRemoved, qt.DeepEquals, []difftypes.DefaultPrivilegeRef{granted})
	c.Assert(reversed.DefaultPrivilegesAdded, qt.HasLen, 0)
	c.Assert(reversed.DefaultPrivilegeOptionsRevoked, qt.DeepEquals, []difftypes.DefaultPrivilegeRef{revoked})
	c.Assert(reversed.DefaultPrivilegeOptionsAdded, qt.HasLen, 0)
}
