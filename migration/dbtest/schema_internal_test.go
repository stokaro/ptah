package dbtest

// White-box testing required: preserveUnmanagedObjects is an internal safety
// boundary that filters a precomputed live diff. SQLite introspection cannot
// reliably synthesize every case-equivalent replacement needed to exercise
// this policy through the public runner API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestPreserveUnmanagedObjects_UsesIdentifierSemantics(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("sqlite")
	diff := &difftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
		IndexesAdded:        difftypes.IndexChanges{{Index: schemamodel.Index{Name: "idx_users_name", Fields: []string{"name"}}, TableName: "users"}},
		IndexesRemoved: []difftypes.IndexRef{{
			Name:      "IDX_USERS_NAME",
			TableName: "USERS",
		}},
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
			Name:      "uq_users_name",
			TableName: "users",
		}},
		ConstraintsRemoved: []difftypes.ConstraintRemovalInfo{{
			Name:      "UQ_USERS_NAME",
			TableName: "USERS",
		}},
		EnumsModified: []difftypes.EnumDiff{
			{EnumName: "status", ValuesAdded: []string{"published"}, ValuesRemoved: []string{"legacy"}},
			{EnumName: "priority", ValuesRemoved: []string{"obsolete"}},
		},
	}

	preserveUnmanagedObjects(diff, "sqlite")

	c.Assert(diff.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{{
		Name:      "idx_users_name",
		TableName: "users",
	}})
	c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, []string{"uq_users_name"})
	c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, difftypes.ConstraintRemovals{{
		Name:      "uq_users_name",
		TableName: "users",
	}})
	c.Assert(diff.EnumsModified, qt.DeepEquals, []difftypes.EnumDiff{{
		EnumName:    "status",
		ValuesAdded: []string{"published"},
	}})
}

// TestPreserveUnmanagedObjects_KeepsTheDefaultPrivilegesTheDatabaseHolds pins
// this step as additive for the default-privilege family.
//
// A migration test runs against a database that may already carry default
// privileges no Go declaration describes -- another suite's, or the DBA's. They
// are outside this step's ownership, so applying a desired schema must not plan
// a REVOKE against them. The additions stay, because bringing the declared
// state up is exactly what this step is for.
func TestPreserveUnmanagedObjects_KeepsTheDefaultPrivilegesTheDatabaseHolds(t *testing.T) {
	c := qt.New(t)
	ref := difftypes.DefaultPrivilegeRef{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader", Privilege: "SELECT",
	}
	diff := &difftypes.SchemaDiff{
		DefaultPrivilegesAdded:         []difftypes.DefaultPrivilegeRef{ref},
		DefaultPrivilegesRemoved:       []difftypes.DefaultPrivilegeRef{ref},
		DefaultPrivilegeOptionsAdded:   []difftypes.DefaultPrivilegeRef{ref},
		DefaultPrivilegeOptionsRevoked: []difftypes.DefaultPrivilegeRef{ref},
	}

	preserveUnmanagedObjects(diff, "postgres")

	c.Assert(diff.DefaultPrivilegesRemoved, qt.IsNil)
	c.Assert(diff.DefaultPrivilegeOptionsRevoked, qt.IsNil)
	c.Assert(diff.DefaultPrivilegesAdded, qt.DeepEquals, []difftypes.DefaultPrivilegeRef{ref})
	c.Assert(diff.DefaultPrivilegeOptionsAdded, qt.DeepEquals, []difftypes.DefaultPrivilegeRef{ref})
}

func TestPreserveUnmanagedObjects_NormalizesReplacementForPlanner(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("sqlite")
	diff := &difftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
			Name:            "check_users_name",
			TableName:       "users",
			Type:            "CHECK",
			CheckExpression: "name <> ''",
		}},
		ConstraintsRemoved: []difftypes.ConstraintRemovalInfo{{
			Name:      "CHECK_USERS_NAME",
			TableName: "USERS",
			Type:      "CHECK",
		}},
	}

	preserveUnmanagedObjects(diff, "postgres")
	sql, err := planner.GenerateSchemaDiffSQL(diff, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Matches, `(?s).*DROP CONSTRAINT IF EXISTS "check_users_name".*ADD CONSTRAINT "check_users_name".*`)
}
