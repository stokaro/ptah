package dbtest

// White-box testing required: preserveUnmanagedObjects is an internal safety
// boundary that filters a precomputed live diff. SQLite introspection cannot
// reliably synthesize every case-equivalent replacement needed to exercise
// this policy through the public runner API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/migration/planner"
	schemadifftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestPreserveUnmanagedObjects_UsesIdentifierSemantics(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("sqlite")
	diff := &schemadifftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
		IndexesAdded: []schemadifftypes.IndexRef{{
			Name:      "idx_users_name",
			TableName: "users",
		}},
		IndexesRemoved: []schemadifftypes.IndexRef{{
			Name:      "IDX_USERS_NAME",
			TableName: "USERS",
		}},
		ConstraintsAdded: []string{"uq_users_name"},
		ConstraintsAddedWithTables: []schemadifftypes.ConstraintAdditionInfo{{
			Name:      "uq_users_name",
			TableName: "users",
		}},
		ConstraintsRemoved: []string{"UQ_USERS_NAME"},
		ConstraintsRemovedWithTables: []schemadifftypes.ConstraintRemovalInfo{{
			Name:      "UQ_USERS_NAME",
			TableName: "USERS",
		}},
		EnumsModified: []schemadifftypes.EnumDiff{
			{EnumName: "status", ValuesAdded: []string{"published"}, ValuesRemoved: []string{"legacy"}},
			{EnumName: "priority", ValuesRemoved: []string{"obsolete"}},
		},
	}

	preserveUnmanagedObjects(diff, "sqlite")

	c.Assert(diff.IndexesRemoved, qt.DeepEquals, []schemadifftypes.IndexRef{{
		Name:      "idx_users_name",
		TableName: "users",
	}})
	c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, []string{"uq_users_name"})
	c.Assert(diff.ConstraintsRemovedWithTables, qt.DeepEquals, []schemadifftypes.ConstraintRemovalInfo{{
		Name:      "uq_users_name",
		TableName: "users",
	}})
	c.Assert(diff.EnumsModified, qt.DeepEquals, []schemadifftypes.EnumDiff{{
		EnumName:    "status",
		ValuesAdded: []string{"published"},
	}})
}

func TestPreserveUnmanagedObjects_NormalizesReplacementForPlanner(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("sqlite")
	diff := &schemadifftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
		ConstraintsAdded:    []string{"check_users_name"},
		ConstraintsAddedWithTables: []schemadifftypes.ConstraintAdditionInfo{{
			Name:            "check_users_name",
			TableName:       "users",
			Type:            "CHECK",
			CheckExpression: "name <> ''",
		}},
		ConstraintsRemoved: []string{"CHECK_USERS_NAME"},
		ConstraintsRemovedWithTables: []schemadifftypes.ConstraintRemovalInfo{{
			Name:      "CHECK_USERS_NAME",
			TableName: "USERS",
			Type:      "CHECK",
		}},
	}

	preserveUnmanagedObjects(diff, "postgres")
	sql, err := planner.GenerateSchemaDiffSQL(diff, &goschema.Database{}, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Matches, `(?s).*DROP CONSTRAINT IF EXISTS "check_users_name".*ADD CONSTRAINT "check_users_name".*`)
}
