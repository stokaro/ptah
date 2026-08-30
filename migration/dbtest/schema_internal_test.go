package dbtest

// White-box testing required: preserveUnmanagedObjects is an internal safety
// boundary that filters a precomputed live diff. SQLite introspection cannot
// reliably synthesize every case-equivalent replacement needed to exercise
// this policy through the public runner API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
		ConstraintsAdded: []string{"uq_users_name"},
		ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
			Name:      "uq_users_name",
			TableName: "users",
		}},
		ConstraintsRemoved: []string{"UQ_USERS_NAME"},
		ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{{
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
	c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, []string{"uq_users_name"})
	c.Assert(diff.ConstraintsRemovedWithTables, qt.DeepEquals, []difftypes.ConstraintRemovalInfo{{
		Name:      "uq_users_name",
		TableName: "users",
	}})
	c.Assert(diff.EnumsModified, qt.DeepEquals, []difftypes.EnumDiff{{
		EnumName:    "status",
		ValuesAdded: []string{"published"},
	}})
}

func TestPreserveUnmanagedObjects_NormalizesReplacementForPlanner(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("sqlite")
	diff := &difftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
		ConstraintsAdded:    []string{"check_users_name"},
		ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
			Name:            "check_users_name",
			TableName:       "users",
			Type:            "CHECK",
			CheckExpression: "name <> ''",
		}},
		ConstraintsRemoved: []string{"CHECK_USERS_NAME"},
		ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{{
			Name:      "CHECK_USERS_NAME",
			TableName: "USERS",
			Type:      "CHECK",
		}},
	}

	preserveUnmanagedObjects(diff, "postgres")
	sql, err := planner.GenerateSchemaDiffSQL(diff, &schemamodel.Database{}, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Matches, `(?s).*DROP CONSTRAINT IF EXISTS "check_users_name".*ADD CONSTRAINT "check_users_name".*`)
}
