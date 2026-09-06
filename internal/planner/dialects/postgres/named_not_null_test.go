package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// notNullNameDiff is one table whose column's NOT NULL constraint name moved
// from current to desired.
func notNullNameDiff(current, desired string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: "widget",
			ColumnsModified: []difftypes.ColumnDiff{{
				ColumnName: "a",
				Changes:    make(map[string]string),
				NotNullConstraintNameChange: &difftypes.NotNullConstraintNameChange{
					Current: current,
					Desired: desired,
				},
			}},
		}},
	}
}

// planNotNullName renders the plan for one name transition against a target
// whose named-NOT-NULL answer is the caller's.
func planNotNullName(c *qt.C, current, desired string, named bool) string {
	c.Helper()

	caps := capability.Postgres17().With(capability.NamedNotNullConstraints, named)
	nodes, err := postgres.NewForDialect(platform.Postgres, caps).
		GenerateMigrationAST(notNullNameDiff(current, desired))
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQLWithCapabilities(platform.Postgres, caps, nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// TestPlanner_ANameOnlyDriftIsRenamed pins the statement, and the reason it is
// a rename rather than a drop and an add.
//
// PostgreSQL generates a name for an unnamed NOT NULL, so re-establishing the
// constraint through `ALTER COLUMN ... SET NOT NULL` would land on the
// generated name rather than the declared one, and the column is momentarily
// nullable in between (stokaro/ptah#2161).
func TestPlanner_ANameOnlyDriftIsRenamed(t *testing.T) {
	c := qt.New(t)

	sql := planNotNullName(c, "c_old", "c_new", true)

	c.Assert(sql, qt.Contains, `RENAME CONSTRAINT "c_old" TO "c_new"`)
	c.Assert(sql, qt.Not(qt.Contains), "SET NOT NULL")
	c.Assert(sql, qt.Not(qt.Contains), "DROP CONSTRAINT")
}

// TestPlanner_ATargetWithoutTheCapabilitySaysSo is the gap a live run found.
//
// The renderer refuses a named NOT NULL at CREATE TABLE, but a column that
// already exists never reaches that path -- so the declared name vanished from
// the plan with no diagnostic at all, which is the silent drop this whole path
// exists to prevent. The plan now states it, naming the key and the release
// that added it, following the precedent the generated-column gate set.
func TestPlanner_ATargetWithoutTheCapabilitySaysSo(t *testing.T) {
	c := qt.New(t)

	sql := planNotNullName(c, "c_old", "c_new", false)

	c.Assert(sql, qt.Contains, "named_not_null_constraints")
	c.Assert(sql, qt.Contains, "c_new")
	c.Assert(sql, qt.Not(qt.Contains), "RENAME CONSTRAINT")
}

// TestPlanner_NothingToRenameFromIsNotARename is the third answer.
//
// The catalog reported no name, so there is no constraint to rename; the
// declared one has to be established with the column's own NOT NULL. Emitting
// a rename here would name a constraint that does not exist.
func TestPlanner_NothingToRenameFromIsNotARename(t *testing.T) {
	c := qt.New(t)

	sql := planNotNullName(c, "", "c_new", true)

	c.Assert(sql, qt.Not(qt.Contains), "RENAME CONSTRAINT")
}
