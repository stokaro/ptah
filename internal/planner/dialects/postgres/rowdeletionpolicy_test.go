package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// policyDiff is a diff whose only content is one table's row deletion policy
// transition.
func policyDiff(desired, current *ast.RowDeletionPolicySpec) *types.SchemaDiff {
	return &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName: "sessions",
			RowDeletionPolicyChange: &types.RowDeletionPolicyChange{
				Desired: desired, Current: current,
			},
		}},
	}
}

// TestPlanner_RowDeletionPolicyTransitions pins the statement each transition
// produces.
//
// Every expectation was executed against the Cloud Spanner emulator behind
// PGAdapter 0.55.2 and read back from
// information_schema.tables.row_deletion_policy_expression, which is what makes
// these strings a claim about convergence rather than about formatting
// (stokaro/ptah#2236).
func TestPlanner_RowDeletionPolicyTransitions(t *testing.T) {
	tests := []struct {
		name    string
		desired *ast.RowDeletionPolicySpec
		current *ast.RowDeletionPolicySpec
		want    []string
	}{
		{
			name:    "adding a policy",
			desired: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			current: nil,
			want:    []string{`ALTER TABLE "sessions" ADD TTL INTERVAL '30 days' ON "created_at";`},
		},
		{
			// ADD and ALTER are not interchangeable: the server refuses each in
			// the other's position, which is why the diff carries both sides.
			name:    "changing the interval",
			desired: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "60 days"},
			current: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			want:    []string{`ALTER TABLE "sessions" ALTER TTL INTERVAL '60 days' ON "created_at";`},
		},
		{
			name:    "moving the policy to another column",
			desired: &ast.RowDeletionPolicySpec{Column: "updated_at", Interval: "30 days"},
			current: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			want:    []string{`ALTER TABLE "sessions" ALTER TTL INTERVAL '30 days' ON "updated_at";`},
		},
		{
			// The removal names no column: the clause goes and the timestamp
			// column it referred to stays.
			name:    "dropping the policy",
			desired: nil,
			current: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			want:    []string{`ALTER TABLE "sessions" DROP TTL;`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements := planRowDeletionPolicy(c, policyDiff(test.desired, test.current))

			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// TestPlanner_ThePolicyIsRetargetedBeforeItsColumnIsDropped pins the ORDER, which
// is the half a per-transition test cannot see.
//
// A migration that moves a policy off a column and drops that column is one
// plan, and the two statements only work in one order: the column the policy
// still names cannot be dropped while it names it. Placed after the column
// removal — where it originally sat, beside the row-level TTL step — the plan
// reads correctly statement by statement and fails as a whole.
func TestPlanner_ThePolicyIsRetargetedBeforeItsColumnIsDropped(t *testing.T) {
	c := qt.New(t)

	statements := planRowDeletionPolicy(c, &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName:      "sessions",
			ColumnsRemoved: []string{"created_at"},
			RowDeletionPolicyChange: &types.RowDeletionPolicyChange{
				Desired: &ast.RowDeletionPolicySpec{Column: "updated_at", Interval: "30 days"},
				Current: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			},
		}},
	})

	c.Assert(indexOfStatement(c, statements, "ALTER TTL") <
		indexOfStatement(c, statements, "DROP COLUMN"), qt.IsTrue,
		qt.Commentf("statements were: %v", statements))
}

// TestPlanner_TheDroppedPolicyGoesBeforeItsColumn is the same ordering for the
// other transition, because the removal names no column and could look
// order-independent.
func TestPlanner_TheDroppedPolicyGoesBeforeItsColumn(t *testing.T) {
	c := qt.New(t)

	statements := planRowDeletionPolicy(c, &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName:      "sessions",
			ColumnsRemoved: []string{"created_at"},
			RowDeletionPolicyChange: &types.RowDeletionPolicyChange{
				Current: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
			},
		}},
	})

	c.Assert(indexOfStatement(c, statements, "DROP TTL") <
		indexOfStatement(c, statements, "DROP COLUMN"), qt.IsTrue,
		qt.Commentf("statements were: %v", statements))
}

// TestPlanner_RowDeletionPolicyIsNotPlannedWithoutTheCapability pins the gate:
// a target without the capability gets the renderer's measured refusal rather
// than an ALTER the server rejects halfway through a migration.
func TestPlanner_RowDeletionPolicyIsNotPlannedWithoutTheCapability(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{platform.Postgres, capability.Postgres17()},
		{platform.CockroachDB, capability.CockroachDB26()},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			planner := postgres.NewForDialect(test.dialect, test.caps)

			nodes, err := planner.GenerateMigrationASTChecked(policyDiff(
				&ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"}, nil), nil)

			c.Assert(err, qt.IsNil)
			c.Assert(renderedStatements(c, nodes, test.caps, test.dialect), qt.HasLen, 0)
		})
	}
}

// planRowDeletionPolicy renders a diff for the one dialect that has the clause.
func planRowDeletionPolicy(c *qt.C, diff *types.SchemaDiff) []string {
	c.Helper()

	planner := postgres.NewForDialect(platform.Spanner, capability.SpannerPostgres())
	nodes, err := planner.GenerateMigrationASTChecked(diff, nil)
	c.Assert(err, qt.IsNil)

	return renderedStatements(c, nodes, capability.SpannerPostgres(), platform.Spanner)
}

// indexOfStatement is where a statement containing fragment appears, and fails
// the test when nothing does — an ordering assertion between two statements
// that are not both there would otherwise pass on a plan missing one.
func indexOfStatement(c *qt.C, statements []string, fragment string) int {
	c.Helper()
	for i, statement := range statements {
		if strings.Contains(statement, fragment) {
			return i
		}
	}
	c.Fatalf("no statement contains %q, in: %v", fragment, statements)
	return -1
}
