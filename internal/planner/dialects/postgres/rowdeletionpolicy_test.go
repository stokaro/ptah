package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerdiff"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// rowDeletionChanges is the owner change the comparator attaches to a table
// whose Spanner row deletion policy differs. A nil side is a known absence.
func rowDeletionChanges(table string, desired, current *spannerschema.Policy) []schemaext.ChangeRecord {
	change := &spannerdiff.RowDeletion{}
	if desired != nil {
		change.After = &spannerschema.DesiredRowDeletion{Policy: *desired}
	}
	if current != nil {
		change.Before = &spannerschema.ObservedRowDeletion{Policy: *current}
	}
	subject := objectidentity.NewBuilder(identifier.ForDialect(platform.Spanner)).Table(table)
	return []schemaext.ChangeRecord{{Subject: subject, Value: change}}
}

// policyDiff is a diff whose only content is one table's row deletion policy
// transition.
func policyDiff(desired, current *spannerschema.Policy) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "sessions",
			FeatureChanges: rowDeletionChanges("sessions", desired, current),
		}},
	}
}

// TestPlanner_RowDeletionPolicyTransitions pins the statement each transition
// produces.
//
// Every expectation was executed against the Cloud Spanner emulator behind
// PGAdapter and read back from
// information_schema.tables.row_deletion_policy_expression, which is what makes
// these strings a claim about convergence rather than about formatting
// (stokaro/ptah#2236).
func TestPlanner_RowDeletionPolicyTransitions(t *testing.T) {
	tests := []struct {
		name    string
		desired *spannerschema.Policy
		current *spannerschema.Policy
		want    []string
	}{
		{
			name:    "adding a policy",
			desired: &spannerschema.Policy{Column: "created_at", Interval: "30 days"},
			want:    []string{`ALTER TABLE "sessions" ADD TTL INTERVAL '30 days' ON "created_at";`},
		},
		{
			// ADD and ALTER are not interchangeable: the server refuses each in
			// the other's position, which is why the change carries both sides.
			name:    "changing the interval",
			desired: &spannerschema.Policy{Column: "created_at", Interval: "60 days"},
			current: &spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"},
			want:    []string{`ALTER TABLE "sessions" ALTER TTL INTERVAL '60 days' ON "created_at";`},
		},
		{
			name:    "moving the policy to another column",
			desired: &spannerschema.Policy{Column: "updated_at", Interval: "30 days"},
			current: &spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"},
			want:    []string{`ALTER TABLE "sessions" ALTER TTL INTERVAL '30 days' ON "updated_at";`},
		},
		{
			// The removal names no column: the clause goes and the timestamp
			// column it referred to stays.
			name:    "dropping the policy",
			current: &spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"},
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

// TestPlanner_ThePolicyIsRetargetedBeforeItsColumnIsDropped pins the ORDER,
// which is the half a per-transition test cannot see.
//
// A migration that moves a policy off a column and drops that column is one
// plan, and the two statements only work in one order: the column the policy
// still names cannot be dropped while it names it.
func TestPlanner_ThePolicyIsRetargetedBeforeItsColumnIsDropped(t *testing.T) {
	c := qt.New(t)

	statements := planRowDeletionPolicy(c, &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "sessions",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "created_at"}},
			FeatureChanges: rowDeletionChanges("sessions",
				&spannerschema.Policy{Column: "updated_at", Interval: "30 days"},
				&spannerschema.Policy{Column: "created_at", Interval: "30 days"}),
		}},
	})

	c.Assert(statementIndex(c, statements, "ALTER TTL") <
		statementIndex(c, statements, "DROP COLUMN"), qt.IsTrue,
		qt.Commentf("statements were: %v", statements))
}

// TestPlanner_TheDroppedPolicyGoesBeforeItsColumn is the same ordering for the
// other transition, because the removal names no column and could look
// order-independent.
func TestPlanner_TheDroppedPolicyGoesBeforeItsColumn(t *testing.T) {
	c := qt.New(t)

	statements := planRowDeletionPolicy(c, &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "sessions",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "created_at"}},
			FeatureChanges: rowDeletionChanges("sessions", nil, &spannerschema.Policy{Column: "created_at", Interval: "30 days"}),
		}},
	})

	c.Assert(statementIndex(c, statements, "DROP TTL") <
		statementIndex(c, statements, "DROP COLUMN"), qt.IsTrue,
		qt.Commentf("statements were: %v", statements))
}

// TestPlanner_RowDeletionPolicyIsRefusedWithoutAnOwner pins the gate: a diff
// carrying a Spanner row deletion policy change on another target means the
// comparison saw a policy no owner on that target plans. The plan refuses it
// rather than emitting nothing, so the explanation arrives before any
// statement does.
func TestPlanner_RowDeletionPolicyIsRefusedWithoutAnOwner(t *testing.T) {
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

			nodes, err := planner.GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				policyDiff(&spannerschema.Policy{Column: "created_at", Interval: "30 days"}, nil),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// planRowDeletionPolicy renders a diff for the one dialect that has the clause.
func planRowDeletionPolicy(c *qt.C, diff *difftypes.SchemaDiff) []string {
	c.Helper()

	planner := postgres.NewForDialect(platform.Spanner, capability.SpannerPostgres())
	nodes, err := planner.GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		withDeclaredObjects(diff, nil),
	)
	c.Assert(err, qt.IsNil)

	return renderedStatements(c, nodes, capability.SpannerPostgres(), platform.Spanner)
}
