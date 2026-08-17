package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func ttlIntp(value int64) *int64 { return &value }

// ttlDiff is a diff whose only content is one table's TTL transition, which is
// what the comparator produces for a table that differs in nothing else.
func ttlDiff(desired, current *ast.RowTTLSpec) *types.SchemaDiff {
	return &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName:    "sessions",
			RowTTLChange: &types.RowTTLChange{Desired: desired, Current: current},
		}},
	}
}

// TestPlanner_RowTTLTransitions pins the statements each transition produces,
// in order.
//
// Every expectation below was applied to live CockroachDB v25.4.14 and v26.2.5
// through the real CLI, and the second pass over the same declaration then
// reported "Schema is synced, no changes to be made." That is what makes these
// strings a claim about convergence rather than about formatting.
func TestPlanner_RowTTLTransitions(t *testing.T) {
	tests := []struct {
		name    string
		desired *ast.RowTTLSpec
		current *ast.RowTTLSpec
		want    []string
	}{
		{
			name:    "adding a policy",
			desired: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			current: nil,
			want:    []string{`ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at');`},
		},
		{
			name:    "changing the expression",
			desired: &ast.RowTTLSpec{ExpirationExpression: "expires_at + INTERVAL '1 hour'"},
			current: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			want: []string{
				`ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at + INTERVAL ''1 hour''');`,
			},
		},
		{
			// The RESET is what `SET` alone would not do: measured, SET
			// replaces only the parameters it names and leaves the rest, so a
			// declaration that stopped naming the batch size would keep it
			// forever without this statement.
			name:    "dropping a knob while the policy stays",
			desired: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			current: &ast.RowTTLSpec{ExpirationExpression: "expires_at", SelectBatchSize: ttlIntp(500)},
			want: []string{
				`ALTER TABLE "sessions" RESET (ttl_select_batch_size);`,
				`ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at');`,
			},
		},
		{
			name:    "removing the whole policy",
			desired: nil,
			current: &ast.RowTTLSpec{ExpirationExpression: "expires_at", JobCron: "@daily"},
			want:    []string{`ALTER TABLE "sessions" RESET (ttl);`},
		},
		{
			// One statement, not one per parameter: RESET takes several names.
			name:    "dropping several knobs at once",
			desired: &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
			current: &ast.RowTTLSpec{
				ExpirationExpression: "expires_at",
				JobCron:              "@daily",
				DeleteBatchSize:      ttlIntp(100),
			},
			want: []string{
				`ALTER TABLE "sessions" RESET (ttl_job_cron, ttl_delete_batch_size);`,
				`ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at');`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements := planRowTTL(c, ttlDiff(test.desired, test.current))

			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// TestPlanner_RowTTLIsNotPlannedWithoutTheCapability pins the gate.
//
// A diff carrying a TTL change on a target without the capability means the
// comparison saw a declared policy the renderer will refuse. Nothing is emitted
// here so the refusal arrives from the renderer with its measured explanation,
// rather than as an ALTER the server rejects halfway through a migration.
func TestPlanner_RowTTLIsNotPlannedWithoutTheCapability(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{platform.Postgres, capability.Postgres17()},
		{platform.YugabyteDB, capability.YugabyteDB25()},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			planner := postgres.NewForDialect(test.dialect, test.caps)
			nodes, err := planner.GenerateMigrationASTChecked(
				ttlDiff(&ast.RowTTLSpec{ExpirationExpression: "expires_at"}, nil), nil)

			c.Assert(err, qt.IsNil)
			c.Assert(renderedStatements(c, nodes, test.caps, test.dialect), qt.HasLen, 0)
		})
	}
}

// TestPlanner_UnchangedTTLPlansNothing keeps the planner quiet where the
// comparator found nothing, which is the state every converged schema is in.
func TestPlanner_UnchangedTTLPlansNothing(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{TableName: "sessions", ColumnsAdded: []string{"note"}}},
	}

	planner := postgres.NewForDialect(platform.CockroachDB, capability.CockroachDB26())
	nodes, err := planner.GenerateMigrationASTChecked(diff, nil)

	c.Assert(err, qt.IsNil)
	for _, statement := range renderedStatements(c, nodes, capability.CockroachDB26(), platform.CockroachDB) {
		c.Assert(statement, qt.Not(qt.Contains), "ttl")
	}
}

// planRowTTL plans a TTL diff on CockroachDB and returns the TTL statements it
// produced, dropping the comment nodes the planner writes around them.
func planRowTTL(c *qt.C, diff *types.SchemaDiff) []string {
	c.Helper()

	planner := postgres.NewForDialect(platform.CockroachDB, capability.CockroachDB26())
	nodes, err := planner.GenerateMigrationASTChecked(diff, nil)
	c.Assert(err, qt.IsNil)

	return renderedStatements(c, nodes, capability.CockroachDB26(), platform.CockroachDB)
}

// renderedStatements renders planned nodes into the statements a server would
// run, keeping only the ALTER lines. Rendering rather than inspecting the nodes
// is deliberate: the statement text is what reaches the database, and a node
// the renderer drops is a node that changed nothing.
func renderedStatements(
	c *qt.C, nodes []ast.Node, caps capability.Capabilities, dialect string,
) []string {
	c.Helper()

	var statements []string
	for _, node := range nodes {
		sql, err := renderer.RenderSQLWithCapabilities(dialect, caps, node)
		c.Assert(err, qt.IsNil)
		for _, line := range strings.Split(sql, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "ALTER TABLE") {
				statements = append(statements, line)
			}
		}
	}
	return statements
}
