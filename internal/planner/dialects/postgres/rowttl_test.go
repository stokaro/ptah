package postgres_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// rowTTLChanges is the owner change the comparator attaches to a table whose
// row-level TTL differs. A nil side is a known absence.
func rowTTLChanges(table string, desired, current *crdbschema.Policy) []schemaext.ChangeRecord {
	change := &crdbdiff.RowTTL{}
	if desired != nil {
		change.After = &crdbschema.DesiredRowTTL{Policy: *desired}
	}
	if current != nil {
		change.Before = &crdbschema.ObservedRowTTL{Policy: *current}
	}
	subject := objectidentity.NewBuilder(identifier.ForDialect(platform.CockroachDB)).Table(table)
	return []schemaext.ChangeRecord{{Subject: subject, Value: change}}
}

// ttlDiff is a diff whose only content is one table's TTL transition, which is
// what the comparator produces for a table that differs in nothing else.
func ttlDiff(desired, current *crdbschema.Policy) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "sessions",
			FeatureChanges: rowTTLChanges("sessions", desired, current),
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
		desired *crdbschema.Policy
		current *crdbschema.Policy
		want    []string
	}{
		{
			name:    "adding a policy",
			desired: &crdbschema.Policy{ExpirationExpression: "expires_at"},
			current: nil,
			want:    []string{`ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at');`},
		},
		{
			name:    "changing the expression",
			desired: &crdbschema.Policy{ExpirationExpression: "expires_at + INTERVAL '1 hour'"},
			current: &crdbschema.Policy{ExpirationExpression: "expires_at"},
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
			desired: &crdbschema.Policy{ExpirationExpression: "expires_at"},
			current: &crdbschema.Policy{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(500))},
			want: []string{
				`ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at');`,
				`ALTER TABLE "sessions" RESET (ttl_select_batch_size);`,
			},
		},
		{
			name:    "removing the whole policy",
			desired: nil,
			current: &crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily"},
			want:    []string{`ALTER TABLE "sessions" RESET (ttl);`},
		},
		{
			// One statement, not one per parameter: RESET takes several names.
			name:    "dropping several knobs at once",
			desired: &crdbschema.Policy{ExpirationExpression: "expires_at"},
			current: &crdbschema.Policy{
				ExpirationExpression: "expires_at",
				JobCron:              "@daily",
				DeleteBatchSize:      new(int64(100)),
			},
			want: []string{
				`ALTER TABLE "sessions" SET (ttl_expiration_expression = 'expires_at');`,
				`ALTER TABLE "sessions" RESET (ttl_job_cron, ttl_delete_batch_size);`,
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

// TestPlanner_RowTTLIsRefusedWithoutAnOwner pins the gate.
//
// A diff carrying a CockroachDB row-level TTL change on another target means
// the comparison saw a policy no owner on that target plans. The plan refuses
// it rather than emitting nothing, so the explanation arrives before any
// statement does.
func TestPlanner_RowTTLIsRefusedWithoutAnOwner(t *testing.T) {
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
			nodes, err := planner.GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				ttlDiff(&crdbschema.Policy{ExpirationExpression: "expires_at"}, nil),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestPlanner_UnchangedTTLPlansNothing keeps the planner quiet where the
// comparator found nothing, which is the state every converged schema is in.
func TestPlanner_UnchangedTTLPlansNothing(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{TableName: "sessions", ColumnsAdded: difftypes.ColumnChanges{{Name: "note"}}}},
	}

	planner := postgres.NewForDialect(platform.CockroachDB, capability.CockroachDB26())
	nodes, err := planner.GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		withDeclaredObjects(diff, nil),
	)

	c.Assert(err, qt.IsNil)
	for _, statement := range renderedStatements(c, nodes, capability.CockroachDB26(), platform.CockroachDB) {
		c.Assert(statement, qt.Not(qt.Contains), "ttl")
	}
}

// planRowTTL plans a TTL diff on CockroachDB and returns the TTL statements it
// produced, dropping the comment nodes the planner writes around them.
func planRowTTL(c *qt.C, diff *difftypes.SchemaDiff) []string {
	c.Helper()

	planner := postgres.NewForDialect(platform.CockroachDB, capability.CockroachDB26())
	nodes, err := planner.GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		withDeclaredObjects(diff, nil),
	)
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
		sql, err := builtin.RenderSQLWithCapabilities(dialect, caps, node)
		c.Assert(err, qt.IsNil)
		for line := range strings.SplitSeq(sql, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "ALTER TABLE") {
				statements = append(statements, line)
			}
		}
	}
	return statements
}

// TestPlanner_TheTTLIsSetBeforeItsColumnIsDropped pins the ORDER, which the
// per-transition table above cannot see.
//
// A plan that moves a TTL expression off a column and drops that column is one
// plan, and the two statements only work in one order: CockroachDB will not
// drop a column the policy still refers to. The step's own comment has always
// said "before anything is dropped" — it sat below removeTableColumns, so that
// was true of tables and not of columns (stokaro/ptah#2236 found it while
// placing the row deletion policy, which carries the same constraint).
func TestPlanner_TheTTLIsSetBeforeItsColumnIsDropped(t *testing.T) {
	c := qt.New(t)

	statements := planRowTTL(c, &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "sessions",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "expires_at"}},
			FeatureChanges: rowTTLChanges("sessions",
				&crdbschema.Policy{ExpirationExpression: "deleted_at"},
				&crdbschema.Policy{ExpirationExpression: "expires_at"}),
		}},
	})

	c.Assert(statementIndex(c, statements, "ttl_expiration_expression") <
		statementIndex(c, statements, "DROP COLUMN"), qt.IsTrue,
		qt.Commentf("statements were: %v", statements))
}

// TestPlanner_TheTTLIsResetBeforeItsColumnIsDropped is the same ordering for
// the removal, which names no column and could look order-independent.
func TestPlanner_TheTTLIsResetBeforeItsColumnIsDropped(t *testing.T) {
	c := qt.New(t)

	statements := planRowTTL(c, &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "sessions",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "expires_at"}},
			FeatureChanges: rowTTLChanges("sessions", nil, &crdbschema.Policy{ExpirationExpression: "expires_at"}),
		}},
	})

	c.Assert(statementIndex(c, statements, "RESET") <
		statementIndex(c, statements, "DROP COLUMN"), qt.IsTrue,
		qt.Commentf("statements were: %v", statements))
}

// statementIndex is where a statement containing fragment appears, failing the
// test when none does — an ordering assertion between two statements that are
// not both present would otherwise pass on a plan missing one.
func statementIndex(c *qt.C, statements []string, fragment string) int {
	c.Helper()
	for i, statement := range statements {
		if strings.Contains(statement, fragment) {
			return i
		}
	}
	c.Fatalf("no statement contains %q, in: %v", fragment, statements)
	return -1
}

// ownerlessRuntime knows the CockroachDB target and the row-level TTL models,
// and registers no planning service, as an embedder's runtime can.
func ownerlessRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: crdbschema.Owner, Targets: []engine.Target{{Name: platform.CockroachDB}}, Codecs: crdbschema.Codecs(),
	}))
}

// droppedTable is a removal of events whose capture carries the given facets.
func droppedTable(facets schemaext.Facets) *difftypes.SchemaDiff {
	removal := difftypes.TableRemoval{Name: "events"}
	removal.Current.Table = catalog.Table{Name: "events", Facets: facets}
	return &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{removal}}
}

// TestPlanner_DroppingATableWithoutOwnedStateAsksNoOwner pins that a table
// drop depends on a feature owner only when the table carries owned state: a
// runtime with no CockroachDB services, as an embedder may build, still drops
// a plain table.
func TestPlanner_DroppingATableWithoutOwnedStateAsksNoOwner(t *testing.T) {
	c := qt.New(t)

	planner := postgres.NewForDialect(platform.CockroachDB, capability.CockroachDB26())
	nodes, err := planner.GenerateMigrationAST(t.Context(), ownerlessRuntime(), droppedTable(schemaext.Facets{}))

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(renderedSQL(c, nodes), "\n"), qt.Contains, `DROP TABLE IF EXISTS "events"`)
}

// TestPlanner_DroppingATableWithOwnedStateNeedsItsOwner is the control on the
// test above: a table that carries owned state anywhere the runtime looks --
// a policy, a facet on a column, or only a coverage record -- is accounted for
// by its owner, and a runtime without one refuses the drop rather than losing
// the receipt.
func TestPlanner_DroppingATableWithOwnedStateNeedsItsOwner(t *testing.T) {
	policy := must.Must(schemaext.NewFacets(&crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at"}}))
	subject := objectidentity.NewBuilder(identifier.ForDialect(platform.CockroachDB)).Table("events")
	coverage := must.Must(crdbschema.RowTTLCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were read"},
		[]schemaext.SubjectCoverage{{Kind: crdbschema.RowTTLKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}))
	tests := []struct {
		name    string
		current schemacapture.TableObservation
	}{
		{name: "a policy", current: schemacapture.TableObservation{Table: catalog.Table{Name: "events", Facets: policy}}},
		{name: "a column facet", current: schemacapture.TableObservation{Table: catalog.Table{Name: "events",
			Columns: []catalog.Column{{Name: "id", Facets: policy}}}}},
		{name: "a coverage record", current: schemacapture.TableObservation{Table: catalog.Table{Name: "events"}, FeatureCoverage: coverage}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "events", Current: test.current}}}
			planner := postgres.NewForDialect(platform.CockroachDB, capability.CockroachDB26())
			nodes, err := planner.GenerateMigrationAST(t.Context(), ownerlessRuntime(), diff)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// renderedSQL renders every planned node.
func renderedSQL(c *qt.C, nodes []ast.Node) []string {
	c.Helper()
	var statements []string
	for _, node := range nodes {
		sql, err := builtin.RenderSQLWithCapabilities(platform.CockroachDB, capability.CockroachDB26(), node)
		c.Assert(err, qt.IsNil)
		statements = append(statements, sql)
	}
	return statements
}
