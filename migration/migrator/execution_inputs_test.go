package migrator_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/inputapproval"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

type inputSession struct {
	*metadataSession
	transform func([]renderer.Fragment) []renderer.Fragment
	approved  bool
}

func (s *inputSession) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return s, nil
}
func (s *inputSession) OpenSession(context.Context) (dbschema.NativeSession, error) { return s, nil }
func (s *inputSession) Writer() catalog.SchemaWriter                                { return s }
func (s *inputSession) PrepareUnitInputs(ctx context.Context, units []renderer.Fragment) ([]renderer.Fragment, error) {
	result := slices.Clone(units)
	for i := range result {
		if strings.Contains(result[i].SQL, "needs_input") {
			result[i].Inputs = []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "DECLARED_INPUT"}}
		}
	}
	return s.transform(result), ctx.Err()
}
func (s *inputSession) ExecuteSQL(ctx context.Context, sql string, _ ...any) error {
	if !s.approved {
		return fmt.Errorf("execution preceded approval")
	}
	if strings.Contains(sql, "needs_input") && !slices.Equal(inputapproval.References(ctx, "sqlite", sql), []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "DECLARED_INPUT"}}) {
		return fmt.Errorf("execution lost its approved input references")
	}
	s.statements++
	return nil
}

func inputMigrator(c *qt.C, transform func([]renderer.Fragment) []renderer.Fragment) (*inputSession, *migrator.Migrator) {
	c.Helper()
	owner := &inputSession{metadataSession: &metadataSession{}, transform: transform}
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{"inputs": {Target: "sqlite", Connector: owner}})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "inputs://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	migration := migrator.CreateMigrationFromSQL(1, "inputs", "CREATE TABLE prefix (id INTEGER); CREATE TABLE needs_input (id INTEGER);", "DROP TABLE needs_input; DROP TABLE prefix;")
	migration.DownTxMode = migrationfile.FileTxModeNone
	runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).WithMigrationsTable("main", "_revisions").
		WithTransactionMode(migrator.MigrationTxModeNone).WithoutMigrationLock()
	return owner, runner
}

func TestMigrationInputsAreApprovedBeforeExecutionAndRetainedForRollback(t *testing.T) {
	c := qt.New(t)
	owner, runner := inputMigrator(c, func(units []renderer.Fragment) []renderer.Fragment { return units })
	var observed []migrator.MigrationInput
	c.Assert(runner.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{
		PlanObserver: func(_ context.Context, plan migrator.MigrationPlan) {
			c.Assert(plan.Inputs, qt.HasLen, 1)
			c.Assert(plan.Inputs[0].StatementIndex, qt.Equals, 2)
			plan.Inputs[0].Inputs[0].Name = "MUTATED_OBSERVER_COPY"
		},
		Preflight: func(_ context.Context, plan migrator.MigrationPlan) error {
			c.Assert(owner.statements, qt.Equals, 0)
			c.Assert(plan.Inputs[0].Inputs[0].Name, qt.Equals, "DECLARED_INPUT")
			c.Assert(plan.Inputs[0].StatementDigest, qt.Matches, "sha256:[0-9a-f]{64}")
			observed = plan.Inputs
			plan.Inputs[0].Inputs[0].Name = "MUTATED_HOOK_COPY"
			owner.approved = true
			return nil
		},
	}), qt.IsNil)
	c.Assert(owner.statements, qt.Equals, 2)
	owner.approved = false
	c.Assert(runner.MigrateDownToWithPreflight(c.Context(), 0, func(_ context.Context, plan migrator.MigrationPlan) error {
		c.Assert(plan.Inputs, qt.HasLen, 1)
		c.Assert(plan.Inputs[0].StatementIndex, qt.Equals, 1)
		c.Assert(plan.Inputs[0].StatementDigest, qt.Not(qt.Equals), observed[0].StatementDigest)
		owner.approved = true
		return nil
	}), qt.IsNil)
	c.Assert(owner.statements, qt.Equals, 4)
}

func TestMigrationInputPreparationRefusesChangedBodiesBeforePrefix(t *testing.T) {
	for _, test := range []struct {
		name      string
		transform func([]renderer.Fragment) []renderer.Fragment
	}{
		{"missing unit", func(units []renderer.Fragment) []renderer.Fragment { return units[:1] }},
		{"changed body", func(units []renderer.Fragment) []renderer.Fragment { units[1].SQL = "DROP TABLE prefix;"; return units }},
		{"invalid reference", func(units []renderer.Fragment) []renderer.Fragment { units[1].Inputs[0].Source = "file"; return units }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner, runner := inputMigrator(c, test.transform)
			c.Assert(runner.MigrateUp(c.Context()), qt.ErrorIs, renderer.ErrInvalidResult)
			c.Assert(owner.statements, qt.Equals, 0)
			c.Assert(owner.history, qt.HasLen, 0)
		})
	}
}
