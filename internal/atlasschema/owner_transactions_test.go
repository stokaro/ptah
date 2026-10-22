package atlasschema_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/planext"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
)

type transactionOperation struct{}

func (transactionOperation) Kind() schemaext.Kind                 { return "example.org/transaction-test" }
func (transactionOperation) CloneExtension() ast.ExtensionPayload { return transactionOperation{} }

type transactionRuntime struct {
	*engine.Runtime
	requirement plangraph.Transaction
}

func (r transactionRuntime) MigrationPlanner(string, planext.Options) (planext.Planner, error) {
	return r, nil
}
func (r transactionRuntime) GenerateMigrationAST(context.Context, featureplan.Runtime, *difftypes.SchemaDiff) ([]ast.Node, error) {
	return []ast.Node{
		&ast.CreateTableNode{Name: "prefix", Columns: []*ast.ColumnNode{{Name: "id", Type: "INTEGER"}}},
		&ast.ExtensionStatement{Payload: transactionOperation{}, Transaction: r.requirement},
	}, nil
}
func (r transactionRuntime) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	result := renderer.Result{Complete: true}
	for _, node := range request.Nodes {
		if _, owned := node.(*ast.ExtensionStatement); owned {
			result.Fragments = append(result.Fragments, renderer.Fragment{SQL: `CREATE TABLE planned (id INTEGER);`})
			continue
		}
		one := request
		one.Nodes = []ast.Node{node}
		rendered, err := r.Runtime.Render(ctx, one)
		if err != nil {
			return renderer.Result{}, err
		}
		result.Fragments = append(result.Fragments, rendered.Fragments...)
	}
	return result, nil
}

func prepareTransactionPlan(c *qt.C, requirement plangraph.Transaction, mode migrator.MigrationTxMode) (atlasschema.ApplyRuntimePlan, *dbschema.DatabaseConnection) {
	c.Helper()
	conn := connectSQLite(c, filepath.Join(c.TempDir(), "transactions.db"))
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	runtime := transactionRuntime{Runtime: must.Must(builtin.New()), requirement: requirement}
	plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{
		Runtime: runtime, TxMode: mode, Desired: &schemamodel.Database{Tables: []schemamodel.Table{{Name: "planned"}}},
	})
	c.Assert(err, qt.IsNil)
	return plan, conn
}

func TestPreparedPlanRefusesOwnerTransactionMismatchBeforeAnyWrite(t *testing.T) {
	for _, test := range []struct {
		requirement plangraph.Transaction
		mode        migrator.MigrationTxMode
	}{
		{plangraph.TransactionRequired, migrator.MigrationTxModeNone},
		{plangraph.TransactionForbidden, migrator.MigrationTxModeFile},
		{plangraph.TransactionForbidden, migrator.MigrationTxModeAll},
	} {
		t.Run(string(test.requirement)+"/"+string(test.mode), func(t *testing.T) {
			c := qt.New(t)
			plan, conn := prepareTransactionPlan(c, test.requirement, test.mode)
			preflight := plan.PreflightExecution(plan.SQL())
			c.Assert(preflight, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			report := atlasschema.NewApplyReport(atlasschema.ApplyEvidence{Err: preflight})
			c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
			c.Assert(report.Refusal.Code, qt.Equals, atlasschema.RefusalTransactionPreflight)
			c.Assert(plan.Execute(c.Context()), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			observed, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			c.Assert(observed.Tables, qt.HasLen, 0)
		})
	}
}

func TestPreparedPlanExecutesCompatibleOwnerRequirements(t *testing.T) {
	for _, test := range []struct {
		requirement plangraph.Transaction
		mode        migrator.MigrationTxMode
	}{
		{plangraph.TransactionRequired, migrator.MigrationTxModeFile},
		{plangraph.TransactionForbidden, migrator.MigrationTxModeNone},
	} {
		t.Run(string(test.requirement), func(t *testing.T) {
			c := qt.New(t)
			plan, conn := prepareTransactionPlan(c, test.requirement, test.mode)
			c.Assert(plan.Execute(c.Context()), qt.IsNil)
			observed, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			c.Assert(observed.Tables, qt.HasLen, 2)
		})
	}
}

func TestEditedOwnerPlanNeedsNewTransactionAssessment(t *testing.T) {
	c := qt.New(t)
	plan, conn := prepareTransactionPlan(c, plangraph.TransactionForbidden, migrator.MigrationTxModeNone)
	edited := plan.SQL() + "CREATE TABLE extra (id INTEGER);"
	c.Assert(plan.PreflightExecution(edited), qt.ErrorMatches, ".*new owner assessment.*")
	c.Assert(plan.ExecuteSQL(c.Context(), edited), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	observed, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables, qt.HasLen, 0)
}

func TestSavedPlanRetainsOwnerRequirementsThroughJSON(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLite(c, filepath.Join(c.TempDir(), "saved.db"))
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	runtime := transactionRuntime{Runtime: must.Must(builtin.New()), requirement: plangraph.TransactionRequired}
	plan, err := atlasschema.PreparePlanFile(c.Context(), conn, atlasschema.PlanFileOptions{
		Runtime: runtime, Desired: &schemamodel.Database{Tables: []schemamodel.Table{{Name: "planned"}}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 2)
	c.Assert(plan.Statements[1].Transactions, qt.DeepEquals, []plangraph.Transaction{plangraph.TransactionRequired})
	encoded, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	decoded, _, err := atlasschema.DecodePlanFile(encoded, "transaction.plan.json")
	c.Assert(err, qt.IsNil)
	c.Assert(decoded.Statements[1].Transactions, qt.DeepEquals, plan.Statements[1].Transactions)
	invalid := bytes.ReplaceAll(encoded, []byte(`"required"`), []byte(`"invalid"`))
	_, _, err = atlasschema.DecodePlanFile(invalid, "invalid.plan.json")
	c.Assert(err, qt.ErrorMatches, ".*unknown owner transaction requirement.*")
	c.Assert(decoded.Execute(c.Context(), conn, migrator.MigrationTxModeNone), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	observed, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables, qt.HasLen, 0)
	_, err = atlasschema.MarshalPlanFileHCL(decoded)
	c.Assert(err, qt.ErrorMatches, ".*cannot preserve owner transaction requirements.*")
	unchanged, err := decoded.WithStatementsFromSQL(c.Context(), runtime, decoded.SQL())
	c.Assert(err, qt.IsNil)
	c.Assert(unchanged.Statements[1].Transactions, qt.DeepEquals, decoded.Statements[1].Transactions)
	_, err = decoded.WithStatementsFromSQL(c.Context(), runtime, decoded.SQL()+"CREATE TABLE extra (id INTEGER);")
	c.Assert(err, qt.ErrorMatches, ".*new owner assessment.*")
	c.Assert(decoded.Execute(c.Context(), conn, migrator.MigrationTxModeFile), qt.IsNil)
	observed, err = conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables, qt.HasLen, 2)
}

func TestRehearsalRefusesOwnerRequirementsBeforeOpeningDev(t *testing.T) {
	c := qt.New(t)
	plan, conn := prepareTransactionPlan(c, plangraph.TransactionRequired, migrator.MigrationTxModeNone)
	c.Assert(plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{DevURL: "invalid://must-not-open"}), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	err := atlasschema.RehearsePlanStatements(c.Context(), conn, plan.Statements(), &schemamodel.Database{}, atlasschema.PlanRehearsalOptions{
		Runtime: must.Must(builtin.New()), DevURL: "invalid://must-not-open", TxMode: migrator.MigrationTxModeNone,
		OwnerTransactions: [][]plangraph.Transaction{nil, {plangraph.TransactionRequired}},
	})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	observed, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables, qt.HasLen, 0)
}
