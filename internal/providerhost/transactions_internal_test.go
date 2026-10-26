package providerhost

// White-box testing required: frozen engine advertisements and the private
// planner are the boundaries under test; transport is covered by process tests.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/planext"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// The discarded transport buffer is private state; a black-box request cannot
// observe whether its backing bytes were cleared after dispatch.
func TestExecutionInputCleanupClearsBackingBytes(t *testing.T) {
	c := qt.New(t)
	value := []byte("private input")
	request := &protocol.ExecuteRequest{InputBindings: []*protocol.ExecutionInputBinding{{Value: value}}}
	clearExecutionInputs(request)
	c.Assert(request.InputBindings, qt.IsNil)
	c.Assert(value, qt.DeepEquals, make([]byte, len(value)))
	clearExecutionInputs(request)
	c.Assert(request.InputBindings, qt.IsNil)
}

func TestTransactionsRequireSelectedEngineAndGlobalService(t *testing.T) {
	for _, row := range []struct {
		name           string
		global, engine []string
		want           bool
	}{
		{"absent", nil, nil, false},
		{"other engine only", []string{provider.TransactionsFeature}, nil, false},
		{"missing service", nil, []string{provider.TransactionsFeature}, false},
		{"negotiated", []string{provider.TransactionsFeature}, []string{provider.TransactionsFeature}, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			target := &target{host: &Host{description: &protocol.Description{Features: row.global}}, engine: &protocol.Engine{Features: row.engine}}
			c.Assert(target.supportsTransactions(), qt.Equals, row.want)
		})
	}
}

type transactionOperation struct{}

func (*transactionOperation) Kind() schemaext.Kind                 { return "example.org/transaction/operation" }
func (*transactionOperation) CloneExtension() ast.ExtensionPayload { return &transactionOperation{} }

type transactionPlanRuntime struct{ *engine.Runtime }

func (*transactionPlanRuntime) PlanFeatures(context.Context, featureplan.Request) (featureplan.Result, error) {
	return featureplan.Result{Complete: true, Contributions: []plangraph.Contribution[featureplan.Operation]{{
		Owner: "example.org/transaction", Steps: []plangraph.Step[featureplan.Operation]{{
			ID:          plangraph.StepID{Owner: "example.org/transaction", Name: "operation"},
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: &transactionOperation{}},
			Transaction: plangraph.TransactionRequired,
		}},
	}}}, nil
}
func TestRequiredTransactionPlanningNeedsNegotiationAndAtomicDDL(t *testing.T) {
	for _, row := range []struct {
		name       string
		negotiated bool
		caps       capability.Capabilities
		want       error
		nodes      int
	}{
		{"no service", false, capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: true}), ptaherr.ErrUnsupportedFeature, 0},
		{"unknown atomicity", true, capability.Capabilities{}, ptaherr.ErrUnsupportedFeature, 0},
		{"nontransactional DDL", true, capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false}), ptaherr.ErrUnsupportedFeature, 0},
		{"supported", true, capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: true}), nil, 1},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			base, err := engine.New()
			c.Assert(err, qt.IsNil)
			semantics := identifier.ForDialect("sqlite")
			runtime := &transactionPlanRuntime{Runtime: base}
			planner := commonPlanner{target: "fixture", transactions: row.negotiated, options: planext.Options{Capabilities: row.caps}}
			nodes, err := planner.GenerateMigrationAST(c.Context(), runtime, &difftypes.SchemaDiff{IdentifierSemantics: &semantics})
			c.Assert(err, qt.ErrorIs, row.want)
			c.Assert(nodes, qt.HasLen, row.nodes)
		})
	}
}
