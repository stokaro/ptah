package spannerplan_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerast"
	"ptah.run/dialect/spanner/spannerdiff"
	"ptah.run/dialect/spanner/spannerplan"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/engine"
)

// planningRuntime selects only this owner, so the engine's reply validation
// runs without the bundled runtime.
func planningRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: spannerschema.Owner, Targets: []engine.Target{{Name: "spanner"}},
		Codecs: slices.Concat(spannerschema.Codecs(), spannerdiff.Codecs(), spannerast.Codecs()),
		Planning: []engine.Planning{{
			Target: "spanner", Kinds: []schemaext.Kind{spannerdiff.RowDeletionKind}, ParentKinds: []schemaext.Kind{spannerschema.RowDeletionKind},
			OperationKinds: []schemaext.Kind{spannerast.AlterRowDeletionKind}, Service: spannerplan.Service{},
		}},
	}))
}

func requestFixture() featureplan.Request {
	semantics := identifier.ForDialect("spanner")
	builder := objectidentity.NewBuilder(semantics)
	sessions, events := builder.Table("audit.sessions"), builder.Table("events")
	change := &spannerdiff.RowDeletion{
		Before: &spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"}},
		After:  &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "60 days"}},
	}
	dropped := featureplan.Table{Subject: events, Action: featureplan.DropTable}
	dropped.Current.Table = catalog.Table{Name: "events"}
	return featureplan.Request{
		Target: "spanner", Identifiers: semantics,
		Tables:  []featureplan.Table{{Subject: sessions}, dropped},
		Changes: []schemaext.ChangeRecord{{Subject: sessions, Value: change}},
	}
}

// TestPlanFeatures_PlansTheChangeAndAccountsForTheRemovedTable pins the
// receipts: one owned operation for the change, carrying copied operands and
// the comment the plan writes above it, and a parent receipt for the policy a
// dropped table takes with it.
func TestPlanFeatures_PlansTheChangeAndAccountsForTheRemovedTable(t *testing.T) {
	c := qt.New(t)

	request := requestFixture()
	result, err := planningRuntime().PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Kind, qt.Equals, spannerdiff.RowDeletionKind)
	c.Assert(result.Parents, qt.DeepEquals, []featureplan.ParentPlan{{
		Subject: request.Tables[1].Subject, Kind: spannerschema.RowDeletionKind, Action: featureplan.DropTable,
		Strategy: "remove the row deletion policy with the table",
	}})
	c.Assert(result.Contributions, qt.HasLen, 1)
	steps := result.Contributions[0].Steps
	c.Assert(steps, qt.HasLen, 1)
	c.Assert(result.Changes[0].Steps, qt.DeepEquals, []plangraph.StepID{steps[0].ID})
	c.Assert(steps[0].Impact.Impact, qt.Equals, schemaext.Behavioral)
	c.Assert(steps[0].Payload.Role, qt.Equals, ast.AlterExtension)
	c.Assert(steps[0].Payload.Parent, qt.DeepEquals, request.Tables[0].Subject)
	c.Assert(steps[0].Payload.Notes, qt.DeepEquals, []string{"Row deletion policy on table: audit.sessions"})
	op := steps[0].Payload.Payload.(*spannerast.AlterRowDeletion)
	c.Assert(op.Change.Before.Policy.Interval, qt.Equals, "4 WEEKS 2 DAYS")
	op.Change.Before.Policy.Interval = "mutated"
	c.Assert(request.Changes[0].Value.(*spannerdiff.RowDeletion).Before.Policy.Interval, qt.Equals, "4 WEEKS 2 DAYS")
}

// TestPlanFeatures_KeepsTheOrderOfTheChanges pins that independent steps are
// scheduled in the order of the changes, past ten of them: a scheduler orders
// such steps by name, and an unpadded index would put the tenth before the
// second.
func TestPlanFeatures_KeepsTheOrderOfTheChanges(t *testing.T) {
	c := qt.New(t)

	semantics := identifier.ForDialect("spanner")
	request := featureplan.Request{Target: "spanner", Identifiers: semantics}
	var want []objectidentity.ID
	for _, name := range []string{"t00", "t01", "t02", "t03", "t04", "t05", "t06", "t07", "t08", "t09", "t10", "t11"} {
		subject := objectidentity.NewBuilder(semantics).Table(name)
		want = append(want, subject)
		request.Tables = append(request.Tables, featureplan.Table{Subject: subject})
		request.Changes = append(request.Changes, schemaext.ChangeRecord{Subject: subject, Value: &spannerdiff.RowDeletion{
			After: &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "30 days"}},
		}})
	}
	result, err := spannerplan.Service{}.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	plan, err := plangraph.Schedule(t.Context(), result.Contributions...)
	c.Assert(err, qt.IsNil)

	var got []objectidentity.ID
	for _, step := range plan.Steps {
		got = append(got, step.Payload.Parent)
	}
	c.Assert(got, qt.DeepEquals, want)
}

// TestPlanFeatures_RefusesWithoutOutput pins the completed refusals: no
// receipt, contribution or prefix survives one.
func TestPlanFeatures_RefusesWithoutOutput(t *testing.T) {
	tests := []struct {
		name   string
		change func(*featureplan.Request)
		want   string
	}{
		{"a change without its table", func(r *featureplan.Request) { r.Tables = r.Tables[1:] }, "requires captured parent state"},
		{"a change on a removed table", func(r *featureplan.Request) { r.Tables[0].Action = featureplan.DropTable }, "survives the plan in place"},
		{"a change that changes nothing", func(r *featureplan.Request) {
			r.Changes[0].Value = &spannerdiff.RowDeletion{
				Before: &spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: "ts", Interval: "4 WEEKS 2 DAYS"}},
				After:  &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "ts", Interval: "30 days"}},
			}
		}, "operands contain no change"},
		{"a rebuilt table", func(r *featureplan.Request) { r.Tables[1].Action = featureplan.RebuildTable }, `no plan for parent action "rebuild-table"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := requestFixture()
			request.ParentKinds = []schemaext.Kind{spannerschema.RowDeletionKind}
			test.change(&request)
			result, err := spannerplan.Service{}.PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.ValidateOutcome(request), qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, test.want)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Parents, qt.HasLen, 0)
		})
	}
}

func TestPlanFeatures_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	tests := []struct {
		name    string
		ctx     context.Context
		target  string
		wantErr string
	}{
		{name: "canceled", ctx: canceled, target: "spanner", wantErr: "context canceled"},
		{name: "another target", ctx: t.Context(), target: "postgres", wantErr: `.*Spanner planning on "postgres".*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := requestFixture()
			request.Target = test.target
			result, err := spannerplan.Service{}.PlanFeatures(test.ctx, request)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
		})
	}
}

// cancelAfter is a context canceled once its Err has answered nil allowed
// times, so a test can cancel a plan at a chosen check rather than before the
// first one.
type cancelAfter struct {
	context.Context
	allowed int
}

func (c *cancelAfter) Err() error {
	if c.allowed == 0 {
		return context.Canceled
	}
	c.allowed--
	return nil
}

// TestPlanFeatures_CanceledAfterTheChangesReturnsNoPrefix covers the last
// check: a request without parent kinds canceled after its changes were
// planned returns the error and no partial receipt.
func TestPlanFeatures_CanceledAfterTheChangesReturnsNoPrefix(t *testing.T) {
	c := qt.New(t)
	request := requestFixture()
	ctx := &cancelAfter{Context: t.Context(), allowed: 1 + len(request.Changes)}

	result, err := spannerplan.Service{}.PlanFeatures(ctx, request)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}
