package ydbplan_test

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
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
)

// ttlPlanningRuntime selects only this owner's TTL service, so the engine's
// reply validation runs without the bundled runtime.
func ttlPlanningRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: ydbschema.Owner, Targets: []engine.Target{{Name: "ydb"}},
		Codecs: slices.Concat(ydbschema.TTLCodecs(), []schemaext.Codec{ydbdiff.TTLCodec(), ydbast.TTLCodec()}),
		Planning: []engine.Planning{{
			Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.TTLKind}, ParentKinds: []schemaext.Kind{ydbschema.TTLKind},
			OperationKinds: []schemaext.Kind{ydbast.AlterTTLKind}, Service: ydbplan.TTLService{},
		}},
	}))
}

// ttlDeclaration is table events with a timestamp, a signed and an unsigned
// integer column.
func ttlDeclaration() schemacapture.TableDeclaration {
	return schemacapture.TableDeclaration{
		Table: schemamodel.Table{StructName: "E", Name: "events"},
		Fields: []schemamodel.Field{
			{StructName: "E", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "E", Name: "ts", Type: "TIMESTAMP", Nullable: true},
			{StructName: "E", Name: "n", Type: "BIGINT", Nullable: true},
			{StructName: "E", Name: "e", Type: "BIGINT UNSIGNED", Nullable: true},
		},
	}
}

func ttlRequestFixture(before *ydbschema.ObservedTTL, after *ydbschema.DesiredTTL) featureplan.Request {
	semantics := identifier.ForDialect("ydb")
	builder := objectidentity.NewBuilder(semantics)
	events, archive := builder.Table("events"), builder.Table("archive")
	dropped := featureplan.Table{Subject: archive, Action: featureplan.DropTable}
	dropped.Current.Table = catalog.Table{Name: "archive"}
	return featureplan.Request{
		Target: "ydb", Identifiers: semantics, Capabilities: capability.YDB262(),
		Tables:  []featureplan.Table{{Subject: events, Desired: ttlDeclaration()}, dropped},
		Changes: []schemaext.ChangeRecord{{Subject: events, Value: &ydbdiff.TTL{Before: before, After: after}}},
	}
}

// TestTTLPlanFeatures_PlansTheChangeAndAccountsForTheRemovedTable pins the
// receipts: one owned operation for the change, carrying copied operands and
// no note, and a parent receipt for the TTL a dropped table takes with it.
func TestTTLPlanFeatures_PlansTheChangeAndAccountsForTheRemovedTable(t *testing.T) {
	c := qt.New(t)

	request := ttlRequestFixture(&ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P30D"}},
		&ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "e", Interval: "PT1H", Unit: "SECONDS"}})
	result, err := ttlPlanningRuntime().PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Kind, qt.Equals, ydbdiff.TTLKind)
	c.Assert(result.Parents, qt.DeepEquals, []featureplan.ParentPlan{{
		Subject: request.Tables[1].Subject, Kind: ydbschema.TTLKind, Action: featureplan.DropTable, Strategy: "remove the TTL with the table",
	}})
	steps := result.Contributions[0].Steps
	c.Assert(steps, qt.HasLen, 1)
	c.Assert(result.Changes[0].Steps, qt.DeepEquals, []plangraph.StepID{steps[0].ID})
	c.Assert(steps[0].Impact.Impact, qt.Equals, schemaext.Behavioral)
	c.Assert(steps[0].Transaction, qt.Equals, plangraph.TransactionForbidden)
	c.Assert(steps[0].Payload.Role, qt.Equals, ast.AlterExtension)
	c.Assert(steps[0].Payload.Notes, qt.HasLen, 0)
	op := steps[0].Payload.Payload.(*ydbast.AlterTTL)
	op.Change.Before.Policy.Interval = "mutated"
	c.Assert(request.Changes[0].Value.(*ydbdiff.TTL).Before.Policy.Interval, qt.Equals, "P30D")
}

// TestTTLPlanFeatures_RefusesWithoutOutput pins the completed refusals: a
// change YDB would refuse or that would lose a setting, each with no receipt,
// contribution or prefix.
func TestTTLPlanFeatures_RefusesWithoutOutput(t *testing.T) {
	declared := func(policy ydbschema.TTL) *ydbschema.DesiredTTL { return &ydbschema.DesiredTTL{Policy: policy} }
	tests := []struct {
		name   string
		before *ydbschema.ObservedTTL
		after  *ydbschema.DesiredTTL
		caps   capability.Capabilities
		want   string
	}{
		{name: "a target without TTLs", after: declared(ydbschema.TTL{Column: "ts", Interval: "P1D"}),
			caps: capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).With(capability.RowDeletionPolicy, false),
			want: `the TTL of table "events", which requires target capability row_deletion_policy`},
		{name: "a removal on a target without TTLs", before: &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P1D"}},
			caps: capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).With(capability.RowDeletionPolicy, false),
			want: `the TTL of table "events", which requires target capability row_deletion_policy`},
		{name: "an integer column's unit without the key", after: declared(ydbschema.TTL{Column: "e", Interval: "P1D", Unit: "SECONDS"}),
			caps: capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false), want: "requires target capability row_deletion_policy_epoch_column"},
		{name: "a signed integer column", after: declared(ydbschema.TTL{Column: "n", Interval: "P1D", Unit: "SECONDS"}),
			caps: capability.YDB262(), want: `column "n" is Int64`},
		{name: "an integer column without its unit", after: declared(ydbschema.TTL{Column: "e", Interval: "P1D"}),
			caps: capability.YDB262(), want: `column "e" is Uint64, an integer type, and YDB needs the unit`},
		{name: "a column the table does not declare", after: declared(ydbschema.TTL{Column: "gone", Interval: "P1D"}),
			caps: capability.YDB262(), want: `it reads column "gone", which the table does not declare`},
		{name: "a run interval SET would reset", before: &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P1D"}, RunIntervalSeconds: 1800},
			after: declared(ydbschema.TTL{Column: "ts", Interval: "P2D"}), caps: capability.YDB262(),
			want: "the table's TTL runs every 1800 seconds, which only the SDK and the CLI set, and SET (TTL = ...) resets it to YDB's default"},
		{name: "a change that changes nothing", before: &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P30D"}},
			after: declared(ydbschema.TTL{Column: "ts", Interval: "PT720H"}), caps: capability.YDB262(), want: "operands contain no change"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := ttlRequestFixture(test.before, test.after)
			request.Capabilities = test.caps
			request.ParentKinds = []schemaext.Kind{ydbschema.TTLKind}
			result, err := ydbplan.TTLService{}.PlanFeatures(t.Context(), request)
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

// TestTTLPlanFeatures_ARunIntervalGoesWithARemoval pins that removing a TTL
// whose run interval only the SDK set is planned: RESET takes the run interval
// with it, which is what the declaration asks for.
func TestTTLPlanFeatures_ARunIntervalGoesWithARemoval(t *testing.T) {
	c := qt.New(t)

	request := ttlRequestFixture(&ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P1D"}, RunIntervalSeconds: 1800}, nil)
	result, err := ydbplan.TTLService{}.PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Contributions, qt.HasLen, 1)
}

// rebuiltEvents is table events rebuilt by the plan with TTL policy declared.
func rebuiltEvents(policy ydbschema.TTL) featureplan.Table {
	declaration := ttlDeclaration()
	declaration.Table.Facets = must.Must(schemaext.NewFacets(&ydbschema.DesiredTTL{Policy: policy}))
	return featureplan.Table{Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).Table("events"), Action: featureplan.RebuildTable, Desired: declaration}
}

// TestTTLPlanFeatures_AccountsForARebuiltTable pins the rebuild: the new CREATE
// TABLE writes the declared TTL, so the plan accounts for it there.
func TestTTLPlanFeatures_AccountsForARebuiltTable(t *testing.T) {
	c := qt.New(t)
	request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		Tables: []featureplan.Table{rebuiltEvents(ydbschema.TTL{Column: "ts", Interval: "P1D"})}, ParentKinds: []schemaext.Kind{ydbschema.TTLKind}}

	result, err := ydbplan.TTLService{}.PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.ValidateOutcome(request), qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Parents, qt.HasLen, 1)
	c.Assert(result.Parents[0].Strategy, qt.Equals, "write the declared TTL into the rebuilt table")
}

// TestTTLPlanFeatures_RefusesARebuiltTableTheTargetCannotHold pins that a
// rebuild whose TTL the target cannot write is refused before any statement
// rather than when the rebuild is rendered.
func TestTTLPlanFeatures_RefusesARebuiltTableTheTargetCannotHold(t *testing.T) {
	tests := []struct {
		name   string
		policy ydbschema.TTL
		caps   capability.Capabilities
		want   string
	}{
		{name: "a target without TTLs", policy: ydbschema.TTL{Column: "ts", Interval: "P1D"},
			caps: capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).With(capability.RowDeletionPolicy, false),
			want: "requires target capability row_deletion_policy"},
		{name: "a column YDB reads no TTL from", policy: ydbschema.TTL{Column: "n", Interval: "P1D", Unit: "SECONDS"}, caps: capability.YDB262(),
			want: `column "n" is Int64`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: test.caps,
				Tables: []featureplan.Table{rebuiltEvents(test.policy)}, ParentKinds: []schemaext.Kind{ydbschema.TTLKind}}
			result, err := ydbplan.TTLService{}.PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.ValidateOutcome(request), qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, test.want)
			c.Assert(result.Parents, qt.HasLen, 0)
		})
	}
}

func TestTTLPlanFeatures_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	tests := []struct {
		name    string
		ctx     context.Context
		target  string
		wantErr string
	}{
		{name: "canceled", ctx: canceled, target: "ydb", wantErr: "context canceled"},
		{name: "another target", ctx: t.Context(), target: "spanner", wantErr: `.*YDB TTL planning on "spanner".*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := ttlRequestFixture(nil, &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P1D"}})
			request.Target = test.target
			result, err := ydbplan.TTLService{}.PlanFeatures(test.ctx, request)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
		})
	}
}
