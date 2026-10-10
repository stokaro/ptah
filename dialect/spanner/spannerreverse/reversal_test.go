package spannerreverse_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerdiff"
	"ptah.run/dialect/spanner/spannerreverse"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/engine"
)

// reversalRuntime selects only this owner, so the engine's reply validation
// runs without the bundled runtime.
func reversalRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: spannerschema.Owner, Targets: []engine.Target{{Name: "spanner"}},
		Codecs:    append(spannerschema.Codecs(), spannerdiff.Codecs()...),
		Reversals: []engine.Reversal{{Target: "spanner", Kinds: []schemaext.Kind{spannerdiff.RowDeletionKind}, Service: spannerreverse.Service{}}},
	}))
}

func record(before, after *spannerschema.Policy) schemaext.ChangeRecord {
	change := &spannerdiff.RowDeletion{}
	if before != nil {
		change.Before = &spannerschema.ObservedRowDeletion{Policy: *before}
	}
	if after != nil {
		change.After = &spannerschema.DesiredRowDeletion{Policy: *after}
	}
	subject := objectidentity.NewBuilder(identifier.ForDialect("spanner")).Table("sessions")
	return schemaext.ChangeRecord{Subject: subject, Value: change}
}

// TestReverseChanges_RestoresThePriorPolicy pins each reversal: the operands
// swap sides, the forward state predicts the policy the forward change leaves,
// and a forward policy that deleted rows reports that restoring the prior one
// cannot bring them back.
func TestReverseChanges_RestoresThePriorPolicy(t *testing.T) {
	added := &spannerschema.Policy{Column: "created_at", Interval: "30 days"}
	changed := &spannerschema.Policy{Column: "created_at", Interval: "60 days"}
	tests := []struct {
		name            string
		before, after   *spannerschema.Policy
		wantBefore      *spannerschema.ObservedRowDeletion
		wantAfter       *spannerschema.DesiredRowDeletion
		wantForward     schemaext.Value
		wantStrategy    string
		wantLimitations int
	}{
		{
			name: "a policy the forward change added is removed", after: added,
			wantBefore: &spannerschema.ObservedRowDeletion{Policy: *added}, wantForward: &spannerschema.ObservedRowDeletion{Policy: *added},
			wantStrategy: "remove the row deletion policy the forward change added", wantLimitations: 1,
		},
		{
			name: "a policy the forward change removed is restored", before: added,
			wantAfter: &spannerschema.DesiredRowDeletion{Policy: *added}, wantForward: nil,
			wantStrategy: "restore the captured row deletion policy in place", wantLimitations: 0,
		},
		{
			name: "a changed policy is restored", before: added, after: changed,
			wantBefore: &spannerschema.ObservedRowDeletion{Policy: *changed}, wantAfter: &spannerschema.DesiredRowDeletion{Policy: *added},
			wantForward:  &spannerschema.ObservedRowDeletion{Policy: *changed},
			wantStrategy: "restore the captured row deletion policy in place", wantLimitations: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := record(test.before, test.after)
			result, err := reversalRuntime().ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "spanner", Changes: []schemaext.ChangeRecord{input}})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			c.Assert(result[0].Change.Subject, qt.DeepEquals, input.Subject)
			reversed := result[0].Change.Value.(*spannerdiff.RowDeletion)
			c.Assert(reversed.Before, qt.DeepEquals, test.wantBefore)
			c.Assert(reversed.After, qt.DeepEquals, test.wantAfter)
			c.Assert(result[0].ForwardState, qt.HasLen, 1)
			c.Assert(result[0].ForwardState[0].Placement, qt.Equals, schemaext.FacetPlacement)
			c.Assert(result[0].ForwardState[0].Kind, qt.Equals, spannerschema.RowDeletionKind)
			c.Assert(result[0].ForwardState[0].Value, qt.DeepEquals, test.wantForward)
			c.Assert(result[0].Strategy, qt.Equals, test.wantStrategy)
			c.Assert(result[0].Limitations, qt.HasLen, test.wantLimitations)
		})
	}
}

func TestReverseChanges_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	tests := []struct {
		name    string
		ctx     context.Context
		request schemaext.ReversalRequest
		wantErr string
	}{
		{name: "canceled", ctx: canceled, request: schemaext.ReversalRequest{Target: "spanner"}, wantErr: "context canceled"},
		{name: "another target", ctx: t.Context(), request: schemaext.ReversalRequest{Target: "postgres"}, wantErr: `.*Spanner reversal on "postgres".*`},
		{name: "a change that changes nothing", ctx: t.Context(), request: schemaext.ReversalRequest{Target: "spanner", Changes: []schemaext.ChangeRecord{
			record(&spannerschema.Policy{Column: "ts", Interval: "4 WEEKS 2 DAYS"}, &spannerschema.Policy{Column: "ts", Interval: "30 days"}),
		}}, wantErr: `(?s).*operands contain no change.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := spannerreverse.Service{}.ReverseChanges(test.ctx, test.request)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.IsNil)
		})
	}
}
