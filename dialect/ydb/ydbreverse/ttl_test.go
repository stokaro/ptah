package ydbreverse_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
)

// reversalRuntime selects only this owner, so the engine's reply validation
// runs without the bundled runtime.
func reversalRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: ydbschema.Owner, Targets: []engine.Target{{Name: "ydb"}},
		Codecs:    append(ydbschema.TTLCodecs(), []schemaext.Codec{ydbdiff.TTLCodec()}...),
		Reversals: []engine.Reversal{{Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.TTLKind}, Service: ydbreverse.TTLService{}}},
	}))
}

func record(before, after *ydbschema.TTL) schemaext.ChangeRecord {
	change := &ydbdiff.TTL{}
	if before != nil {
		change.Before = &ydbschema.ObservedTTL{Policy: *before}
	}
	if after != nil {
		change.After = &ydbschema.DesiredTTL{Policy: *after}
	}
	subject := objectidentity.NewBuilder(identifier.ForDialect("ydb")).Table("sessions")
	return schemaext.ChangeRecord{Subject: subject, Value: change}
}

// TestReverseChanges_RestoresThePriorPolicy pins each reversal: the operands
// swap sides, the forward state predicts the policy the forward change leaves,
// and a forward policy that deleted rows reports that restoring the prior one
// cannot bring them back.
func TestReverseChanges_RestoresThePriorPolicy(t *testing.T) {
	added := &ydbschema.TTL{Column: "created_at", Interval: "P30D"}
	changed := &ydbschema.TTL{Column: "created_at", Interval: "P60D"}
	tests := []struct {
		name            string
		before, after   *ydbschema.TTL
		wantBefore      *ydbschema.ObservedTTL
		wantAfter       *ydbschema.DesiredTTL
		wantForward     schemaext.Value
		wantStrategy    string
		wantLimitations int
	}{
		{
			name: "a policy the forward change added is removed", after: added,
			wantBefore: &ydbschema.ObservedTTL{Policy: *added}, wantForward: &ydbschema.ObservedTTL{Policy: *added},
			wantStrategy: "remove the TTL the forward change added", wantLimitations: 1,
		},
		{
			name: "a policy the forward change removed is restored", before: added,
			wantAfter: &ydbschema.DesiredTTL{Policy: *added}, wantForward: nil,
			wantStrategy: "restore the captured TTL in place", wantLimitations: 0,
		},
		{
			name: "a changed policy is restored", before: added, after: changed,
			wantBefore: &ydbschema.ObservedTTL{Policy: *changed}, wantAfter: &ydbschema.DesiredTTL{Policy: *added},
			wantForward:  &ydbschema.ObservedTTL{Policy: *changed},
			wantStrategy: "restore the captured TTL in place", wantLimitations: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := record(test.before, test.after)
			result, err := reversalRuntime().ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "ydb", Changes: []schemaext.ChangeRecord{input}})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			c.Assert(result[0].Change.Subject, qt.DeepEquals, input.Subject)
			reversed := result[0].Change.Value.(*ydbdiff.TTL)
			c.Assert(reversed.Before, qt.DeepEquals, test.wantBefore)
			c.Assert(reversed.After, qt.DeepEquals, test.wantAfter)
			c.Assert(result[0].ForwardState, qt.HasLen, 1)
			c.Assert(result[0].ForwardState[0].Placement, qt.Equals, schemaext.FacetPlacement)
			c.Assert(result[0].ForwardState[0].Kind, qt.Equals, ydbschema.TTLKind)
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
		{name: "canceled", ctx: canceled, request: schemaext.ReversalRequest{Target: "ydb"}, wantErr: "context canceled"},
		{name: "another target", ctx: t.Context(), request: schemaext.ReversalRequest{Target: "postgres"}, wantErr: `.*YDB TTL reversal on "postgres".*`},
		{name: "a change that changes nothing", ctx: t.Context(), request: schemaext.ReversalRequest{Target: "ydb", Changes: []schemaext.ChangeRecord{
			record(&ydbschema.TTL{Column: "ts", Interval: "P30D"}, &ydbschema.TTL{Column: "ts", Interval: "PT720H"}),
		}}, wantErr: `(?s).*operands contain no change.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := ydbreverse.TTLService{}.ReverseChanges(test.ctx, test.request)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.IsNil)
		})
	}
}
