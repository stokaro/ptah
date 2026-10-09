package crdbreverse_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbreverse"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/engine"
)

// reversalRuntime selects only this owner, so the engine's reply validation
// runs without the bundled runtime.
func reversalRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: crdbschema.Owner, Targets: []engine.Target{{Name: "cockroachdb"}},
		Codecs:    append(crdbschema.Codecs(), crdbdiff.Codecs()...),
		Reversals: []engine.Reversal{{Target: "cockroachdb", Kinds: []schemaext.Kind{crdbdiff.RowTTLKind}, Service: crdbreverse.Service{}}},
	}))
}

func record(before, after *crdbschema.Policy) schemaext.ChangeRecord {
	change := &crdbdiff.RowTTL{}
	if before != nil {
		change.Before = &crdbschema.ObservedRowTTL{Policy: *before}
	}
	if after != nil {
		change.After = &crdbschema.DesiredRowTTL{Policy: *after}
	}
	subject := objectidentity.NewBuilder(identifier.ForDialect("cockroachdb")).Table("sessions")
	return schemaext.ChangeRecord{Subject: subject, Value: change}
}

// TestReverseChanges_RestoresThePriorPolicy pins each reversal: the operands
// swap sides, the forward state predicts the policy the forward change leaves,
// and a forward policy that deleted rows reports that restoring the prior one
// cannot bring them back.
func TestReverseChanges_RestoresThePriorPolicy(t *testing.T) {
	added := &crdbschema.Policy{ExpirationExpression: "expires_at"}
	changed := &crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@hourly"}
	tests := []struct {
		name            string
		before, after   *crdbschema.Policy
		wantBefore      *crdbschema.ObservedRowTTL
		wantAfter       *crdbschema.DesiredRowTTL
		wantForward     schemaext.Value
		wantStrategy    string
		wantLimitations int
	}{
		{
			name: "a policy the forward change added is removed", after: added,
			wantBefore: &crdbschema.ObservedRowTTL{Policy: *added}, wantForward: &crdbschema.ObservedRowTTL{Policy: *added},
			wantStrategy: "remove the row-level TTL the forward change added", wantLimitations: 1,
		},
		{
			name: "a policy the forward change removed is restored", before: added,
			wantAfter: &crdbschema.DesiredRowTTL{Policy: *added}, wantForward: nil,
			wantStrategy: "restore the captured row-level TTL storage parameters in place", wantLimitations: 0,
		},
		{
			name: "a changed policy is restored", before: added, after: changed,
			wantBefore: &crdbschema.ObservedRowTTL{Policy: *changed}, wantAfter: &crdbschema.DesiredRowTTL{Policy: *added},
			wantForward:  &crdbschema.ObservedRowTTL{Policy: *changed},
			wantStrategy: "restore the captured row-level TTL storage parameters in place", wantLimitations: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := record(test.before, test.after)
			result, err := reversalRuntime().ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "cockroachdb", Changes: []schemaext.ChangeRecord{input}})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			c.Assert(result[0].Change.Subject, qt.DeepEquals, input.Subject)
			reversed := result[0].Change.Value.(*crdbdiff.RowTTL)
			c.Assert(reversed.Before, qt.DeepEquals, test.wantBefore)
			c.Assert(reversed.After, qt.DeepEquals, test.wantAfter)
			c.Assert(result[0].ForwardState, qt.HasLen, 1)
			c.Assert(result[0].ForwardState[0].Placement, qt.Equals, schemaext.FacetPlacement)
			c.Assert(result[0].ForwardState[0].Kind, qt.Equals, crdbschema.RowTTLKind)
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
		{name: "canceled", ctx: canceled, request: schemaext.ReversalRequest{Target: "cockroachdb"}, wantErr: "context canceled"},
		{name: "another target", ctx: t.Context(), request: schemaext.ReversalRequest{Target: "postgres"}, wantErr: `.*CockroachDB reversal on "postgres".*`},
		{name: "a change that changes nothing", ctx: t.Context(), request: schemaext.ReversalRequest{Target: "cockroachdb", Changes: []schemaext.ChangeRecord{
			record(&crdbschema.Policy{ExpireAfter: "72:00:00"}, &crdbschema.Policy{ExpireAfter: "72 hours"}),
		}}, wantErr: `(?s).*operands contain no change.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := crdbreverse.Service{}.ReverseChanges(test.ctx, test.request)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.IsNil)
		})
	}
}
