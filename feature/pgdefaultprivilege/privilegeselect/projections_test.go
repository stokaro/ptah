package privilegeselect_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegeselect"
)

func projectionRef() objectidentity.ID {
	return must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}, identifier.ForDialect("postgres")))
}

func TestSelectAdditionsRetainsACLAdditionsAndBefore(t *testing.T) {
	before := &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "EXECUTE"}}}
	tests := []struct {
		name   string
		change pgdefaultprivilege.Change
		want   schemaext.ChangeValue
	}{
		{name: "mixed", change: pgdefaultprivilege.Change{Before: before, Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Revoked: []pgdefaultprivilege.Grant{{Privilege: "EXECUTE"}}, GrantOptionsAdded: []string{"INSERT"}, GrantOptionsRevoked: []string{"UPDATE"}}, want: &pgdefaultprivilege.Change{Before: before, Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, GrantOptionsAdded: []string{"INSERT"}}},
		{name: "revocation", change: pgdefaultprivilege.Change{Before: before, Revoked: []pgdefaultprivilege.Grant{{Privilege: "EXECUTE"}}}},
		{name: "option revocation", change: pgdefaultprivilege.Change{Before: before, GrantOptionsRevoked: []string{"EXECUTE"}}},
		{name: "restoration", change: pgdefaultprivilege.Change{Before: before, Restore: &pgdefaultprivilege.Observed{}}},
		{name: "unread grant", change: pgdefaultprivilege.Change{UnknownBefore: true, Granted: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}, want: &pgdefaultprivilege.Change{UnknownBefore: true, Granted: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}},
		{name: "unread revoke", change: pgdefaultprivilege.Change{UnknownBefore: true, Revoked: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			snapshot := test.change.Copy()
			result, err := (privilegeselect.Service{}).SelectAdditions(c.Context(), selectext.AdditiveRequest{Changes: []schemaext.ChangeRecord{{Subject: projectionRef(), Value: &test.change}}})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, selectext.AdditiveResult{Complete: true, Decisions: []selectext.AdditiveDecision{{Subject: projectionRef(), Kind: pgdefaultprivilege.ChangeKind, Value: test.want}}})
			c.Assert(&test.change, qt.DeepEquals, snapshot)
		})
	}
}

func observedProjection(value pgdefaultprivilege.Observed) schemaext.Objects {
	return must.Must(schemaext.NewObjects(must.Must(pgdefaultprivilege.ObservedObject(projectionRef(), value))))
}

func TestSubtractBaselineSelectsIndividualACLRightIdentities(t *testing.T) {
	current := observedProjection(pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}, {Privilege: "INSERT"}}, Revoked: []string{"EXECUTE"}})
	tests := []struct {
		name               string
		baseline, declared schemaext.Objects
		want               schemaext.Value
	}{
		{name: "no baseline", want: &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}, {Privilege: "SELECT", WithOption: true}}, Revoked: []string{"EXECUTE"}}},
		{name: "nil declaration", baseline: observedProjection(pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}, {Privilege: "EXECUTE"}}}), want: &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}}}},
		{name: "keep revoked identity", baseline: observedProjection(pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}, {Privilege: "EXECUTE"}}}), declared: observedProjection(pgdefaultprivilege.Observed{Revoked: []string{"EXECUTE"}}), want: &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}}, Revoked: []string{"EXECUTE"}}},
		{name: "declare all", baseline: observedProjection(pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}, {Privilege: "EXECUTE"}}}), declared: observedProjection(pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}), want: &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}, {Privilege: "SELECT", WithOption: true}}, Revoked: []string{"EXECUTE"}}},
		{name: "remove all", baseline: current},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (privilegeselect.Service{}).SubtractBaseline(c.Context(), selectext.BaselineRequest{Current: current, StartingPoint: test.baseline, Declared: test.declared})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Decisions, qt.HasLen, 1)
			c.Assert(result.Decisions[0].Subject, qt.Equals, projectionRef())
			c.Assert(result.Decisions[0].Value, qt.DeepEquals, test.want)
		})
	}
}

func TestOwnerProjectionsRefuseMalformedAndCanceledBatches(t *testing.T) {
	c := qt.New(t)
	changes, err := (privilegeselect.Service{}).SelectAdditions(c.Context(), selectext.AdditiveRequest{Changes: []schemaext.ChangeRecord{{Subject: projectionRef(), Value: (*pgdefaultprivilege.Change)(nil)}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(changes, qt.DeepEquals, selectext.AdditiveResult{})
	objects, err := (privilegeselect.Service{}).SubtractBaseline(c.Context(), selectext.BaselineRequest{Current: must.Must(schemaext.NewObjects(schemaext.Object{Ref: projectionRef(), Value: &pgdefaultprivilege.Desired{}}))})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(objects, qt.DeepEquals, selectext.BaselineResult{})
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	changes, err = (privilegeselect.Service{}).SelectAdditions(ctx, selectext.AdditiveRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(changes, qt.DeepEquals, selectext.AdditiveResult{})
	objects, err = (privilegeselect.Service{}).SubtractBaseline(ctx, selectext.BaselineRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(objects, qt.DeepEquals, selectext.BaselineResult{})
}
