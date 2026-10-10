package pgpolicyprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policyreverse"
)

const accessLimitation = "Restoring the previous row-security definition does not undo access the forward plan granted or withheld while it stood."

func reversal(changes ...schemaext.ChangeRecord) schemaext.ReversalRequest {
	return schemaext.ReversalRequest{Target: "postgres", Identifiers: postgres, Changes: changes}
}

// TestReverseChanges_HappyPath pins each inverse: the forward declaration
// becomes the state the reverse starts from, as the server reports it, the
// observation it replaced becomes the declaration the reverse applies, and the
// access is assessed again from the reversed operands. Every inverse says it
// does not undo access the forward plan granted or withheld.
func TestReverseChanges_HappyPath(t *testing.T) {
	commented := restrictiveObserved
	commented.Comment = "tenants"
	restored := pgpolicy.DesiredPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{public}, Using: new("tenant_id = 1"),
		Composition: pgpolicy.Restrictive}
	restoredPermissive := pgpolicy.DesiredPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{reader}, Using: new("tenant_id = 1"),
		Composition: pgpolicy.Permissive}
	recommented := restored
	recommented.Comment = "tenants"
	selectOnly := permissiveDeclared
	selectOnly.Command = pgpolicy.CommandSelect
	selectObserved := pgpolicy.ObservedPolicy{Command: pgpolicy.CommandSelect, Roles: []pgpolicy.RoleSelector{reader},
		Using: new("tenant_id = 1"), Composition: pgpolicy.Permissive}
	tests := []struct {
		name     string
		change   schemaext.ChangeRecord
		want     *pgpolicy.PolicyChange
		strategy string
	}{
		{name: "a creation", change: policyChange("tenant", nil, &permissiveDeclared, false), strategy: "drop the created policy",
			want: &pgpolicy.PolicyChange{Before: &permissiveObserved,
				Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "removing a permissive policy can hide the rows it admitted"}}},
		{name: "a drop", change: policyChange("tenant", &restrictiveObserved, nil, false), strategy: "recreate the dropped policy from its captured definition",
			want: &pgpolicy.PolicyChange{After: &restored,
				Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "a restrictive policy can hide rows from the roles it names"}}},
		{name: "a change", change: policyChange("tenant", &permissiveObserved, &selectOnly, false), strategy: "replace the policy with its captured definition",
			want: &pgpolicy.PolicyChange{Before: &selectObserved, After: &restoredPermissive,
				Access: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "the policy applies to commands it did not apply to"}}},
		{name: "a comment", change: policyChange("tenant", &restrictiveObserved, &pgpolicy.DesiredPolicy{Composition: pgpolicy.Restrictive,
			Using: new("tenant_id = 1"), Comment: "tenants"}, true), strategy: "restore the policy's captured comment",
			want: &pgpolicy.PolicyChange{Before: &commented, After: &restored, CommentOnly: true,
				Access: schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "only the policy's comment changes"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversed, err := newRuntime(c).ReverseChanges(t.Context(), reversal(test.change))

			c.Assert(err, qt.IsNil)
			c.Assert(reversed, qt.HasLen, 1)
			c.Assert(reversed[0].Change.Subject, qt.DeepEquals, test.change.Subject)
			c.Assert(reversed[0].Change.Value, qt.DeepEquals, schemaext.ChangeValue(test.want))
			c.Assert(reversed[0].Strategy, qt.Equals, test.strategy)
			c.Assert(reversed[0].Limitations, qt.DeepEquals, []string{accessLimitation})
		})
	}
}

// TestReverseChanges_ProjectsTheForwardState pins the state a forward change
// leaves: the policy it created, as the server reports it, and nothing for one
// it dropped.
func TestReverseChanges_ProjectsTheForwardState(t *testing.T) {
	tests := []struct {
		name   string
		change schemaext.ChangeRecord
		want   schemaext.Value
	}{
		{name: "a creation", change: policyChange("tenant", nil, &permissiveDeclared, false), want: &permissiveObserved},
		{name: "a drop", change: policyChange("tenant", &restrictiveObserved, nil, false)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversed, err := newRuntime(c).ReverseChanges(t.Context(), reversal(test.change))

			c.Assert(err, qt.IsNil)
			c.Assert(reversed[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{
				{Placement: schemaext.ObjectPlacement, Kind: pgpolicy.PolicyKind, Value: test.want}})
		})
	}
}

// TestReverseChanges_TableSwitches pins the inverse of a switch change: the
// switches swap, the access is assessed again, and the forward switches are
// the table's projected state.
func TestReverseChanges_TableSwitches(t *testing.T) {
	c := qt.New(t)

	reversed, err := newRuntime(c).ReverseChanges(t.Context(),
		reversal(switchChange("orders", pgpolicy.ObservedTableState{Enabled: true, Forced: true}, pgpolicy.DesiredTableState{Enabled: true})))

	c.Assert(err, qt.IsNil)
	c.Assert(reversed[0].Change.Value, qt.DeepEquals, schemaext.ChangeValue(&pgpolicy.TableStateChange{
		Before: &pgpolicy.ObservedTableState{Enabled: true}, After: &pgpolicy.DesiredTableState{Enabled: true, Forced: true},
		Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "FORCE ROW LEVEL SECURITY subjects the table's owner to its policies"}}))
	c.Assert(reversed[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{
		{Placement: schemaext.FacetPlacement, Kind: pgpolicy.TableStateKind, Value: &pgpolicy.ObservedTableState{Enabled: true}}})
	c.Assert(reversed[0].Limitations, qt.DeepEquals, []string{accessLimitation})
}

// TestReverseChanges_FailurePath pins the inverses that have no answer: a
// policy TO a role keyword nobody resolved, whose forward state names a role
// no prediction can know, and a target outside the PostgreSQL family asked
// directly.
func TestReverseChanges_FailurePath(t *testing.T) {
	c := qt.New(t)
	keyword := pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}}, Using: new("true")}

	reversed, err := newRuntime(c).ReverseChanges(t.Context(), reversal(policyChange("tenant", nil, &keyword, false)))

	c.Assert(err, qt.ErrorIs, schemaext.ErrIrreversible)
	c.Assert(reversed, qt.IsNil)

	request := reversal(policyChange("tenant", nil, &permissiveDeclared, false))
	request.Target = "mysql"
	direct, err := policyreverse.Service{}.ReverseChanges(t.Context(), request)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(direct, qt.IsNil)
}
