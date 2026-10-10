package chreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chreverse"
	"ptah.run/dialect/clickhouse/chschema"
)

// The inverse of each transition restores the captured policy: a creation is
// dropped, a drop is created again and a change is changed back, each with
// its own assessment. The forward state is the policy the forward change
// leaves, and every reversal says it cannot take back rows already read.
func TestRowPolicyReversal(t *testing.T) {
	before := &chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{Names: []string{"alice"}}}
	after := &chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{Names: []string{"alice"}}}
	for _, test := range []struct {
		name                     string
		change                   *chdiff.RowPolicy
		inverseBefore, inverseAt bool
		access                   schemaext.Access
		strategy                 string
	}{
		{"a creation", chdiff.NewRowPolicy(nil, after), true, false, schemaext.AccessUnknown, "drop the created row policy"},
		{"a drop", chdiff.NewRowPolicy(before, nil), false, true, schemaext.AccessUnknown, "create the dropped row policy again from its captured definition"},
		{"a change in place", chdiff.NewRowPolicy(before, after), true, true, schemaext.AccessWidens, "change the row policy back in place with ALTER ROW POLICY"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			record := schemaext.ChangeRecord{Subject: chschema.RowPolicyRef("app", "orders", "tenant"), Value: test.change}

			reversals, err := chreverse.RowPolicyService{}.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "clickhouse", Changes: []schemaext.ChangeRecord{record}})

			c.Assert(err, qt.IsNil)
			c.Assert(reversals, qt.HasLen, 1)
			inverse, ok := reversals[0].Change.Value.(*chdiff.RowPolicy)
			c.Assert(ok, qt.IsTrue)
			c.Assert(inverse.Before != nil, qt.Equals, test.inverseBefore)
			c.Assert(inverse.After != nil, qt.Equals, test.inverseAt)
			c.Assert(inverse.Access.Access, qt.Equals, test.access)
			c.Assert(reversals[0].Strategy, qt.Equals, test.strategy)
			c.Assert(reversals[0].Limitations, qt.DeepEquals, []string{
				"Restoring a row policy changes what its users can read from then on; it cannot undo rows they read while the forward plan applied."})
			c.Assert(reversals[0].ForwardState, qt.HasLen, 1)
			c.Assert(reversals[0].ForwardState[0].Placement, qt.Equals, schemaext.ObjectPlacement)
			c.Assert(reversals[0].ForwardState[0].Value != nil, qt.Equals, test.change.After != nil)
		})
	}
}

// A restored drop states the composition the captured policy had, so the
// inverse never depends on a default.
func TestRowPolicyReversalRestoresTheCapturedComposition(t *testing.T) {
	c := qt.New(t)
	before := &chschema.ObservedRowPolicy{Composition: chschema.Restrictive, Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}
	record := schemaext.ChangeRecord{Subject: chschema.RowPolicyRef("app", "orders", "tenant"), Value: chdiff.NewRowPolicy(before, nil)}

	reversals := must.Must(chreverse.RowPolicyService{}.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "clickhouse", Changes: []schemaext.ChangeRecord{record}}))

	c.Assert(reversals[0].Change.Value.(*chdiff.RowPolicy).After, qt.DeepEquals, before.Desired())
}

func TestRowPolicyReversal_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		target  string
		changes []schemaext.ChangeRecord
		want    error
	}{
		{"another target", "postgres", nil, ptaherr.ErrUnsupportedDialect},
		{"another change", "clickhouse", []schemaext.ChangeRecord{{Subject: chschema.RowPolicyRef("app", "orders", "tenant"), Value: &chdiff.Refresh{}}}, schemaext.ErrInvalidValue},
		{"a database-wide policy", "clickhouse", []schemaext.ChangeRecord{{Subject: chschema.RowPolicyRef("app", "", "tenant"),
			Value: chdiff.NewRowPolicy(nil, &chschema.DesiredRowPolicy{})}}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversals, err := chreverse.RowPolicyService{}.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: test.target, Changes: test.changes})

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(reversals, qt.IsNil)
		})
	}
}
