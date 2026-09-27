package shadow

// White-box testing required: mismatch collection turns a schema diff into the
// deterministic list the verification error carries, and the ordering and
// qualification it applies are not observable without calling it directly.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestCollectMismatches_ReportsEveryDefaultPrivilegeDirection holds the drift
// rows for a family whose absence reads as agreement.
//
// Shadow verification reports what a replay did not reproduce, and it reports
// it from this list. A family the list does not name makes a change that never
// landed read as a clean apply, which is the answer the verification exists to
// refuse. The rows are named by the whole identity because a grantor and a
// grantee are both part of what makes one object.
func TestCollectMismatches_ReportsEveryDefaultPrivilegeDirection(t *testing.T) {
	c := qt.New(t)
	ref := difftypes.DefaultPrivilegeRef{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader", Privilege: "SELECT",
	}
	diff := &difftypes.SchemaDiff{
		DefaultPrivilegesAdded:         []difftypes.DefaultPrivilegeRef{ref},
		DefaultPrivilegesRemoved:       []difftypes.DefaultPrivilegeRef{ref},
		DefaultPrivilegeOptionsAdded:   []difftypes.DefaultPrivilegeRef{ref},
		DefaultPrivilegeOptionsRevoked: []difftypes.DefaultPrivilegeRef{ref},
	}
	object := "SELECT on TABLES in app for app_owner to app_reader"

	got := collectMismatches(diff)

	c.Assert(got, qt.DeepEquals, []Mismatch{
		{
			Kind:    "missing_default_privilege",
			Object:  object,
			Message: "missing default privilege " + object,
		},
		{
			Kind:    "extra_default_privilege",
			Object:  object,
			Message: "extra default privilege " + object,
		},
		{
			Kind:    "missing_default_privilege_option",
			Object:  object,
			Message: "missing default privilege grant option " + object,
		},
		{
			Kind:    "extra_default_privilege_option",
			Object:  object,
			Message: "extra default privilege grant option " + object,
		},
	})
}

// TestCollectMismatches_OrdersDefaultPrivilegesByTheirIdentity pins the order
// the rows come out in. A verification error is read by a person and diffed by
// a pipeline, so a list whose order follows the input is one nobody can compare
// between two runs.
func TestCollectMismatches_OrdersDefaultPrivilegesByTheirIdentity(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{DefaultPrivilegesAdded: []difftypes.DefaultPrivilegeRef{
		{
			Grantor: "beta_owner", Schema: "app", ObjectType: "TABLES",
			Grantee: "app_reader", Privilege: "SELECT",
		},
		{
			Grantor: "alpha_owner", Schema: "app", ObjectType: "TABLES",
			Grantee: "app_reader", Privilege: "SELECT",
		},
	}}

	got := collectMismatches(diff)

	c.Assert(got, qt.HasLen, 2)
	c.Assert(got[0].Object, qt.Equals, "SELECT on TABLES in app for alpha_owner to app_reader")
	c.Assert(got[1].Object, qt.Equals, "SELECT on TABLES in app for beta_owner to app_reader")
}

func TestCollectMismatches_ReportsQualifiedIndex(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{Name: "idx_shared", Fields: []string{"code"}}, TableName: "users"},
		{Index: schemamodel.Index{Name: "idx_shared", Fields: []string{"code"}}, TableName: "orders"},
	})

	got := collectMismatches(diff)

	c.Assert(got, qt.DeepEquals, []Mismatch{
		{
			Kind:    "missing_index",
			Table:   "orders",
			Object:  "orders.idx_shared",
			Message: "missing index orders.idx_shared",
		},
		{
			Kind:    "missing_index",
			Table:   "users",
			Object:  "users.idx_shared",
			Message: "missing index users.idx_shared",
		},
	})
}

// TestNewBaselineMismatchError_ReportsWhatTheTargetReadCouldNotCheck holds the
// baseline to the objects the replay created and the target read could not
// see. They are mismatches, one each after the differences, and without a
// difference they are the whole error rather than a pass (stokaro/ptah#3844).
func TestNewBaselineMismatchError_ReportsWhatTheTargetReadCouldNotCheck(t *testing.T) {
	withheld := coverage.Refused(coverage.Role)
	withheld.Name = "reporter"
	undecided := Mismatch{
		Kind:   "undecided_object",
		Object: "reporter",
		Message: "undecided role reporter: the target database does not describe role objects" +
			" because the read was refused the catalog that would have listed them",
	}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []Mismatch
	}{
		{
			name: "nothing differs",
			diff: &difftypes.SchemaDiff{},
			want: []Mismatch{undecided},
		},
		{
			name: "a missing table beside it",
			diff: &difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{Name: "notes"}}},
			want: []Mismatch{
				{Kind: "missing_table", Table: "notes", Object: "notes", Message: "missing table notes"},
				undecided,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := newBaselineMismatchError(test.diff, []coverage.Object{withheld})

			c.Assert(err.Result.Stage, qt.Equals, "schema-match")
			c.Assert(err.Result.Mismatches, qt.DeepEquals, test.want)
		})
	}
}
