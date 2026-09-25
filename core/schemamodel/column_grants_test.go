package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// TestGrant_ByColumn pins the split a column-by-column comparison reads: one
// grant per column, each keyed by its table and that column, and a grant on a
// whole object left as it is.
func TestGrant_ByColumn(t *testing.T) {
	c := qt.New(t)
	grant := schemamodel.Grant{Role: "app", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"a", "b"}}

	split := grant.ByColumn()
	whole := schemamodel.Grant{Role: "app", Privileges: []string{"UPDATE"}, OnTable: "t"}.ByColumn()

	c.Assert(split, qt.HasLen, 2)
	c.Assert(split[0].TargetKey(), qt.Equals, "TABLE t (a)")
	c.Assert(split[1].TargetKey(), qt.Equals, "TABLE t (b)")
	c.Assert(grant.Columns, qt.DeepEquals, []string{"a", "b"})
	c.Assert(whole, qt.HasLen, 1)
	c.Assert(whole[0].TargetKey(), qt.Equals, "TABLE t")
}

// TestValidateRevokedGrants_Columns pins which column declarations contradict
// each other. A column grant beside a revoke of the table privilege is the
// shape a schema writes to leave only some columns writable, and holds. A
// table grant beside a revoke of the same privilege on one column cannot both
// hold: PostgreSQL does not take a column out of a table privilege.
func TestValidateRevokedGrants_Columns(t *testing.T) {
	c := qt.New(t)
	columnUnderTableRevoke := &schemamodel.Database{
		Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"a"}}},
		RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"UPDATE"}, OnTable: "t"}},
	}
	tableUnderColumnRevoke := &schemamodel.Database{
		Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"UPDATE"}, OnTable: "t"}},
		RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"a", "b"}}},
	}
	sameColumn := &schemamodel.Database{
		Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"a", "b"}}},
		RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t", Columns: []string{"b"}}},
	}

	c.Assert(schemamodel.ValidateRevokedGrants(columnUnderTableRevoke), qt.IsNil)
	c.Assert(schemamodel.ValidateRevokedGrants(tableUnderColumnRevoke), qt.ErrorMatches,
		`UPDATE on TABLE t \(a\) is both granted to and revoked from "app"; declare one or the other`)
	c.Assert(schemamodel.ValidateRevokedGrants(sameColumn), qt.ErrorMatches,
		`SELECT on TABLE t \(b\) is both granted to and revoked from "app"; declare one or the other`)
}
