package capability_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// UNIQUE ... NULLS [NOT] DISTINCT arrived in PostgreSQL 15, and the version
// ladder sent every major at or above 14 to Postgres16, which declares the
// clause. The capability probe measured the disagreement on master:
//
//	cell postgres-14: CAPABILITY
//	  unique_nulls_distinct_clause: preset says true, server does false
//
// The probe fan-out does not run on a pull request, so a preset wrong for one
// release line reaches master and is caught by the nightly matrix rather than
// by the change that wrote it. This table is the offline half of that
// measurement: it pins the boundary version by version, so the next preset
// that lands on the wrong side of 15 fails where it is written.
//
// Both sides matter. A test asserting only that 14 is false would pass on a
// preset that turned the clause off for every line, and a test asserting only
// that 16 is true would pass on the bug this repairs.
func TestPostgresNullsDistinctClauseArrivesIn15(t *testing.T) {
	tests := []struct {
		name   string
		banner string
		want   bool
	}{
		{name: "12 is before the clause", banner: "12.20", want: false},
		{name: "13 is before the clause", banner: "13.16", want: false},
		{name: "14 is the line the probe caught", banner: "14.13", want: false},
		{name: "15 is the release that grew it", banner: "15.8", want: true},
		{name: "16 has it", banner: "16.4", want: true},
		{name: "17 has it", banner: "17.2", want: true},
		{name: "18 has it", banner: "18.0", want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			resolved := capability.ResolveServerVersion(platform.Postgres, test.banner)

			c.Assert(resolved.Capabilities.Has(capability.UniqueNullsDistinctClause),
				qt.Equals, test.want)
		})
	}
}

// TestPostgres14IsPostgres16WithOneKeyFlipped is the control that keeps the
// repair from being "PostgreSQL 14 lost a preset".
//
// 14 cannot take Postgres13 either: CREATE OR REPLACE TRIGGER and SP-GiST
// INCLUDE columns both arrived in 14, and Postgres13 is the preset that lacks
// them. So the new preset has to differ from Postgres16 in exactly one key,
// which this asserts by construction rather than by counting differences --
// the equality is the whole claim, and it fails on a preset that took away
// anything else.
func TestPostgres14IsPostgres16WithOneKeyFlipped(t *testing.T) {
	c := qt.New(t)

	fourteen := capability.Postgres14()

	c.Assert(fourteen, qt.DeepEquals,
		capability.Postgres16().With(capability.UniqueNullsDistinctClause, false))
	c.Assert(fourteen.Has(capability.CreateOrReplaceTrigger), qt.IsTrue)
	c.Assert(fourteen.Has(capability.IndexIncludeSPGiST), qt.IsTrue)
}
