package mysql

// White-box testing required: the requirement is decided by an unexported
// predicate, and the one place that reads it is currently shielded by an
// earlier gate (see the test comment below), so the decision cannot be observed
// from outside the package at all.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// The SHOW_ROUTINE requirement gates a destructive operation: the privilege set
// is what proves the metadata read saw everything, and that proof is what
// authorizes dropping the schema. An unreadable version therefore has to demand
// the privilege rather than excuse it.
//
// This is hardening, not a live fix. `DropAllTables` runs the VIEW_TABLE_USAGE
// gate first, and that one already refuses a MySQL version it cannot parse, so
// no unparseable version reaches the privilege decision today. The ordering of
// two independent checks is the only thing standing between the old answer and
// a weaker privilege set authorizing a drop, which is not something worth
// leaving to ordering (stokaro/ptah#916).
func TestShowRoutineRequirementFailsClosedOnAnUnreadableVersion(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    bool
	}{
		// 8.0.20 introduced the privilege.
		{name: "the release that introduced it", version: "8.0.20", want: true},
		{name: "one patch below it", version: "8.0.19", want: false},
		{name: "an older 8.0", version: "8.0.13", want: false},
		{name: "a later 8.x minor", version: "8.4.0", want: true},
		{name: "a later major", version: "9.7.1", want: true},
		{name: "a much later major", version: "26.7.0", want: true},
		{name: "an older major", version: "5.7.44", want: false},

		// The reason this test exists.
		{name: "an unreadable version", version: "not-a-version", want: true},
		{name: "an empty version", version: "", want: true},
		{name: "a two-part version", version: "8.0", want: true},
		{name: "a non-numeric component", version: "8.x.20", want: true},

		// A build suffix is read, not refused: the numeric head parses.
		{name: "a distribution build suffix", version: "8.0.36-0ubuntu0.22.04.1", want: true},
		{name: "a pre-8.0.20 build suffix", version: "8.0.19-0ubuntu0.20.04.3", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(requiresMySQLShowRoutinePrivilege(tt.version), qt.Equals, tt.want)
		})
	}
}
