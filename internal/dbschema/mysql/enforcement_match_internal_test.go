package mysql

// White-box testing required: newConstraint and enforcedExpr are unexported,
// and what they read are the catalog's own values. Reaching them through
// ReadSchema would mean scripting every other catalog query to observe these
// two. The live tests in integration/ read the clauses off MySQL.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
)

// TestNewConstraint_ReadsMatchAndEnforcement reads the values MySQL 8.4.11
// and 9.7.2 report: MATCH_OPTION FULL, PARTIAL or NONE, and ENFORCED YES or NO
// (stokaro/ptah#3853).
func TestNewConstraint_ReadsMatchAndEnforcement(t *testing.T) {
	tests := []struct {
		name            string
		matchOption     string
		enforced        string
		wantMatch       string
		wantNotEnforced bool
	}{
		{name: "MATCH FULL", matchOption: "FULL", enforced: "YES", wantMatch: "FULL"},
		{name: "MATCH PARTIAL", matchOption: "PARTIAL", enforced: "YES", wantMatch: "PARTIAL"},
		{name: "MATCH SIMPLE, reported as NONE", matchOption: "NONE", enforced: "YES"},
		{name: "a CHECK not enforced", enforced: "NO", wantNotEnforced: true},
		{name: "a constraint enforced", enforced: "YES"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			constraint := newConstraint("k", "t", "FOREIGN KEY",
				constraintRefs{matchOption: test.matchOption, enforced: test.enforced}, checkConstraintClauses{})

			c.Assert(constraint.Match, qt.Equals, test.wantMatch)
			c.Assert(constraint.NotEnforced, qt.Equals, test.wantNotEnforced)
		})
	}
}

// TestEnforcedExpr_AsksWhereTheColumnExists reads TABLE_CONSTRAINTS.ENFORCED
// on a server that has it, and a constant on MariaDB, which answers ERROR 1054
// to the column.
func TestEnforcedExpr_AsksWhereTheColumnExists(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		want string
	}{
		{name: "MySQL", caps: capability.MySQL84(), want: "COALESCE(tc.ENFORCED, 'YES')"},
		{name: "MariaDB", caps: capability.MariaDB1011(), want: "'YES'"},
		{name: "MySQL before 8.0.16", caps: capability.MySQLLegacy(), want: "'YES'"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := NewMySQLReaderWithCapabilities(nil, "app", test.caps)

			c.Assert(reader.enforcedExpr(), qt.Equals, test.want)
		})
	}
}
