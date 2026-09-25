package rlspolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/rlspolicy"
)

// TestCommand_FoldsAnUnspecifiedClauseOntoALL pins the fold.
//
// The spellings that must agree are listed against each other rather than
// against a hand-written expectation, because the property is that two sides of
// a comparison land on one value -- not what that value is called.
func TestCommand_FoldsAnUnspecifiedClauseOntoALL(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		same bool
	}{
		{name: "unspecified equals ALL", a: "", b: "ALL", same: true},
		{name: "ALL equals unspecified", a: "ALL", b: "", same: true},
		{name: "whitespace is unspecified", a: "   ", b: "ALL", same: true},
		{name: "case does not separate them", a: "all", b: "ALL", same: true},
		{name: "surrounding space does not either", a: " ALL ", b: "ALL", same: true},
		{name: "SELECT is not ALL", a: "SELECT", b: "ALL", same: false},
		{name: "SELECT is not unspecified", a: "SELECT", b: "", same: false},
		{name: "INSERT is not UPDATE", a: "INSERT", b: "UPDATE", same: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			folded := rlspolicy.Command(test.a) == rlspolicy.Command(test.b)

			c.Assert(folded, qt.Equals, test.same,
				qt.Commentf("%q vs %q folded to %q and %q",
					test.a, test.b, rlspolicy.Command(test.a), rlspolicy.Command(test.b)))
		})
	}
}

// The fold answers with a spelling a renderer would accept, so a caller that
// does write it somewhere writes a real clause rather than a marker.
func TestCommand_AnswersWithARealClause(t *testing.T) {
	c := qt.New(t)

	c.Assert(rlspolicy.Command(""), qt.Equals, "ALL")
	c.Assert(rlspolicy.Command("select"), qt.Equals, "SELECT")
}

// TestRoles_PostgresFamilyReadsTheClauseAsASet pins the fold on the targets
// where an omitted TO means PUBLIC. Like the FOR clause test above, the
// spellings are compared with each other, because the property is that the two
// sides of a comparison land on one value.
func TestRoles_PostgresFamilyReadsTheClauseAsASet(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		same bool
	}{
		{name: "omitted equals the PUBLIC the catalog reports", a: "", b: "PUBLIC", same: true},
		{name: "whitespace is omitted", a: "  ", b: "PUBLIC", same: true},
		{name: "public in any case", a: "public", b: "PUBLIC", same: true},
		{name: "PUBLIC beside a role is PUBLIC", a: "PUBLIC, app_a", b: "PUBLIC", same: true},
		{name: "a role beside PUBLIC is PUBLIC", a: "app_a,public", b: "", same: true},
		{name: "the separator does not matter", a: "app_a, app_b", b: "app_a,app_b", same: true},
		{name: "the order does not matter", a: "app_b, app_a", b: "app_a,app_b", same: true},
		{name: "a repeated role is one role", a: "app_a, app_a", b: "app_a", same: true},
		{name: "a role is not PUBLIC", a: "app_a", b: "", same: false},
		{name: "a role is not the PUBLIC the catalog reports", a: "app_a", b: "PUBLIC", same: false},
		{name: "two roles are not one", a: "app_a, app_b", b: "app_a", same: false},
		{name: "role names keep their case", a: "App_A", b: "app_a", same: false},
	}
	for _, dialect := range []string{"postgres", "cockroachdb", "yugabytedb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				folded := rlspolicy.Roles(dialect, test.a) == rlspolicy.Roles(dialect, test.b)
				c.Assert(folded, qt.Equals, test.same,
					qt.Commentf("%q vs %q folded to %q and %q",
						test.a, test.b, rlspolicy.Roles(dialect, test.a), rlspolicy.Roles(dialect, test.b)))
			})
		}
	}
}

// TestRoles_OtherTargetsKeepTheClause is the control: an omitted TO does not
// mean PUBLIC everywhere. A ClickHouse row policy with no TO applies to nobody,
// so folding it onto PUBLIC would call a policy for nobody equal to one for
// everybody.
func TestRoles_OtherTargetsKeepTheClause(t *testing.T) {
	tests := []struct {
		dialect string
		roles   string
	}{
		{dialect: "clickhouse", roles: ""},
		{dialect: "clickhouse", roles: "ALL"},
		{dialect: "sqlserver", roles: ""},
		{dialect: "", roles: ""},
		{dialect: "", roles: "app_b, app_a"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.roles, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rlspolicy.Roles(test.dialect, test.roles), qt.Equals, test.roles)
		})
	}
}
