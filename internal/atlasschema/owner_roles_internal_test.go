package atlasschema

// White-box testing required: refuseOwnerTransfers takes the writer through an
// unexported interface, and the exported path, CheckApplyStatements, reaches it
// only through a live connection's writer. The fake checker here is what lets
// an offline test see that plannedOwners hands each ownership statement the
// roles and memberships of the statements before it.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/objectowner"
)

// fakeOwnerChecker answers that app may not give an object to rv_a, and that
// the server holds rv_a and nothing else a test asks about.
type fakeOwnerChecker struct{}

func (fakeOwnerChecker) OwnerTransfers(context.Context) (objectowner.Transfers, error) {
	return objectowner.Transfers{Connected: "app", Privilege: "SET", Refused: []string{"rv_a"}}, nil
}

func (fakeOwnerChecker) HeldRoles(_ context.Context, role, _ string) ([]string, error) {
	return map[string][]string{"rv_a": {"rv_a"}}[role], nil
}

func (fakeOwnerChecker) Relation(_ context.Context, _, _ string) (objectowner.Relation, error) {
	return objectowner.Relation{}, nil
}

// TestRefuseOwnerTransfers_HappyPath pins that an ownership statement is
// judged with the statements before it: a GRANT of the owner to the
// connection, and a chain through a role the plan creates.
func TestRefuseOwnerTransfers_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
	}{
		{name: "a grant of the owner", statements: []string{"GRANT rv_a TO app", `ALTER VIEW "v" OWNER TO "rv_a"`}},
		{name: "a chain through a role the plan creates",
			statements: []string{"CREATE USER fresh", "GRANT fresh TO app", "GRANT rv_a TO fresh", `ALTER VIEW "v" OWNER TO "rv_a"`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := refuseOwnerTransfers(c.Context(), "postgres", fakeOwnerChecker{}, plannedOwners("postgres", test.statements))

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestRefuseOwnerTransfers_FailurePath pins that a statement after the
// ownership statement does not count, and that a REVOKE between the grant and
// the ownership statement takes the grant away.
func TestRefuseOwnerTransfers_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
	}{
		{name: "a grant after the statement", statements: []string{`ALTER VIEW "v" OWNER TO "rv_a"`, "GRANT rv_a TO app"}},
		{name: "a revoke after the grant", statements: []string{"GRANT rv_a TO app", "REVOKE rv_a FROM app", `ALTER VIEW "v" OWNER TO "rv_a"`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := refuseOwnerTransfers(c.Context(), "postgres", fakeOwnerChecker{}, plannedOwners("postgres", test.statements))

			var refusal *OwnerTransferError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Role, qt.Equals, "rv_a")
		})
	}
}
