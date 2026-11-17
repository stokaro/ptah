package objectowner_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/objectowner"
)

// TestReadRoleStatements_HappyPath pins what the ownership check learns from
// a statement before an ALTER ... OWNER TO: every spelling that creates a
// role, the schema parser refuses CREATE USER, CREATE GROUP and an unmodeled
// CREATE ROLE option, and every membership a GRANT of roles grants, with the
// SET option it states. A GRANT of privileges and any other statement grant
// nothing.
func TestReadRoleStatements_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect, text string
		want                []objectowner.RoleStatement
	}{
		{name: "CREATE ROLE", dialect: "postgres", text: "CREATE ROLE Reporting", want: []objectowner.RoleStatement{{Text: "CREATE ROLE Reporting", Creates: "reporting"}}},
		{name: "CREATE USER with options", dialect: "postgres", text: "CREATE USER \"Report Owner\" WITH LOGIN PASSWORD 'p;w'",
			want: []objectowner.RoleStatement{{Text: "CREATE USER \"Report Owner\" WITH LOGIN PASSWORD 'p;w'", Creates: "Report Owner"}}},
		{name: "CREATE GROUP", dialect: "postgres", text: "CREATE GROUP auditors;", want: []objectowner.RoleStatement{{Text: "CREATE GROUP auditors", Creates: "auditors"}}},
		{name: "CREATE ROLE with an option the parser refuses", dialect: "postgres", text: "CREATE ROLE r CONNECTION LIMIT 3 IN ROLE g",
			want: []objectowner.RoleStatement{{Text: "CREATE ROLE r CONNECTION LIMIT 3 IN ROLE g", Creates: "r"}}},
		{name: "CREATE USER IF NOT EXISTS on CockroachDB", dialect: "cockroachdb", text: `CREATE USER IF NOT EXISTS "Reporting"`,
			want: []objectowner.RoleStatement{{Text: `CREATE USER IF NOT EXISTS "Reporting"`, Creates: "reporting"}}},
		{name: "a GRANT of a role", dialect: "postgres", text: "GRANT r TO app",
			want: []objectowner.RoleStatement{{Text: "GRANT r TO app", Grants: []objectowner.Membership{{Role: "r", Member: "app"}}}}},
		{name: "a GRANT of roles to members, the last SET option kept", dialect: "postgres",
			text: `GRANT a, "B" TO app, CURRENT_USER WITH SET TRUE, INHERIT FALSE, SET FALSE GRANTED BY admin`,
			want: []objectowner.RoleStatement{{
				Text: `GRANT a, "B" TO app, CURRENT_USER WITH SET TRUE, INHERIT FALSE, SET FALSE GRANTED BY admin`,
				Grants: []objectowner.Membership{
					{Role: "a", Member: "app", Set: new(false)}, {Role: "a", Member: "", Set: new(false)},
					{Role: "B", Member: "app", Set: new(false)}, {Role: "B", Member: "", Set: new(false)},
				},
			}}},
		{name: "SET OPTION", dialect: "postgres", text: "GRANT r TO app WITH SET OPTION",
			want: []objectowner.RoleStatement{{Text: "GRANT r TO app WITH SET OPTION", Grants: []objectowner.Membership{{Role: "r", Member: "app", Set: new(true)}}}}},
		{name: "INHERIT states no SET option", dialect: "postgres", text: "GRANT r TO app WITH INHERIT TRUE",
			want: []objectowner.RoleStatement{{Text: "GRANT r TO app WITH INHERIT TRUE", Grants: []objectowner.Membership{{Role: "r", Member: "app"}}}}},
		{name: "a GRANT of privileges", dialect: "postgres", text: "GRANT SELECT ON docs TO app", want: []objectowner.RoleStatement{{Text: "GRANT SELECT ON docs TO app"}}},
		{name: "a GRANT of a privilege list", dialect: "postgres", text: "GRANT SELECT, UPDATE (body) ON docs TO app",
			want: []objectowner.RoleStatement{{Text: "GRANT SELECT, UPDATE (body) ON docs TO app"}}},
		{name: "statements in order", dialect: "postgres", text: "CREATE USER x; -- note\nGRANT x TO app; ALTER VIEW v OWNER TO x",
			want: []objectowner.RoleStatement{
				{Text: "CREATE USER x", Creates: "x"},
				{Text: "GRANT x TO app", Grants: []objectowner.Membership{{Role: "x", Member: "app"}}},
				{Text: "ALTER VIEW v OWNER TO x"},
			}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(objectowner.ReadRoleStatements(test.dialect, test.text), qt.DeepEquals, test.want)
		})
	}
}

// heldRoles answers the roles a role the server holds holds the privilege on,
// from a fixed set: rv_g holds rv_far through a membership the server holds.
func heldRoles(role string) ([]string, error) {
	held := map[string][]string{"rv_g": {"rv_g", "rv_far"}}
	return held[role], nil
}

// TestTransfers_Gains_HappyPath pins which grants before an ownership
// statement give the connection a role the server answered it lacks: a grant
// to the connection, to the session's role, or to a role it holds or an
// earlier grant gave it, and through a membership the server holds after the
// granted role. On a server that checks SET, a grant whose SET option is
// FALSE does not count, and neither does one that states none after a grant
// of the same membership stated FALSE. On a server that checks membership,
// every grant counts.
func TestTransfers_Gains_HappyPath(t *testing.T) {
	set := objectowner.Transfers{Connected: "app", Privilege: "SET", Refused: []string{"rv_a", "rv_b", "rv_far", "rv_g"}}
	member := objectowner.Transfers{Connected: "app", Privilege: "MEMBER", Refused: []string{"rv_a"}}
	tests := []struct {
		name      string
		transfers objectowner.Transfers
		role      string
		created   []string
		grants    []objectowner.Membership
		want      bool
	}{
		{name: "no grant", transfers: set, role: "rv_a"},
		{name: "a grant to the connection", transfers: set, role: "rv_a", grants: []objectowner.Membership{{Role: "rv_a", Member: "app"}}, want: true},
		{name: "a grant to the session's role", transfers: set, role: "rv_a", grants: []objectowner.Membership{{Role: "rv_a"}}, want: true},
		{name: "a grant to a role the connection holds", transfers: set, role: "rv_a", grants: []objectowner.Membership{{Role: "rv_a", Member: "held"}}, want: true},
		{name: "a grant to a role the connection does not hold", transfers: set, role: "rv_a", grants: []objectowner.Membership{{Role: "rv_a", Member: "rv_b"}}},
		{name: "a grant to a role the plan creates", transfers: set, role: "rv_a", created: []string{"fresh"},
			grants: []objectowner.Membership{{Role: "rv_a", Member: "fresh"}}},
		{name: "a chain of grants in any order", transfers: set, role: "rv_a",
			grants: []objectowner.Membership{{Role: "rv_a", Member: "rv_b"}, {Role: "rv_b", Member: "app"}}, want: true},
		{name: "a membership the server holds", transfers: set, role: "rv_far", grants: []objectowner.Membership{{Role: "rv_g", Member: "app"}}, want: true},
		{name: "another role", transfers: set, role: "rv_b", grants: []objectowner.Membership{{Role: "rv_a", Member: "app"}}},
		{name: "SET FALSE", transfers: set, role: "rv_a", grants: []objectowner.Membership{{Role: "rv_a", Member: "app", Set: new(false)}}},
		{name: "no SET option after SET FALSE", transfers: set, role: "rv_a",
			grants: []objectowner.Membership{{Role: "rv_a", Member: "app", Set: new(false)}, {Role: "rv_a", Member: "app"}}},
		{name: "SET TRUE after SET FALSE", transfers: set, role: "rv_a",
			grants: []objectowner.Membership{{Role: "rv_a", Member: "app", Set: new(false)}, {Role: "rv_a", Member: "app", Set: new(true)}}, want: true},
		{name: "SET FALSE where the server checks membership", transfers: member, role: "rv_a",
			grants: []objectowner.Membership{{Role: "rv_a", Member: "app", Set: new(false)}}, want: true},
		{name: "a server not asked", transfers: objectowner.Transfers{Refused: []string{"rv_a"}}, role: "rv_a",
			grants: []objectowner.Membership{{Role: "rv_a", Member: "app"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := test.transfers.Gains("postgres", test.role, test.created, test.grants, heldRoles)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestTransfers_Gains_FailurePath pins that a failed question about the roles
// a granted role holds is the answer's error, not a refusal or a pass.
func TestTransfers_Gains_FailurePath(t *testing.T) {
	c := qt.New(t)
	failed := errors.New("connection lost")
	transfers := objectowner.Transfers{Connected: "app", Privilege: "SET", Refused: []string{"rv_a", "rv_g"}}

	got, err := transfers.Gains("postgres", "rv_a", nil, []objectowner.Membership{{Role: "rv_g", Member: "app"}},
		func(string) ([]string, error) { return nil, failed })

	c.Assert(err, qt.ErrorIs, failed)
	c.Assert(got, qt.IsFalse)
}
