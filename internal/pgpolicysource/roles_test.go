package pgpolicysource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
)

func TestParseRoleList_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []pgpolicy.RoleSelector
	}{
		{name: "a keyword in any case", text: "public", want: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}},
		{name: "every keyword", text: "PUBLIC, current_role,Current_User , SESSION_USER", want: []pgpolicy.RoleSelector{
			{Keyword: pgpolicy.Public}, {Keyword: pgpolicy.CurrentRole}, {Keyword: pgpolicy.CurrentUser}, {Keyword: pgpolicy.SessionUser}}},
		{name: "bare names fold to lower case", text: "app_user, Admin$1", want: []pgpolicy.RoleSelector{{Name: "app_user"}, {Name: "admin$1"}}},
		{name: "quoted names keep their bytes", text: `"Admin", "a, b", "say ""hi""", "public"`, want: []pgpolicy.RoleSelector{
			{Name: "Admin"}, {Name: "a, b"}, {Name: `say "hi"`}, {Name: "public"}}},
		{name: "a name beyond ASCII", text: "Ärger", want: []pgpolicy.RoleSelector{{Name: "Ärger"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			roles, err := pgpolicysource.ParseRoleList(test.text)

			c.Assert(err, qt.IsNil)
			c.Assert(roles, qt.DeepEquals, test.want)
		})
	}
}

func TestParseRoleList_FailurePath(t *testing.T) {
	tests := []struct {
		text string
		want string
	}{
		{text: "", want: `.*role list "" has an empty entry`},
		{text: "a,,b", want: `.*role list "a,,b" has an empty entry`},
		{text: "a,", want: `.*role list "a," has an empty entry`},
		{text: `"open`, want: `.*role "open is not one double-quoted name`},
		{text: `"a" b`, want: `.*role "a" b is not one double-quoted name`},
		{text: `"a"b"c"`, want: `.*role "a"b"c" is not one double-quoted name`},
		{text: "two words", want: `.*role "two words" must be double-quoted to be a name`},
		{text: "1st", want: `.*role "1st" must be double-quoted to be a name`},
		{text: "a-b", want: `.*role "a-b" must be double-quoted to be a name`},
	}
	for _, test := range tests {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)

			roles, err := pgpolicysource.ParseRoleList(test.text)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(roles, qt.IsNil)
		})
	}
}

// TestFormatRoleList_ReadsBack pins that a written list reads back as the same
// selectors in their canonical order, and how each one is spelled.
func TestFormatRoleList_ReadsBack(t *testing.T) {
	c := qt.New(t)
	roles := []pgpolicy.RoleSelector{{Name: "Admin"}, {Name: `say "hi"`}, {Name: "a, b"}, {Name: "reader"}, {Name: "public"},
		{Name: "current_user"}, {Name: "two words"}, {Keyword: pgpolicy.SessionUser}, {Keyword: pgpolicy.Public}}

	text := pgpolicysource.FormatRoleList(roles)

	c.Assert(text, qt.Equals, `PUBLIC, SESSION_USER, "Admin", "a, b", "current_user", "public", reader, "say ""hi""", "two words"`)
	parsed, err := pgpolicysource.ParseRoleList(text)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed, qt.DeepEquals, pgpolicy.CanonicalRoles(roles))
}
