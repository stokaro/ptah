package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

func TestParseMemberOf_HappyPath(t *testing.T) {
	t.Run("plain names state no option", func(t *testing.T) {
		c := qt.New(t)
		groups, options, err := schemamodel.ParseMemberOf([]string{"readers", " writers "})
		c.Assert(err, qt.IsNil)
		c.Assert(groups, qt.DeepEquals, []string{"readers", "writers"})
		c.Assert(options, qt.IsNil)
	})
	t.Run("a bare option is true", func(t *testing.T) {
		c := qt.New(t)
		groups, options, err := schemamodel.ParseMemberOf([]string{"readers(admin)"})
		c.Assert(err, qt.IsNil)
		c.Assert(groups, qt.DeepEquals, []string{"readers"})
		c.Assert(options, qt.DeepEquals, map[string]schemamodel.RoleMembershipOptions{"readers": {Admin: new(true)}})
	})
	t.Run("every option takes a value", func(t *testing.T) {
		c := qt.New(t)
		_, options, err := schemamodel.ParseMemberOf([]string{"readers( admin = false ; INHERIT=true; set=false )", "writers"})
		c.Assert(err, qt.IsNil)
		c.Assert(options, qt.DeepEquals, map[string]schemamodel.RoleMembershipOptions{
			"readers": {Admin: new(false), Inherit: new(true), Set: new(false)},
		})
	})
	t.Run("a group written twice with the same options is one group", func(t *testing.T) {
		c := qt.New(t)
		groups, _, err := schemamodel.ParseMemberOf([]string{"readers(admin)", "readers(admin=true)"})
		c.Assert(err, qt.IsNil)
		c.Assert(groups, qt.DeepEquals, []string{"readers"})
	})
	t.Run("no entry is no groups", func(t *testing.T) {
		c := qt.New(t)
		groups, options, err := schemamodel.ParseMemberOf(nil)
		c.Assert(err, qt.IsNil)
		c.Assert(groups, qt.IsNil)
		c.Assert(options, qt.IsNil)
	})
}

func TestParseMemberOf_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		wantErr string
	}{
		{name: "an unknown option", entries: []string{"readers(superuser)"},
			wantErr: `member_of entry "readers\(superuser\)": unknown membership option "superuser", expected admin, inherit or set`},
		{name: "an option stated twice", entries: []string{"readers(admin;admin=false)"},
			wantErr: `member_of entry "readers\(admin;admin=false\)": option admin is stated twice`},
		{name: "a value that is not a boolean", entries: []string{"readers(set=maybe)"},
			wantErr: `member_of entry "readers\(set=maybe\)": option set takes true or false, got "maybe"`},
		{name: "an unclosed list", entries: []string{"readers(admin"},
			wantErr: `member_of entry "readers\(admin": the option list is not closed with \)`},
		{name: "an empty list", entries: []string{"readers()"},
			wantErr: `member_of entry "readers\(\)": the option list is empty`},
		{name: "no group name", entries: []string{"(admin)"},
			wantErr: `member_of entry "\(admin\)": the group name is missing`},
		{name: "a stray parenthesis", entries: []string{"readers)"},
			wantErr: `member_of entry "readers\)": expected a group name or group\(option;option\)`},
		{name: "one group with different options", entries: []string{"readers(admin)", "readers"},
			wantErr: `member_of states membership in "readers" twice with different options`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			groups, options, err := schemamodel.ParseMemberOf(test.entries)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(groups, qt.IsNil)
			c.Assert(options, qt.IsNil)
		})
	}
}

// TestFormatMemberOf_ReadsBackWhatItWrites is the control for the spelling:
// whatever FormatMemberOf writes, ParseMemberOf reads as it was.
func TestFormatMemberOf_ReadsBackWhatItWrites(t *testing.T) {
	c := qt.New(t)
	role := schemamodel.Role{
		Name:     "app",
		MemberOf: []string{"readers", "writers", "auditors"},
		MemberOptions: map[string]schemamodel.RoleMembershipOptions{
			"writers":  {Admin: new(true)},
			"auditors": {Admin: new(false), Inherit: new(false), Set: new(true)},
		},
	}

	entries := schemamodel.FormatMemberOf(role)
	groups, options, err := schemamodel.ParseMemberOf(entries)

	c.Assert(entries, qt.DeepEquals, []string{"readers", "writers(admin=true)", "auditors(admin=false;inherit=false;set=true)"})
	c.Assert(err, qt.IsNil)
	c.Assert(groups, qt.DeepEquals, role.MemberOf)
	c.Assert(options, qt.DeepEquals, role.MemberOptions)
}

func TestRoleMembershipOptions_CloneSharesNoPointer(t *testing.T) {
	c := qt.New(t)
	original := schemamodel.RoleMembershipOptions{Admin: new(true), Set: new(false)}

	clone := original.Clone()
	*clone.Admin = false

	c.Assert(*original.Admin, qt.IsTrue)
	c.Assert(clone.Inherit, qt.IsNil)
	c.Assert(original.Equal(clone), qt.IsFalse)
	c.Assert(original.IsZero(), qt.IsFalse)
	c.Assert(schemamodel.RoleMembershipOptions{}.IsZero(), qt.IsTrue)
	c.Assert(schemamodel.CloneMemberOptions(nil), qt.IsNil)
}
