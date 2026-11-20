package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/builtintest"
)

func parseRoles(c *qt.C, document string) (*schemamodel.Database, error) {
	c.Helper()
	return atlashcl.ParseWithOptions(c.Context(), []byte(document), "schema.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
}

// TestParse_RoleMembershipBlock_HappyPath reads the `membership` block the
// Atlas role syntax states a membership's options in, beside `member_of`.
func TestParse_RoleMembershipBlock_HappyPath(t *testing.T) {
	t.Run("an empty member_of states that the role belongs to no role", func(t *testing.T) {
		c := qt.New(t)
		db, err := parseRoles(c, `
role "vault" {
  member_of = []
}
role "quiet" {}
`)
		c.Assert(err, qt.IsNil)
		c.Assert(db.Roles[0].MemberOfStated, qt.IsTrue)
		c.Assert(db.Roles[0].OwnsMemberships(), qt.IsTrue)
		c.Assert(db.Roles[1].MemberOfStated, qt.IsFalse)
		c.Assert(db.Roles[1].OwnsMemberships(), qt.IsFalse)
	})
	t.Run("a block states the options of its role", func(t *testing.T) {
		c := qt.New(t)
		db, err := parseRoles(c, `
role "reader" {}
role "vault" {
  membership {
    role  = role.reader
    admin = true
    inherit = false
  }
}
`)
		c.Assert(err, qt.IsNil)
		c.Assert(db.Roles[1].MemberOf, qt.DeepEquals, []string{"reader"})
		c.Assert(db.Roles[1].MemberOptions, qt.DeepEquals, map[string]schemamodel.RoleMembershipOptions{
			"reader": {Admin: new(true), Inherit: new(false)},
		})
	})
	t.Run("member_of and a block for another role are both kept", func(t *testing.T) {
		c := qt.New(t)
		db, err := parseRoles(c, `
role "reader" {}
role "writer" {}
role "vault" {
  member_of = [role.reader]
  membership {
    role = role.writer
    set  = false
  }
}
`)
		c.Assert(err, qt.IsNil)
		c.Assert(db.Roles[2].MemberOf, qt.DeepEquals, []string{"reader", "writer"})
		c.Assert(db.Roles[2].MemberOptions, qt.DeepEquals, map[string]schemamodel.RoleMembershipOptions{"writer": {Set: new(false)}})
	})
	t.Run("a block naming a role member_of lists is one membership", func(t *testing.T) {
		c := qt.New(t)
		db, err := parseRoles(c, `
role "reader" {}
role "vault" {
  member_of = [role.reader]
  membership {
    role  = role.reader
    admin = true
  }
}
`)
		c.Assert(err, qt.IsNil)
		c.Assert(db.Roles[1].MemberOf, qt.DeepEquals, []string{"reader"})
	})
	t.Run("a block that states no option is a plain membership", func(t *testing.T) {
		c := qt.New(t)
		db, err := parseRoles(c, `
role "reader" {}
role "vault" {
  membership {
    role = role.reader
  }
}
`)
		c.Assert(err, qt.IsNil)
		c.Assert(db.Roles[1].MemberOf, qt.DeepEquals, []string{"reader"})
		c.Assert(db.Roles[1].MemberOptions, qt.IsNil)
	})
}

func TestParse_RoleMembershipBlock_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "no role", body: `membership { admin = true }`,
			wantErr: `(?s).*membership block requires a role attribute.*`},
		{name: "an unknown attribute", body: `membership {
    role = role.reader
    bogus = true
  }`, wantErr: `(?s).*bogus.*`},
		{name: "an option that is not a bool", body: `membership {
    role  = role.reader
    admin = "yes"
  }`, wantErr: `(?s).*membership attribute "admin" must be a bool.*`},
		{name: "a label", body: `membership "x" {
    role = role.reader
  }`, wantErr: `(?s).*membership block takes no label.*`},
		{name: "two blocks for one role", body: `membership {
    role  = role.reader
    admin = true
  }
  membership {
    role = role.reader
    set  = false
  }`, wantErr: `(?s).*two membership blocks for role "reader".*`},
		{name: "a nested block", body: `membership {
    role = role.reader
    extra {}
  }`, wantErr: `(?s).*unsupported membership block "extra".*`},
		{name: "another block in a role", body: `grant {
    role = role.reader
  }`, wantErr: `(?s).*unsupported role block "grant".*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseRoles(c, "role \"reader\" {}\nrole \"vault\" {\n  "+test.body+"\n}\n")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
