package portablegrants_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/portablegrants"
)

// TestUnqualified_HappyPath reads the database ckshadow and rewrites each
// grant on one of its objects to name the object alone. A grant on the
// database itself, and a grant on another database's object, keep their
// names.
func TestUnqualified_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		grant catalog.Grant
		want  catalog.Grant
	}{
		{
			name:  "a table grant",
			grant: catalog.Grant{Role: "ckr", Privilege: "SELECT", ObjectType: "TABLE", Schema: "ckshadow", ObjectName: "t1"},
			want:  catalog.Grant{Role: "ckr", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1"},
		},
		{
			name:  "a column grant",
			grant: catalog.Grant{Role: "ckr", Privilege: "UPDATE", ObjectType: "TABLE", Schema: "ckshadow", ObjectName: "t1", Column: "name"},
			want:  catalog.Grant{Role: "ckr", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "t1", Column: "name"},
		},
		{
			name:  "a routine grant",
			grant: catalog.Grant{Role: "ckr", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "ckshadow", ObjectName: "f1"},
			want:  catalog.Grant{Role: "ckr", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f1"},
		},
		{
			name:  "a grant on the database",
			grant: catalog.Grant{Role: "ckr", Privilege: "INSERT", ObjectType: "SCHEMA", ObjectName: "ckshadow"},
			want:  catalog.Grant{Role: "ckr", Privilege: "INSERT", ObjectType: "SCHEMA", ObjectName: "ckshadow"},
		},
		{
			name:  "a grant on another database's table",
			grant: catalog.Grant{Role: "ckr", Privilege: "SELECT", ObjectType: "TABLE", Schema: "other", ObjectName: "t1"},
			want:  catalog.Grant{Role: "ckr", Privilege: "SELECT", ObjectType: "TABLE", Schema: "other", ObjectName: "t1"},
		},
		{
			name:  "a grant on a table named for another case of the database",
			grant: catalog.Grant{Role: "ckr", Privilege: "SELECT", ObjectType: "TABLE", Schema: "CKSHADOW", ObjectName: "t1"},
			want:  catalog.Grant{Role: "ckr", Privilege: "SELECT", ObjectType: "TABLE", Schema: "CKSHADOW", ObjectName: "t1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			read := &catalog.Database{CurrentDatabase: "ckshadow", Grants: []catalog.Grant{test.grant}}

			got := portablegrants.Unqualified(read)

			c.Assert(got.Grants, qt.DeepEquals, []catalog.Grant{test.want})
			c.Assert(got.CurrentDatabase, qt.Equals, "ckshadow")
			c.Assert(read.Grants, qt.DeepEquals, []catalog.Grant{test.grant})
		})
	}
}

// TestUnqualified_HappyPathNoCurrentDatabase returns a read of no one
// database, such as a PostgreSQL read whose grants name a schema, as it is.
func TestUnqualified_HappyPathNoCurrentDatabase(t *testing.T) {
	c := qt.New(t)
	read := &catalog.Database{Grants: []catalog.Grant{{Role: "r", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "t1"}}}

	got := portablegrants.Unqualified(read)

	c.Assert(got, qt.Equals, read)
	c.Assert(portablegrants.Unqualified(nil), qt.IsNil)
}
