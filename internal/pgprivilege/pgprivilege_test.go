package pgprivilege_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgprivilege"
)

// TestAll pins what ALL names on each kind, measured with aclexplode after
// GRANT ALL on PostgreSQL 18. PostgreSQL 16 reports the table list without
// MAINTAIN, which is the one difference Portable exists for.
func TestAll(t *testing.T) {
	tests := []struct {
		objectType   string
		wantAll      []string
		wantPortable []string
	}{
		{
			objectType:   "table",
			wantAll:      []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"},
			wantPortable: []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"},
		},
		{objectType: "SCHEMA", wantAll: []string{"USAGE", "CREATE"}, wantPortable: []string{"USAGE", "CREATE"}},
		{objectType: "SEQUENCE", wantAll: []string{"USAGE", "SELECT", "UPDATE"}, wantPortable: []string{"USAGE", "SELECT", "UPDATE"}},
		{objectType: "PROCEDURE", wantAll: []string{"EXECUTE"}, wantPortable: []string{"EXECUTE"}},
		{
			objectType:   "TABLES",
			wantAll:      []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"},
			wantPortable: []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"},
		},
		{objectType: "TYPES", wantAll: []string{"USAGE"}, wantPortable: []string{"USAGE"}},
		{objectType: "LARGE OBJECTS", wantAll: []string{"SELECT", "UPDATE"}, wantPortable: []string{"SELECT", "UPDATE"}},
		{objectType: "DATABASE"},
	}

	for _, test := range tests {
		t.Run(test.objectType, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(pgprivilege.All(test.objectType), qt.DeepEquals, test.wantAll)
			c.Assert(pgprivilege.Portable(test.objectType), qt.DeepEquals, test.wantPortable)
		})
	}
}

// TestNamesAll pins the two spellings of ALL: the keyword, and the list the SQL
// schema reader writes for it, which names every privilege including the ones
// a release added. A list short of one of them is a list, and a kind with no
// privileges known here is never ALL.
func TestNamesAll(t *testing.T) {
	tests := []struct {
		name       string
		objectType string
		privileges []string
		want       bool
	}{
		{name: "the keyword", objectType: "TABLES", privileges: []string{"all"}, want: true},
		{name: "every privilege", objectType: "sequences", privileges: []string{"usage", "SELECT", "UPDATE"}, want: true},
		{
			name:       "every portable privilege of tables",
			objectType: "TABLES",
			privileges: []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"},
			want:       false,
		},
		{name: "one privilege short", objectType: "SCHEMAS", privileges: []string{"USAGE"}, want: false},
		{name: "a kind it does not know", objectType: "DATABASE", privileges: []string{"CONNECT"}, want: false},
		{name: "nothing", objectType: "TYPES", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgprivilege.NamesAll(test.objectType, test.privileges), qt.Equals, test.want)
		})
	}
}
