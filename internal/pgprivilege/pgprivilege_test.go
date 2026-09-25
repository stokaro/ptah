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
