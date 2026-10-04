package schemasecurity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemasecurity"
)

// TestROL01_NamesAGrantOnTheDatabase names a privilege on the database itself,
// YDB's database root, by the path the read found it at: a grant with no table,
// schema or sequence is still a privilege a role holds.
func TestROL01_NamesAGrantOnTheDatabase(t *testing.T) {
	tests := []struct {
		name         string
		databasePath string
		wantName     string
	}{
		{name: "a read names the database by its path", databasePath: "/local", wantName: "/local"},
		{name: "a declaration names it by its kind", wantName: "database"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Grants:       []schemamodel.Grant{{Role: "app", Privileges: []string{"ydb.database.connect"}, OnDatabase: true}},
				DatabasePath: test.databasePath,
			}

			report := schemasecurity.Analyze(db, schemasecurity.Options{RoleObjectUsage: make([]schemasecurity.RoleObjectUsage, 0)})

			c.Assert(report.Findings, qt.HasLen, 1)
			c.Assert(report.Findings[0].Code, qt.Equals, "ROL01")
			c.Assert(report.Findings[0].Subject, qt.Equals, schemasecurity.Subject{Kind: "database", Name: test.wantName})
		})
	}
}
