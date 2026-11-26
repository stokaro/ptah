package schemasecurity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemasecurity"
)

func TestAnalyze_KeepsPrincipalNames(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Roles:  []schemamodel.Role{{Name: " public"}},
		Grants: []schemamodel.Grant{{Role: " public", OnTable: "t", Privileges: []string{"SELECT"}}},
	}

	report := schemasecurity.Analyze(database, schemasecurity.Options{
		RoleMemberships: []schemasecurity.RoleMembership{{Role: "public", Member: "member"}},
		RoleObjectUsage: []schemasecurity.RoleObjectUsage{{Role: "public", Kind: "table", Name: "t"}},
	})

	c.Assert(report.Findings, qt.HasLen, 2)
	c.Assert(report.Findings[0].Code, qt.Equals, "ROL01")
	c.Assert(report.Findings[0].Detail.Roles, qt.DeepEquals, []string{" public"})
	c.Assert(report.Findings[1].Code, qt.Equals, "ROL04")
	c.Assert(report.Findings[1].Subject.Name, qt.Equals, " public")
	c.Assert(report.Findings[1].Detail.Privileges, qt.DeepEquals, []string{"SELECT ON table t"})
}
