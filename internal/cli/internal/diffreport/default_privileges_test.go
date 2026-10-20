package diffreport_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/cli/internal/diffreport"
)

// A default ACL's role identity and every changed right reach the CLI report.
// Grouping rights into one owner change must not hide its privilege names.
func TestCategoriesNamesDefaultPrivilegeIdentityAndRights(t *testing.T) {
	for _, test := range []struct{ grantor, grantee string }{
		{"alpha_owner", "app_reader"}, {"beta_owner", "app_reader"}, {"app_owner", "PUBLIC"},
	} {
		t.Run(test.grantor+"/"+test.grantee, func(t *testing.T) {
			c := qt.New(t)
			ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: test.grantor, Schema: "app", ObjectType: "TABLES", Grantee: test.grantee}, identifier.ForDialect("postgres")))
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ref, Value: &pgdefaultprivilege.Change{
				Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}, {Privilege: "INSERT", WithOption: true}},
				Revoked: []pgdefaultprivilege.Grant{{Privilege: "UPDATE"}}, GrantOptionsRevoked: []string{"DELETE"},
			}}}}
			categories := diffreport.Categories(diff)
			c.Assert(categories, qt.HasLen, 1)
			c.Assert(categories[0].Objects, qt.HasLen, 1)
			report := strings.Join(categories[0].Objects, "\n")
			for _, part := range []string{test.grantor, test.grantee, "app", "TABLES", "grant INSERT WITH GRANT OPTION, SELECT; revoke UPDATE; revoke grant option DELETE"} {
				c.Assert(report, qt.Contains, part)
			}
		})
	}
}
