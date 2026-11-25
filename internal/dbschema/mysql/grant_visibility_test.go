package mysql_test

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
)

// A read of mysql.user proves that roles are visible, but the privilege views
// hide other grantees unless the account holds SELECT on mysql.* or globally.
// Even separate SELECT grants on every grant table do not open those views.
func TestReader_RecordsHiddenRoleGrants(t *testing.T) {
	tests := []struct {
		name   string
		grants []string
	}{
		{name: "roles without grants", grants: []string{
			"GRANT SELECT ON mysql.user TO `app`@`%`",
			"GRANT SELECT ON mysql.role_edges TO `app`@`%`",
			"GRANT SELECT ON mysql.default_roles TO `app`@`%`",
		}},
		{name: "each grant table separately", grants: []string{
			"GRANT SELECT ON mysql.user TO `app`@`%`",
			"GRANT SELECT ON mysql.db TO `app`@`%`",
			"GRANT SELECT ON mysql.tables_priv TO `app`@`%`",
			"GRANT SELECT ON mysql.columns_priv TO `app`@`%`",
		}},
		{name: "global select revoked on mysql", grants: []string{
			"GRANT SELECT ON *.* TO `app`@`%`",
			"REVOKE SELECT ON mysql.* FROM `app`@`%`",
		}},
	}
	for _, version := range []string{dbtest.MySQLVersion, dbtest.MariaDBVersion} {
		for _, selected := range []any{"R_LC", nil} {
			for _, test := range tests {
				t.Run(version+"/"+test.name+"/"+grantVisibilityScope(selected), func(t *testing.T) {
					c := qt.New(t)
					grants := append([]string{
						"GRANT SELECT, SHOW VIEW, TRIGGER, SHOW CREATE ROUTINE ON `R_LC`.* TO `app`@`%`",
						"GRANT SHOW_ROUTINE ON *.* TO `app`@`%`",
					}, test.grants...)
					db := dbtest.Open(t, dbtest.MySQLSession(version, grants,
						foldedPrivileges(visibleCatalog{selected: selected, databases: []string{"R_LC"}, tables: []string{"items"}}.answer)))

					got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.MySQL84()).ReadSchemaContext(t.Context())

					c.Assert(err, qt.IsNil)
					c.Assert(got.NotDescribed.Objects, qt.Contains, coverage.Refused(coverage.Role))
					c.Assert(got.Roles, qt.IsNil)
					c.Assert(got.Grants, qt.IsNil)
					c.Assert(got.WideGrants, qt.IsNil)
					c.Assert(got.Tables, qt.HasLen, 1)
				})
			}
		}
	}
}

func grantVisibilityScope(selected any) string {
	return map[bool]string{true: "server", false: "database"}[selected == nil]
}

func TestReader_KeepsVisibleRoleGrants(t *testing.T) {
	tests := []struct {
		name   string
		grants []string
	}{
		{name: "mysql database", grants: []string{"GRANT SELECT ON mysql.* TO `app`@`%`"}},
		{name: "global select", grants: []string{"GRANT SELECT ON *.* TO `app`@`%`"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			grants := append([]string{
				"GRANT SELECT, SHOW VIEW, TRIGGER ON `R_LC`.* TO `app`@`%`",
				"GRANT SHOW_ROUTINE ON *.* TO `app`@`%`",
			}, test.grants...)
			session := dbtest.MySQLSession(dbtest.MySQLVersion, grants,
				wideGrantsOf([][]driver.Value{{"lc2_r", "R%", "SELECT"}}, nil,
					foldedPrivileges(visibleCatalog{selected: "R_LC", tables: []string{"items"}}.answer)))
			db := dbtest.Open(t, session)

			got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.MySQL84()).ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(got.NotDescribed.Describes(coverage.Role, "lc2_r"), qt.IsTrue)
			c.Assert(got.Roles, qt.HasLen, 1)
			c.Assert(got.Grants, qt.HasLen, 3)
			c.Assert(got.WideGrants, qt.Contains, catalog.WideGrant{
				Role: "lc2_r", Privilege: "SELECT", Grant: "SELECT ON `R%`.*", Schema: "R_LC",
			})
			c.Assert(got.WideGrantsPartial, qt.IsFalse)
		})
	}
}
