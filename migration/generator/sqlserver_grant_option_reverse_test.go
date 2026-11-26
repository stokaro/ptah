package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

// An added grant option is revoked from the grantor of the new statement.
// CurrentGrants still describes the pre-change database, where these rows
// were plain grants, and must neither hide the inverse nor select a different
// historical grantor for it.
func TestPlanBidirectionalSchemaDiff_SQLServerAddedGrantOptionRollsBack_HappyPath(t *testing.T) {
	other := sqlServerRow("G", "SELECT", "TABLE", "t", "")
	other.GrantedBy = "other"
	for _, test := range []struct {
		name     string
		grant    schemamodel.Grant
		held     []catalog.Grant
		up, down string
	}{
		{
			name:  "an object grant",
			grant: schemamodel.Grant{Role: "r", Privileges: []string{"SELECT"}, OnTable: "dbo.t", WithOption: true},
			held:  []catalog.Grant{sqlServerRow("G", "SELECT", "TABLE", "t", "")},
			up:    "GRANT SELECT ON [dbo].[t] TO [r] WITH GRANT OPTION",
			down:  "REVOKE GRANT OPTION FOR SELECT ON [dbo].[t] FROM [r] CASCADE",
		},
		{
			name:  "a column grant",
			grant: schemamodel.Grant{Role: "r", Privileges: []string{"SELECT"}, OnTable: "dbo.t", Columns: []string{"a"}, WithOption: true},
			held:  []catalog.Grant{sqlServerRow("G", "SELECT", "TABLE", "t", "a")},
			up:    "GRANT SELECT ([a]) ON [dbo].[t] TO [r] WITH GRANT OPTION",
			down:  "REVOKE GRANT OPTION FOR SELECT ([a]) ON [dbo].[t] FROM [r] CASCADE",
		},
		{
			name:  "a schema grant",
			grant: schemamodel.Grant{Role: "r", Privileges: []string{"SELECT"}, OnSchema: "dbo", WithOption: true},
			held:  []catalog.Grant{sqlServerRow("G", "SELECT", "SCHEMA", "dbo", "")},
			up:    "GRANT SELECT ON SCHEMA::[dbo] TO [r] WITH GRANT OPTION",
			down:  "REVOKE GRANT OPTION FOR SELECT ON SCHEMA::[dbo] FROM [r] CASCADE",
		},
		{
			name:  "another grantor's plain row stays",
			grant: schemamodel.Grant{Role: "r", Privileges: []string{"SELECT"}, OnTable: "dbo.t", WithOption: true},
			held:  []catalog.Grant{sqlServerRow("G", "SELECT", "TABLE", "t", ""), other},
			up:    "GRANT SELECT ON [dbo].[t] TO [r] WITH GRANT OPTION",
			down:  "REVOKE GRANT OPTION FOR SELECT ON [dbo].[t] FROM [r] CASCADE",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			up, down := planSQLServerRows(c, &schemamodel.Database{Grants: []schemamodel.Grant{test.grant}}, test.held)

			c.Assert(up, qt.DeepEquals, []string{test.up})
			c.Assert(down, qt.DeepEquals, []string{test.down})
		})
	}
}
