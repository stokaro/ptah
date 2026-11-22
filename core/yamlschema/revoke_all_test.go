package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
)

// TestParse_GrantBesideRevokedAll_HappyPath pins that a grant of a privilege
// beside a revoke of ALL on the same table is read rather than refused as a
// privilege both granted and revoked: the pair says the role keeps SELECT and
// nothing else on the table.
func TestParse_GrantBesideRevokedAll_HappyPath(t *testing.T) {
	c := qt.New(t)
	document := `grants:
  reader_orders:
    role: reader
    privileges: [SELECT]
    on_table: orders
revokes:
  reader_everything:
    role: reader
    privileges: [ALL]
    on_table: orders
`

	db, err := yamlschema.Parse(t.Context(), noOwners, []byte(document))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Grants, qt.DeepEquals, []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders"}})
	c.Assert(db.RevokedGrants, qt.DeepEquals, []schemamodel.Grant{{Role: "reader", Privileges: []string{"ALL"}, OnTable: "orders"}})
}
