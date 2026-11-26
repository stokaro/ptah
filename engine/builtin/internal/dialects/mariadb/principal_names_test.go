package mariadb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/mariadb"
)

func TestRenderer_RefusesTrailingSpacePrincipals(t *testing.T) {
	for _, test := range []struct {
		name string
		node ast.Node
	}{
		{name: "create role", node: &ast.CreateRoleNode{Name: "reader "}},
		{name: "drop role", node: &ast.DropRoleNode{Name: "reader "}},
		{name: "grant", node: ast.NewGrantPrivilege("reader ", "TABLE", "t", []string{"SELECT"})},
		{name: "revoke", node: ast.NewRevokePrivilege("reader ", "TABLE", "t", []string{"SELECT"})},
		{name: "grant membership", node: &ast.GrantRoleMembershipNode{Role: "reader", Member: "member "}},
		{name: "revoke membership", node: &ast.RevokeRoleMembershipNode{Role: "reader ", Member: "member"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := mariadb.New().Render(test.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*principal .* has trailing ASCII spaces, which CREATE ROLE removes; .*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
