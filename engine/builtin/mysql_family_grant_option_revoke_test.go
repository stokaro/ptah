package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin"
)

// A revoke of the grant option alone keeps the privilege on MySQL and MariaDB.
// Both engines spell it `REVOKE GRANT OPTION ON ... FROM ...` and name no
// privilege, because the option belongs to the grantee at one object. Measured on
// MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3: the role keeps SELECT
// and loses only the option, where `REVOKE SELECT` takes SELECT away and
// `REVOKE GRANT OPTION FOR SELECT` is error 1064. Without the GRANT OPTION form,
// a plan that only takes the option away renders the plain REVOKE.
func TestRenderSQL_MySQLFamilyRevokesOnlyTheGrantOption(t *testing.T) {
	tests := []struct {
		name string
		node *ast.RevokePrivilegeNode
		want string
	}{
		{
			name: "the grant option on a table",
			node: ast.NewRevokePrivilege("reader", "TABLE", "app.orders", []string{"SELECT"}).SetGrantOptionFor(true),
			want: "REVOKE GRANT OPTION ON `app`.`orders` FROM `reader`;\n",
		},
		{
			name: "the grant option on a schema",
			node: ast.NewRevokePrivilege("reader", "SCHEMA", "app", []string{"SELECT"}).SetGrantOptionFor(true),
			want: "REVOKE GRANT OPTION ON `app`.* FROM `reader`;\n",
		},
		{
			name: "the grant option of two privileges is one statement",
			node: ast.NewRevokePrivilege("reader", "TABLE", "orders", []string{"SELECT", "INSERT"}).SetGrantOptionFor(true),
			want: "REVOKE GRANT OPTION ON `orders` FROM `reader`;\n",
		},
		{
			name: "the privilege itself",
			node: ast.NewRevokePrivilege("reader", "TABLE", "app.orders", []string{"SELECT"}),
			want: "REVOKE SELECT ON `app`.`orders` FROM `reader`;\n",
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				got, err := builtin.RenderSQL(dialect, test.node)

				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.Equals, test.want)
			})
		}
	}
}
