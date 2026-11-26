package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin/internal/dialects/postgres"
)

func TestRenderer_KeepsPrincipalNames(t *testing.T) {
	for _, role := range []string{" reader", "reader ", "\treader", " public", "PUBLIC "} {
		t.Run(role, func(t *testing.T) {
			c := qt.New(t)
			grant, grantErr := postgres.New().Render(ast.NewGrantPrivilege(role, "TABLE", "t", []string{"SELECT"}))
			revoke, revokeErr := postgres.New().Render(ast.NewRevokePrivilege(role, "TABLE", "t", []string{"SELECT"}))

			c.Assert(grantErr, qt.IsNil)
			c.Assert(revokeErr, qt.IsNil)
			c.Assert(grant, qt.Equals, "GRANT SELECT ON TABLE \"t\" TO \""+role+"\";\n")
			c.Assert(revoke, qt.Equals, "REVOKE SELECT ON TABLE \"t\" FROM \""+role+"\";\n")
		})
	}
}
