package privilegerender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
)

func TestCapabilityRefusalNamesEveryOperation(t *testing.T) {
	for _, action := range []pgdefaultprivilege.Action{pgdefaultprivilege.GrantRights, pgdefaultprivilege.RevokeRights, pgdefaultprivilege.RevokeGrantOptions} {
		t.Run(string(action), func(t *testing.T) {
			c := qt.New(t)
			operation := &pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader"}, Action: action, Privileges: []string{"SELECT"}}
			sql, err := builtin.RenderSQLWithCapabilities("spanner", capability.SpannerPostgres(), &ast.ExtensionStatement{Payload: operation})
			c.Assert(err, qt.IsNil)
			c.Assert(strings.TrimSpace(sql), qt.Equals, "-- SPANNER: default privilege app_owner on TABLES in schema app to app_reader is not supported by this target; skipped.")
		})
	}
}

// Invalid operands must fail before a capability skip can hide them.
func TestInvalidOperationPrecedesCapabilityRefusal(t *testing.T) {
	for _, action := range []pgdefaultprivilege.Action{pgdefaultprivilege.GrantRights, pgdefaultprivilege.RevokeRights, pgdefaultprivilege.RevokeGrantOptions} {
		for _, test := range []struct {
			name       string
			identity   pgdefaultprivilege.Identity
			privileges []string
			reason     string
		}{
			{name: "no privileges", identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, reason: "privilege"},
			{name: "no grantor", identity: pgdefaultprivilege.Identity{Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, privileges: []string{"SELECT"}, reason: "grantor"},
			{name: "no class", identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", Grantee: "reader"}, privileges: []string{"SELECT"}, reason: "object type"},
			{name: "no grantee", identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES"}, privileges: []string{"SELECT"}, reason: "grantee"},
		} {
			t.Run(string(action)+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				operation := &pgdefaultprivilege.Operation{Identity: test.identity, Action: action, Privileges: test.privileges}
				sql, err := builtin.RenderSQLWithCapabilities("spanner", capability.SpannerPostgres(), &ast.ExtensionStatement{Payload: operation})
				c.Assert(err, qt.ErrorMatches, "(?s).*"+test.reason+".*")
				c.Assert(sql, qt.Equals, "")
			})
		}
	}
}

func TestCapabilitySkipKeepsNamesInsideItsComment(t *testing.T) {
	c := qt.New(t)
	operation := &pgdefaultprivilege.Operation{Identity: pgdefaultprivilege.Identity{Grantor: "owner\nDROP TABLE users", ObjectType: "TABLES", Grantee: "reader"}, Action: pgdefaultprivilege.GrantRights, Privileges: []string{"SELECT"}}
	sql, err := builtin.RenderSQLWithCapabilities("spanner", capability.SpannerPostgres(), &ast.ExtensionStatement{Payload: operation})
	c.Assert(err, qt.IsNil)
	c.Assert(strings.TrimSpace(sql), qt.Equals, "-- SPANNER: default privilege owner DROP TABLE users on TABLES globally to reader is not supported by this target; skipped.")
}
