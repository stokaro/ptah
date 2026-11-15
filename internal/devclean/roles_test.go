package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
)

// TestRefuseUnconfinedRoles_FailurePath pins the refusal on a dev server the
// run neither owns nor trusts the SQL on: a role created there would outlive
// the run, since the cleanup drops no role on such a server. The refusal names
// every role and ends with the remedies a replay's refusal of CREATE ROLE ends
// with.
func TestRefuseUnconfinedRoles_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		info    catalog.ServerInfo
		roles   []string
		wantErr string
	}{
		{"one role on a named PostgreSQL server",
			catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@roles-named-pg:5432/app_dev"},
			[]string{"rebase_admin"},
			`the desired schema declares role "rebase_admin", which the dev server does not hold, and creating it there ` +
				`cannot be confined to the dev database realm: a role belongs to the server, not to the dev database, ` +
				`and outlives the run's cleanup` + ownedServerRemedy + trustRemedy},
		{"two roles", catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@roles-named-pg-two:5432/app_dev"},
			[]string{"reader", "writer"},
			`the desired schema declares roles "reader", "writer", which the dev server does not hold, and creating them there .*`},
		{"a whole MySQL server the operator named",
			catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true, URL: "mysql://root@roles-named-mysql:3306/"},
			[]string{"app"},
			`the desired schema declares role "app", .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := devclean.RefuseUnconfinedRoles(test.info, test.roles)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, devclean.ErrUnconfinedRoles)
		})
	}
}

// TestRefuseUnconfinedRoles_HappyPath_NoRole pins that a desired state that
// declares no role the server lacks is not asked about, on any server.
func TestRefuseUnconfinedRoles_HappyPath_NoRole(t *testing.T) {
	c := qt.New(t)
	info := catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@roles-none:5432/app_dev"}
	c.Assert(devclean.RefuseUnconfinedRoles(info, nil), qt.IsNil)
}

// TestRefuseUnconfinedRoles_HappyPath_OwnedServer pins a server the run owns:
// the role is created there and the realm cleanup drops it with the run.
func TestRefuseUnconfinedRoles_HappyPath_OwnedServer(t *testing.T) {
	c := qt.New(t)
	info := catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@roles-declared:5432/app_dev"}
	declareDisposable(c, info.URL)
	c.Assert(devclean.RefuseUnconfinedRoles(info, []string{"rebase_admin"}), qt.IsNil)
}

// TestRefuseUnconfinedRoles_HappyPath_TrustedSQL pins a server whose SQL the
// run trusts: the role is created there and stays, as a trusted replay's role
// does, while the server stays one the run does not own.
func TestRefuseUnconfinedRoles_HappyPath_TrustedSQL(t *testing.T) {
	c := qt.New(t)
	info := catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@roles-trusted:5432/app_dev"}
	trustReplaySQL(c, info.URL)
	c.Assert(devclean.RefuseUnconfinedRoles(info, []string{"rebase_admin"}), qt.IsNil)
	c.Assert(devclean.DevReplayRealm(info), qt.Equals, devclean.ReplayRealmDatabase)
}
