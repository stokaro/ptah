package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devclean/replay"
)

// sqlServerDev is a SQL Server dev database the guards below judge for.
var sqlServerDev = catalog.ServerInfo{
	Dialect: platform.SQLServer,
	Schema:  "dbo",
	URL:     "sqlserver://dev@mssql:1433?database=ptah_dev",
}

// claimedDatabaseRoles is the scope of a claim that listed the dev database's
// roles and found one, whose name holds a space.
func claimedDatabaseRoles() replay.DatabaseRoleScope {
	return replay.DatabaseRoleScope{Dropped: true, Kept: []string{"Kept Role"}}
}

// TestDevReplayGuard_SQLServerDatabaseRole_HappyPath pins that a SQL Server
// rehearsal runs a database role's CREATE ROLE, and the DROP ROLE of a role
// the claim did not find, where every cleanup drops the roles the claim did
// not find. A database role lives in the database that holds it.
func TestDevReplayGuard_SQLServerDatabaseRole_HappyPath(t *testing.T) {
	statements := []string{
		`CREATE ROLE [APP]`,
		`CREATE ROLE [App Readers] AUTHORIZATION [dbo]`,
		`create role app_writers`,
		`DROP ROLE [APP]`,
		`DROP ROLE IF EXISTS [Kept Role 2]`,
	}
	for _, statement := range statements {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			guard := devclean.NewDevReplayGuard(sqlServerDev).WithDatabaseRoles(claimedDatabaseRoles)
			c.Assert(guard.ValidateStatement(statement), qt.IsNil)
		})
	}
}

// TestBaselineGuard_SQLServerDatabaseRole_HappyPath pins the same for the
// baseline a rehearsal recreates the target's state with: a role the target
// holds is created on the dev database, and dropped with the run.
func TestBaselineGuard_SQLServerDatabaseRole_HappyPath(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewBaselineGuard(sqlServerDev).WithDatabaseRoles(claimedDatabaseRoles)
	c.Assert(guard.ValidateStatement(`CREATE ROLE [APP]`), qt.IsNil)
}

// TestDevReplayGuard_SQLServerDatabaseRole_FailurePath pins the principal
// statements a SQL Server rehearsal still refuses: every one where no cleanup
// drops the run's roles, the drop of a role the claim found, a membership
// change, an application role and a user.
func TestDevReplayGuard_SQLServerDatabaseRole_FailurePath(t *testing.T) {
	unlisted := func() replay.DatabaseRoleScope { return replay.DatabaseRoleScope{} }
	tests := []struct {
		name      string
		roles     func() replay.DatabaseRoleScope
		statement string
		wantErr   string
	}{
		{"no claim at all", nil, `CREATE ROLE [APP]`,
			`sqlserver migration replay rejects CREATE DATABASE PRINCIPAL because its effects cannot be confined to the disposable database realm.*`},
		{"a claim that listed nothing", unlisted, `DROP ROLE [APP]`,
			`sqlserver migration replay rejects DROP DATABASE PRINCIPAL because its effects cannot be confined to the disposable database realm.*`},
		{"a kept role dropped", claimedDatabaseRoles, `DROP ROLE [Kept Role]`,
			`sqlserver migration replay rejects DROP DATABASE PRINCIPAL of the dev database's own role "Kept Role" because its effects cannot be confined to the disposable database realm.*`},
		{"a kept role dropped in another case", claimedDatabaseRoles, `DROP ROLE IF EXISTS [KEPT ROLE]`,
			`sqlserver migration replay rejects DROP DATABASE PRINCIPAL of the dev database's own role "Kept Role" because .*`},
		{"a member added", claimedDatabaseRoles, `ALTER ROLE [db_owner] ADD MEMBER [APP]`,
			`sqlserver migration replay rejects ALTER DATABASE PRINCIPAL because its effects cannot be confined to the disposable database realm.*`},
		{"an application role", claimedDatabaseRoles, `CREATE APPLICATION ROLE [reporting] WITH PASSWORD = 'x'`,
			`sqlserver migration replay rejects CREATE DATABASE PRINCIPAL because its effects cannot be confined to the disposable database realm.*`},
		{"a user", claimedDatabaseRoles, `CREATE USER [app_user] WITHOUT LOGIN`,
			`sqlserver migration replay rejects CREATE DATABASE PRINCIPAL because its effects cannot be confined to the disposable database realm.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			guard := devclean.NewDevReplayGuard(sqlServerDev).WithDatabaseRoles(test.roles)
			c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, test.wantErr)
		})
	}
}
