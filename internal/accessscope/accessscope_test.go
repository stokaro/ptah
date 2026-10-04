package accessscope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/accessscope"
)

// TestValidateDeclared_HappyPath pins that a target holding the keys takes the
// declarations they gate, and that a schema declaring none of them passes
// everywhere.
func TestValidateDeclared_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		caps     capability.Capabilities
		database *schemamodel.Database
	}{
		{name: "YDB takes a group, a membership and a database grant", dialect: "ydb", caps: capability.YDB251(),
			database: &schemamodel.Database{
				Roles: []schemamodel.Role{
					{Name: "readers", Group: true},
					{Name: "app", Login: true, MemberOf: []string{"readers"}},
				},
				Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"CONNECT"}, OnDatabase: true}},
			}},
		{name: "PostgreSQL takes a plain role and grant", dialect: "postgres", caps: capability.Postgres17(),
			database: &schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: "app_user", Login: true, Superuser: true}},
				Grants: []schemamodel.Grant{{Role: "app_user", Privileges: []string{"USAGE"}, OnSchema: "public"}},
			}},
		{name: "no database", dialect: "ydb", caps: capability.YDB262()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(accessscope.ValidateDeclared(test.dialect, test.caps, test.database), qt.IsNil)
		})
	}
}

// TestValidateDeclared_FailurePath pins that each declaration is refused by
// the key it needs, naming the first offender by name, and that a YDB
// declaration is held to what YDB can carry.
func TestValidateDeclared_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		caps     capability.Capabilities
		database *schemamodel.Database
		wantKey  capability.Capability
		wantErr  string
	}{
		{name: "a group on PostgreSQL", dialect: "postgres", caps: capability.Postgres17(),
			database: &schemamodel.Database{Roles: []schemamodel.Role{
				{Name: "zeta", Group: true}, {Name: "alpha", Group: true},
			}},
			wantKey: capability.GroupPrincipals,
			wantErr: `the schema declares group "alpha", which requires target capability group_principals, unavailable on this postgres target`},
		{name: "a membership on ClickHouse", dialect: "clickhouse", caps: capability.ClickHouse24(),
			database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", MemberOf: []string{"readers"}}}},
			wantKey:  capability.RoleMembership,
			wantErr:  `the schema declares a membership of role "app", which requires target capability role_membership, .*`},
		{name: "a revoke on the database on MySQL", dialect: "mysql", caps: capability.MySQL84(),
			database: &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnDatabase: true}}},
			wantKey:  capability.DatabaseGrants,
			wantErr:  `the schema grants or revokes a privilege on the database to "app", which requires target capability database_grants, .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := accessscope.ValidateDeclared(test.dialect, test.caps, test.database)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Feature, qt.Equals, string(test.wantKey))
		})
	}
}

// TestValidateDeclared_HoldsYDBToItsModel pins that a YDB declaration passing
// the keys still meets what YDB's users and permissions carry.
func TestValidateDeclared_HoldsYDBToItsModel(t *testing.T) {
	c := qt.New(t)
	err := accessscope.ValidateDeclared("ydb", capability.YDB262(), &schemamodel.Database{
		Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t", WithOption: true}},
	})
	c.Assert(err, qt.ErrorMatches, `unsupported feature: grant to "app" declares with_option: .*`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}
