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
		{name: "YDB takes a grant on a topic and a revoke on a secret", dialect: "ydb", caps: capability.YDB262(),
			database: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT ROW"}, OnObject: "app.events", ObjectKind: "TOPIC"}},
				RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT ROW"}, OnObject: "ext.pw", ObjectKind: "SECRET"}},
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
		{name: "a grant on a topic on PostgreSQL", dialect: "postgres", caps: capability.Postgres17(),
			database: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnObject: "app.events", ObjectKind: "TOPIC"}}},
			wantKey:  capability.Topics,
			wantErr:  `the schema grants or revokes a privilege on topic "app.events" to "app", which requires target capability topics, unavailable on this postgres target`},
		{name: "a revoke on an external table on YDB with external data sources off", dialect: "ydb", caps: capability.YDB262(),
			database: &schemamodel.Database{RevokedGrants: []schemamodel.Grant{
				{Role: "app", Privileges: []string{"SELECT ROW"}, OnObject: "ext.events", ObjectKind: "EXTERNAL TABLE"},
				{Role: "app", Privileges: []string{"SELECT ROW"}, OnObject: "ext.bucket", ObjectKind: "EXTERNAL DATA SOURCE"},
			}},
			wantKey: capability.ExternalDataSources,
			wantErr: `the schema grants or revokes a privilege on external data source "ext.bucket" to "app", which requires ` +
				`target capability external_data_sources, unavailable on this ydb target`},
		{name: "a grant on a secret on YDB 25.1", dialect: "ydb", caps: capability.YDB251(),
			database: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT ROW"}, OnObject: "ext.pw", ObjectKind: "SECRET"}}},
			wantKey:  capability.Secrets,
			wantErr:  `the schema grants or revokes a privilege on secret "ext.pw" to "app", which requires target capability secrets, .*`},
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

// TestValidateDeclared_RefusesAnObjectKindNoTargetHolds pins that a grant on
// a kind of object no target has is refused on every target, YDB included,
// rather than rendered as a statement nothing takes.
func TestValidateDeclared_RefusesAnObjectKindNoTargetHolds(t *testing.T) {
	c := qt.New(t)
	err := accessscope.ValidateDeclared("ydb", capability.YDB262(), &schemamodel.Database{
		Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT ROW"}, OnObject: "app.q", ObjectKind: "QUEUE"}},
	})
	c.Assert(err, qt.ErrorMatches, `unsupported feature: the schema grants or revokes a privilege on queue "app.q" to "app", `+
		`and no target holds an object of kind "QUEUE"`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}
