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
		{name: "PostgreSQL takes a plain role and grant", dialect: "postgres", caps: capability.Postgres17(),
			database: &schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: "app_user", Login: true, Superuser: true}},
				Grants: []schemamodel.Grant{{Role: "app_user", Privileges: []string{"USAGE"}, OnSchema: "public"}},
			}},
		{name: "PostgreSQL 16 takes every membership option", dialect: "postgres", caps: capability.Postgres17(),
			database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", MemberOf: []string{"readers"},
				MemberOptions: map[string]schemamodel.RoleMembershipOptions{"readers": {Admin: new(true), Inherit: new(false), Set: new(true)}}}}}},
		{name: "PostgreSQL 14 takes the admin option", dialect: "postgres", caps: capability.Postgres14(),
			database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", MemberOf: []string{"readers"},
				MemberOptions: map[string]schemamodel.RoleMembershipOptions{"readers": {Admin: new(true)}}}}}},
		{name: "MySQL and ClickHouse take the admin option", dialect: "mysql", caps: capability.MySQL84(),
			database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", MemberOf: []string{"readers"},
				MemberOptions: map[string]schemamodel.RoleMembershipOptions{"readers": {Admin: new(true)}}}}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(accessscope.ValidateDeclared(test.dialect, test.caps, test.database), qt.IsNil)
		})
	}
}

// TestValidateDeclared_FailurePath pins that each declaration is refused by
// the key it needs, naming the first offender by name.
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
		{name: "a membership on MariaDB", dialect: "mariadb", caps: capability.MariaDB1011(),
			database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", MemberOf: []string{"readers"}}}},
			wantKey:  capability.RoleMembership,
			wantErr:  `the schema declares a membership of role "app", which requires target capability role_membership, .*`},
		{name: "an admin option on a target without it", dialect: "postgres",
			caps: capability.Postgres17().With(capability.RoleMembershipAdmin, false),
			database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", MemberOf: []string{"readers"},
				MemberOptions: map[string]schemamodel.RoleMembershipOptions{"readers": {Admin: new(false)}}}}},
			wantKey: capability.RoleMembershipAdmin,
			wantErr: `the schema states the admin option of its membership in a role: member "app", which requires target capability role_membership_admin, .*`},
		{name: "an inherit option before PostgreSQL 16", dialect: "postgres", caps: capability.Postgres14(),
			database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", MemberOf: []string{"readers"},
				MemberOptions: map[string]schemamodel.RoleMembershipOptions{"readers": {Inherit: new(true)}}}}},
			wantKey: capability.RoleMembershipInheritSet,
			wantErr: `the schema states the inherit or set option of its membership in a role: member "app", which requires target capability role_membership_inherit_set, .*`},
		{name: "a set option on MySQL", dialect: "mysql", caps: capability.MySQL84(),
			database: &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", MemberOf: []string{"readers"},
				MemberOptions: map[string]schemamodel.RoleMembershipOptions{"readers": {Set: new(false)}}}}},
			wantKey: capability.RoleMembershipInheritSet,
			wantErr: `the schema states the inherit or set option of its membership in a role: member "app", which requires target capability role_membership_inherit_set, .*`},
		{name: "a revoke on the database on MySQL", dialect: "mysql", caps: capability.MySQL84(),
			database: &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnDatabase: true}}},
			wantKey:  capability.DatabaseGrants,
			wantErr:  `the schema grants or revokes a privilege on the database to "app", which requires target capability database_grants, .*`},
		{name: "a grant on a topic on PostgreSQL", dialect: "postgres", caps: capability.Postgres17(),
			database: &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnObject: "app.events", ObjectKind: "TOPIC"}}},
			wantKey:  capability.Topics,
			wantErr:  `the schema grants or revokes a privilege on topic "app.events" to "app", which requires target capability topics, unavailable on this postgres target`},
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

// TestValidateDeclared_RefusesAnObjectKindNoTargetHolds pins that a grant on
// a kind of object no target has is refused on every target rather than
// rendered as a statement nothing takes.
func TestValidateDeclared_RefusesAnObjectKindNoTargetHolds(t *testing.T) {
	c := qt.New(t)
	err := accessscope.ValidateDeclared("postgres", capability.Postgres18(), &schemamodel.Database{
		Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT ROW"}, OnObject: "app.q", ObjectKind: "QUEUE"}},
	})
	c.Assert(err, qt.ErrorMatches, `unsupported feature: the schema grants or revokes a privilege on queue "app.q" to "app", `+
		`and no target holds an object of kind "QUEUE"`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}
