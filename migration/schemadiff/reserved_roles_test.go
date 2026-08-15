package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/reservedrole"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// postgresInfo is the connection metadata a PostgreSQL comparison carries.
func postgresInfo() types.DBInfo {
	return types.DBInfo{Dialect: "postgres", Schema: "public"}
}

// emptyPostgresDatabase is the target the reproduction used: an empty database
// whose reader reported no role in either list, which is also what a reader
// reports for a reserved role that does exist on the server.
func emptyPostgresDatabase() *types.DBSchema {
	return &types.DBSchema{
		Roles:           []types.DBRole{{Name: "app_user", Login: true}},
		RolesOutOfScope: []types.DBRole{{Name: "other_tenant_user", Login: true}},
	}
}

// TestCompareWithDatabaseInfoRefusesAReservedRole is the regression for
// stokaro/ptah#1312 on the compare path.
//
// Reproduced before the fix with ptah-compat schema apply --auto-approve
// against PostgreSQL 17.10: a desired state declaring role "pg_monitor" printed
// `CREATE ROLE "pg_monitor" WITH NOLOGIN ...` and then exited 1 on
// `role name "pg_monitor" is reserved (SQLSTATE 42939)`; declaring role
// "postgres" printed the same shape and exited 1 on SQLSTATE 42710. Neither
// name is in DBSchema.Roles or DBSchema.RolesOutOfScope, because the reader
// excludes both from both reads, so the comparison read them as absent.
func TestCompareWithDatabaseInfoRefusesAReservedRole(t *testing.T) {

	tests := []struct {
		name    string
		roles   []goschema.Role
		wantErr string
	}{
		{
			name:    "the reserved prefix",
			roles:   []goschema.Role{{Name: "pg_monitor"}},
			wantErr: `.*declares reserved PostgreSQL role "pg_monitor".*SQLSTATE 42939.*`,
		},
		{
			name:    "the bootstrap superuser",
			roles:   []goschema.Role{{Name: "postgres", Login: true, Superuser: true}},
			wantErr: `.*declares reserved PostgreSQL role "postgres".*SQLSTATE 42710.*`,
		},
		{
			name: "a reserved role alongside ordinary ones",
			roles: []goschema.Role{
				{Name: "app_user", Login: true},
				{Name: "pg_monitor"},
			},
			wantErr: `.*declares reserved PostgreSQL role "pg_monitor".*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, err := schemadiff.CompareWithDatabaseInfo(
				&goschema.Database{Roles: test.roles},
				emptyPostgresDatabase(),
				postgresInfo(),
				nil,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfoStillComparesAnOrdinaryRole is the control. The
// refusal must not cost the comparison a role it was always right to plan, and
// a name that merely starts like a reserved one is an ordinary role
// (stokaro/ptah#1291).
func TestCompareWithDatabaseInfoStillComparesAnOrdinaryRole(t *testing.T) {

	tests := []struct {
		name       string
		roles      []goschema.Role
		wantAdded  []string
		wantSynced bool
	}{
		{
			name:      "a role the database does not have is still added",
			roles:     []goschema.Role{{Name: "brand_new", Login: true}},
			wantAdded: []string{"brand_new"},
		},
		{
			name:      "pgbouncer is an ordinary role, not a reserved one",
			roles:     []goschema.Role{{Name: "pgbouncer", Login: true}},
			wantAdded: []string{"pgbouncer"},
		},
		{
			name:      "postgres_admin is an ordinary role, not the superuser",
			roles:     []goschema.Role{{Name: "postgres_admin", Login: true}},
			wantAdded: []string{"postgres_admin"},
		},
		{
			name:       "a role the database already has is not added",
			roles:      []goschema.Role{{Name: "app_user", Login: true}},
			wantAdded:  nil,
			wantSynced: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, err := schemadiff.CompareWithDatabaseInfo(
				&goschema.Database{Roles: test.roles},
				emptyPostgresDatabase(),
				postgresInfo(),
				nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.RolesAdded, qt.DeepEquals, test.wantAdded)
			c.Assert(diff.RolesModified, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDatabaseInfoOptInPlansTheReservedRoleAnyway pins the
// capability the refusal would otherwise remove. Measured on PostgreSQL 17.10:
// against a cluster bootstrapped as "admin" rather than "postgres",
// CREATE ROLE "postgres" succeeds, so declaring it is something a user could do
// before the refusal existed and stays reachable behind the variable.
func TestCompareWithDatabaseInfoOptInPlansTheReservedRoleAnyway(t *testing.T) {
	c := qt.New(t)

	c.Setenv(reservedrole.AllowEnvVar, "1")

	diff, err := schemadiff.CompareWithDatabaseInfo(
		&goschema.Database{Roles: []goschema.Role{{Name: "postgres"}, {Name: "pg_monitor"}}},
		emptyPostgresDatabase(),
		postgresInfo(),
		nil,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.RolesAdded, qt.DeepEquals, []string{"pg_monitor", "postgres"})
}
