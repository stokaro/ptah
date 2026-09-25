//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestPostgresGrantAll_ConvergesAfterOneApply applies a schema file granting
// ALL on a table and compares again. The server reports GRANT ALL as one row
// per privilege, and how many depends on its version, so only a server can
// say whether the declared ALL is recognized in what it reports. It was not:
// the second comparison planned the GRANT again, and for a role the schema
// declares it planned a REVOKE for every row (stokaro/ptah#3579).
func TestPostgresGrantAll_ConvergesAfterOneApply(t *testing.T) {
	tests := []struct {
		name    string
		managed bool
	}{
		{name: "a role the schema does not declare"},
		{name: "a role the schema declares", managed: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
			defer cancel()
			fixture := prepareRoutineGrantFixture(c, ctx)
			desired := grantAllSchema(c, fixture, "GRANT ALL ON "+fixture.schema+".ledger TO "+fixture.role+";")
			desired.Roles = managedRole(test.managed, fixture.role)

			fixture.apply(c, ctx, desired)
			settled := fixture.compare(c, ctx, desired)

			c.Assert(settled.GrantsAdded, qt.HasLen, 0, qt.Commentf("grants added: %#v", settled.GrantsAdded))
			c.Assert(settled.GrantsRemoved, qt.HasLen, 0, qt.Commentf("grants removed: %#v", settled.GrantsRemoved))
			c.Assert(fixture.tablePrivilege(c, ctx, "TRUNCATE"), qt.IsTrue)
		})
	}
}

// TestPostgresGrantAll_RevokeAllOnACreatedTable revokes everything ALTER
// DEFAULT PRIVILEGES gives the role on a table the same plan creates. The
// revoke is planned as REVOKE ALL rather than a list that names MAINTAIN,
// which PostgreSQL 16 refuses as an unrecognized privilege.
func TestPostgresGrantAll_RevokeAllOnACreatedTable(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
	defer cancel()
	fixture := prepareRoutineGrantFixture(c, ctx)
	desired := grantAllSchema(c, fixture, "REVOKE ALL ON "+fixture.schema+".ledger FROM "+fixture.role+";")

	applied := fixture.apply(c, ctx, desired)
	settled := fixture.compare(c, ctx, desired)

	c.Assert(plannedPrivileges(applied.GrantsRemoved), qt.DeepEquals, []string{"ALL"})
	c.Assert(fixture.tablePrivilege(c, ctx, "SELECT"), qt.IsFalse)
	c.Assert(fixture.tablePrivilege(c, ctx, "INSERT"), qt.IsFalse)
	c.Assert(settled.GrantsRemoved, qt.HasLen, 0, qt.Commentf("grants removed: %#v", settled.GrantsRemoved))
}

// TestPostgresGrantAll_MaintainIsReadBack declares MAINTAIN, which
// information_schema leaves out of the privileges it reports, and compares
// again. It needs PostgreSQL 17 or later, which the suite runs.
func TestPostgresGrantAll_MaintainIsReadBack(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
	defer cancel()
	fixture := prepareRoutineGrantFixture(c, ctx)
	desired := grantAllSchema(c, fixture, "GRANT MAINTAIN ON "+fixture.schema+".ledger TO "+fixture.role+";")

	fixture.apply(c, ctx, desired)
	settled := fixture.compare(c, ctx, desired)

	c.Assert(fixture.tablePrivilege(c, ctx, "MAINTAIN"), qt.IsTrue)
	c.Assert(settled.GrantsAdded, qt.HasLen, 0, qt.Commentf("grants added: %#v", settled.GrantsAdded))
}

func grantAllSchema(c *qt.C, fixture routineGrantFixture, statement string) *schemamodel.Database {
	c.Helper()
	desired, _, err := sqlschema.Read([]byte(
		"CREATE TABLE "+fixture.schema+".ledger (id bigint PRIMARY KEY);\n"+statement), platform.Postgres)
	c.Assert(err, qt.IsNil)
	return &desired
}

// managedRole declares the fixture role when the row asks for it, so the
// comparison removes what the schema does not grant it.
func managedRole(managed bool, role string) []schemamodel.Role {
	roles := map[bool][]schemamodel.Role{true: {{Name: role, Inherit: true}}}
	return roles[managed]
}

func plannedPrivileges(refs []difftypes.GrantRef) []string {
	privileges := make([]string, 0, len(refs))
	for _, ref := range refs {
		privileges = append(privileges, ref.Privilege)
	}
	return privileges
}

func (f routineGrantFixture) tablePrivilege(c *qt.C, ctx context.Context, privilege string) bool {
	c.Helper()
	var held bool
	err := f.conn.QueryRowContext(ctx, "SELECT has_table_privilege($1, $2, $3)",
		f.role, f.schema+".ledger", privilege).Scan(&held)
	c.Assert(err, qt.IsNil)
	return held
}

// TestPostgresGrantAll_DefaultPrivilegesConverge applies ALTER DEFAULT
// PRIVILEGES ... GRANT ALL ON TABLES and compares again. The server reports the
// default ACL one row per privilege, so the declared ALL has to be recognized
// in them; it was not, and every run granted ALL and revoked each row.
func TestPostgresGrantAll_DefaultPrivilegesConverge(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
	defer cancel()
	fixture := prepareRoutineGrantFixture(c, ctx)
	var owner string
	c.Assert(fixture.conn.QueryRowContext(ctx, "SELECT current_user").Scan(&owner), qt.IsNil)
	desired, _, err := sqlschema.Read([]byte("ALTER DEFAULT PRIVILEGES FOR ROLE "+owner+" IN SCHEMA "+
		fixture.schema+" GRANT ALL ON TABLES TO "+fixture.role+";"), platform.Postgres)
	c.Assert(err, qt.IsNil)

	fixture.apply(c, ctx, &desired)
	settled := fixture.compare(c, ctx, &desired)

	c.Assert(settled.DefaultPrivilegesAdded, qt.HasLen, 0, qt.Commentf("added: %v", settled.DefaultPrivilegesAdded))
	c.Assert(settled.DefaultPrivilegesRemoved, qt.HasLen, 0, qt.Commentf("removed: %v", settled.DefaultPrivilegesRemoved))
}
