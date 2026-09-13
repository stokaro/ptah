//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// A default privilege is the one privilege object whose whole point is what the
// server does LATER, so an offline test can only compare strings about it. Each
// step below is one a server answers.

// defaultPrivilegeSchema declares a schema, the two roles the statement names,
// and one default privilege granting a plain privilege beside a grantable one.
//
// The grantable privilege is what separates a real read from an agreeable one.
// The catalog records grantability per privilege, so a reader that reported one
// flag for the object would satisfy every other assertion here while losing the
// distinction the model exists to carry.
func defaultPrivilegeSchema(schemaName, owner, reader string) *schemamodel.Database {
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: schemaName}},
		Roles: []schemamodel.Role{
			{StructName: "Owner", Name: owner},
			{StructName: "Reader", Name: reader},
		},
		DefaultPrivileges: []schemamodel.DefaultPrivilege{{
			StructName: "Access",
			Grantor:    owner,
			Schema:     schemaName,
			ObjectType: "TABLES",
			Grantee:    reader,
			Privileges: []schemamodel.PrivilegeGrant{
				{Privilege: "SELECT"},
				{Privilege: "INSERT", WithOption: true},
			},
		}},
	}
}

// TestPostgresLiveDefaultPrivilegeConverges drives declare, apply, read back and
// compare.
//
//  1. The rendered statements are ones this engine accepts. A renderer that put
//     the clauses in the wrong order fails here -- PostgreSQL takes FOR ROLE and
//     IN SCHEMA before GRANT and nowhere else -- where an offline test would
//     have compared strings and passed.
//  2. The catalog reports the object back, with the grantor the declaration
//     named. The reader projects pg_get_userbyid over pg_default_acl.defaclrole
//     and explodes the ACL with aclexplode, and a fake server answers by column
//     name, so it would agree with a projection that reports the wrong role.
//  3. Comparing the same declaration against what the server now holds finds
//     nothing to do. This is the step that catches the whole family: a dropped
//     conversion, an unfiltered projection, a grantor lost in the comparator's
//     identity or a spelling that does not fold all surface here.
func TestPostgresLiveDefaultPrivilegeConverges(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	stamp := time.Now().UnixNano()
	schemaName := fmt.Sprintf("ptah_defpriv_%d", stamp)
	owner := fmt.Sprintf("ptah_defpriv_owner_%d", stamp)
	reader := fmt.Sprintf("ptah_defpriv_reader_%d", stamp)
	defer dropDefaultPrivilegeFixture(conn, schemaName, owner, reader)

	description := defaultPrivilegeSchema(schemaName, owner, reader)

	// 1. The rendered statements are the ones the server is given.
	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	joined := strings.Join(statements, "\n")
	c.Assert(joined, qt.Contains, "ALTER DEFAULT PRIVILEGES")
	c.Assert(joined, qt.Contains, "IN SCHEMA")
	c.Assert(joined, qt.Contains, "WITH GRANT OPTION")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The catalog is asked what it holds.
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.DefaultPrivileges, qt.HasLen, 2)
	for _, row := range live.DefaultPrivileges {
		c.Assert(row.Grantor, qt.Equals, owner)
		c.Assert(row.Grantee, qt.Equals, reader)
		c.Assert(row.Schema, qt.Equals, schemaName)
		c.Assert(row.ObjectType, qt.Equals, "TABLES")
	}
	c.Assert(defaultPrivilegeGrantOption(c, live.DefaultPrivileges, "SELECT"), qt.IsFalse)
	c.Assert(defaultPrivilegeGrantOption(c, live.DefaultPrivileges, "INSERT"), qt.IsTrue)

	// 3. The convergence assertion.
	settled := schemadiff.CompareWithDialect(description, live, platform.Postgres)
	c.Assert(settled.DefaultPrivilegesAdded, qt.HasLen, 0)
	c.Assert(settled.DefaultPrivilegesRemoved, qt.HasLen, 0)
}

// TestPostgresLiveDefaultPrivilegeTakesEffectOnACreatedTable is what the object
// is FOR, and the only assertion a catalog read cannot stand in for.
//
// Every other test here measures what the server recorded. This one measures
// what the server then does with it: a table created by the grantor afterwards
// carries the privileges nobody granted on that table.
func TestPostgresLiveDefaultPrivilegeTakesEffectOnACreatedTable(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	stamp := time.Now().UnixNano()
	schemaName := fmt.Sprintf("ptah_defpriv_eff_%d", stamp)
	owner := fmt.Sprintf("ptah_defpriv_eff_owner_%d", stamp)
	reader := fmt.Sprintf("ptah_defpriv_eff_reader_%d", stamp)
	defer dropDefaultPrivilegeFixture(conn, schemaName, owner, reader)

	description := defaultPrivilegeSchema(schemaName, owner, reader)
	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// The connected account has to be a member of the grantor to create the
	// table AS that role, which is the only way the default applies.
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`GRANT %q TO CURRENT_USER`, owner))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`GRANT USAGE, CREATE ON SCHEMA %q TO %q`, schemaName, owner))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`SET ROLE %q`, owner))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q.later (id int)`, schemaName))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `RESET ROLE`)
	c.Assert(err, qt.IsNil)

	var granted bool
	err = conn.QueryRowContext(ctx,
		`SELECT has_table_privilege($1, $2, 'SELECT')`,
		reader, schemaName+".later").Scan(&granted)
	c.Assert(err, qt.IsNil)
	c.Assert(granted, qt.IsTrue,
		qt.Commentf("the reader holds SELECT on a table nobody granted on"))
}

// defaultPrivilegeGrantOption answers whether the named privilege came back
// grantable, and fails the test when the read carries no row for it.
func defaultPrivilegeGrantOption(c *qt.C, rows []catalog.DefaultPrivilege, privilege string) bool {
	for _, row := range rows {
		if row.Privilege == privilege {
			return row.WithOption
		}
	}
	c.Fatalf("the read reports no %q privilege; it holds %+v", privilege, rows)
	return false
}

// dropDefaultPrivilegeFixture removes what the test created.
//
// The default privileges go first and by hand: a role holding one cannot be
// dropped, and DROP SCHEMA CASCADE does not reach an entry in pg_default_acl.
func dropDefaultPrivilegeFixture(conn *dbschema.DatabaseConnection, schemaName, owner, reader string) {
	ctx := context.Background()
	for _, statement := range []string{
		fmt.Sprintf(`SET ROLE %q`, owner),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA %q REVOKE ALL ON TABLES FROM %q`, schemaName, reader),
		`RESET ROLE`,
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES FOR ROLE %q IN SCHEMA %q REVOKE ALL ON TABLES FROM %q`,
			owner, schemaName, reader),
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schemaName),
		fmt.Sprintf(`DROP ROLE IF EXISTS %q`, reader),
		fmt.Sprintf(`REVOKE %q FROM CURRENT_USER`, owner),
		fmt.Sprintf(`DROP ROLE IF EXISTS %q`, owner),
	} {
		_, _ = conn.ExecContext(ctx, statement)
	}
}
