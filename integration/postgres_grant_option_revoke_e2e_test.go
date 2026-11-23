//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// postgresGrantOptionRole creates a role, server-wide, that is dropped when
// the test ends. It is created before the database the test grants in, so
// that database is dropped first, with the privileges the role holds there.
func postgresGrantOptionRole(c *qt.C) string {
	c.Helper()
	admin := connectDevDialect(c, dbtarget.URL(c, dbtarget.PostgreSQL))
	role := fmt.Sprintf("go_r_%d", time.Now().UnixNano()%1_000_000_000)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	_, err := admin.ExecContext(c.Context(), "CREATE ROLE "+role)
	c.Assert(err, qt.IsNil)
	return role
}

// applyPostgresGrantOptionDocument runs `ptah schema apply` on url with a SQL
// schema file holding the table t48 and statements, where {role} stands for
// role, and the given arguments.
func applyPostgresGrantOptionDocument(c *qt.C, url, role, statements string, args ...string) (string, error) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	document := "CREATE TABLE t48 (id integer PRIMARY KEY);\n" + strings.ReplaceAll(statements, "{role}", role)
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return runPtahNativeWithError(append([]string{"schema", "apply", "--db-url", url, "--schema-file", path}, args...)...)
}

// selectOnT48 is what role holds on t48: `SELECT <held> OPTION <held>`.
func selectOnT48(c *qt.C, dev devDialectDatabase, role string) string {
	c.Helper()
	var held, option bool
	c.Assert(dev.conn.QueryRowContext(c.Context(),
		"SELECT has_table_privilege($1, 't48', 'SELECT'), has_table_privilege($1, 't48', 'SELECT WITH GRANT OPTION')",
		role).Scan(&held, &option), qt.IsNil)
	return fmt.Sprintf("SELECT %t OPTION %t", held, option)
}

// A declared REVOKE GRANT OPTION FOR takes the grant option the role holds and
// leaves the privilege, whether or not the schema manages the role. A role the
// schema does not manage keeps an option a GRANT without WITH GRANT OPTION
// alone leaves out, so for such a role the revoke is the declaration that
// takes it. The option is read back from the server, and the next plan is
// empty.
func TestSchemaApplyPostgresTakesADeclaredGrantOptionE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		roles string
	}{
		{name: "unmanaged role", roles: ""},
		{name: "managed role", roles: "CREATE ROLE {role};\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			role := postgresGrantOptionRole(c)
			dev := postgresUnloggedDatabase(c)
			created, err := applyPostgresGrantOptionDocument(c, dev.url, role, test.roles, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", created))
			dev.exec(c, []string{"GRANT SELECT ON t48 TO " + role + " WITH GRANT OPTION"})
			c.Assert(selectOnT48(c, dev, role), qt.Equals, "SELECT true OPTION true")
			statements := test.roles + "GRANT SELECT ON t48 TO {role} WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR SELECT ON t48 FROM {role};\n"

			applied, err := applyPostgresGrantOptionDocument(c, dev.url, role, statements, "--auto-approve")
			settled, settledErr := applyPostgresGrantOptionDocument(c, dev.url, role, statements, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(applied, qt.Contains, "REVOKE GRANT OPTION FOR SELECT ON")
			c.Assert(selectOnT48(c, dev, role), qt.Equals, "SELECT true OPTION false")
			c.Assert(settledErr, qt.IsNil, qt.Commentf("%s", settled))
			c.Assert(settled, qt.Contains, "Schema is synced")
		})
	}
}

// The control for the declared revoke: a GRANT without WITH GRANT OPTION alone
// says nothing about the option of a role the schema does not manage, so the
// role keeps it and the plan is empty.
func TestSchemaApplyPostgresLeavesTheGrantOptionOfAnUnmanagedRoleE2E(t *testing.T) {
	c := qt.New(t)
	role := postgresGrantOptionRole(c)
	dev := postgresUnloggedDatabase(c)
	created, err := applyPostgresGrantOptionDocument(c, dev.url, role, "", "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", created))
	dev.exec(c, []string{"GRANT SELECT ON t48 TO " + role + " WITH GRANT OPTION"})

	settled, err := applyPostgresGrantOptionDocument(c, dev.url, role, "GRANT SELECT ON t48 TO {role};\n", "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", settled))
	c.Assert(settled, qt.Contains, "Schema is synced")
	c.Assert(selectOnT48(c, dev, role), qt.Equals, "SELECT true OPTION true")
}

// grantableOnT48 is every privilege role holds on t48 with the grant option,
// in order, read from the table's access list.
func grantableOnT48(c *qt.C, dev devDialectDatabase, role string) []string {
	c.Helper()
	rows, err := dev.conn.QueryContext(c.Context(), `
		SELECT a.privilege_type FROM pg_class t, aclexplode(t.relacl) a
		WHERE t.oid = 't48'::regclass AND a.grantee = $1::regrole AND a.is_grantable
		ORDER BY 1`, role)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var grantable []string
	for rows.Next() {
		var privilege string
		c.Assert(rows.Scan(&privilege), qt.IsNil)
		grantable = append(grantable, privilege)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return grantable
}

// A REVOKE GRANT OPTION FOR ALL takes the option from each privilege ALL
// grants, MAINTAIN included, which PostgreSQL 17 added and which a comparison
// does not require for ALL to be held. Read back from the table's access list,
// the role holds nothing grantable, and the next plan is empty.
func TestSchemaApplyPostgresTakesTheGrantOptionForAllE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		roles string
	}{
		{name: "unmanaged role", roles: ""},
		{name: "managed role", roles: "CREATE ROLE {role};\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			role := postgresGrantOptionRole(c)
			dev := postgresUnloggedDatabase(c)
			created, err := applyPostgresGrantOptionDocument(c, dev.url, role, test.roles, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", created))
			dev.exec(c, []string{"GRANT ALL ON t48 TO " + role + " WITH GRANT OPTION"})
			c.Assert(grantableOnT48(c, dev, role), qt.DeepEquals,
				[]string{"DELETE", "INSERT", "MAINTAIN", "REFERENCES", "SELECT", "TRIGGER", "TRUNCATE", "UPDATE"})
			statements := test.roles + "GRANT ALL ON t48 TO {role} WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR ALL ON t48 FROM {role};\n"

			applied, err := applyPostgresGrantOptionDocument(c, dev.url, role, statements, "--auto-approve")
			settled, settledErr := applyPostgresGrantOptionDocument(c, dev.url, role, statements, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(applied, qt.Contains, "REVOKE GRANT OPTION FOR MAINTAIN ON")
			c.Assert(grantableOnT48(c, dev, role), qt.IsNil)
			c.Assert(settledErr, qt.IsNil, qt.Commentf("%s", settled))
			c.Assert(settled, qt.Contains, "Schema is synced")
		})
	}
}
