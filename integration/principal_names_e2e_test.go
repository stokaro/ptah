//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
)

// A grant held by a padded role cannot satisfy a grant declared for its
// unpadded namesake. Applying that grant and then revoking the padded role's
// grant must change only the named principal on the server.
func TestSchemaApplyPostgresKeepsPrincipalNamesE2E(t *testing.T) {
	for _, test := range []struct{ name, prefix, suffix string }{
		{name: "leading space", prefix: " "},
		{name: "trailing space", suffix: " "},
		{name: "leading tab", prefix: "\t"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			roles := newConnectingRoles(c)
			padded := test.prefix + roles.grantee + test.suffix
			quoted := sqlident.Quote("postgres", padded)
			admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoted)
				c.Check(dropErr, qt.IsNil)
				dbschema.CloseAndWarn(admin)
			})
			_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+quoted+" NOLOGIN")
			c.Assert(err, qt.IsNil)
			const table = "CREATE TABLE t (id integer PRIMARY KEY);"
			target := scratchOwnedDatabase(c, roles, table+" GRANT SELECT ON t TO "+quoted+";")
			conn, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			read := runPtahIn(c, "db", "read", "--db-url", target)
			c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("%s", read.Stderr))
			c.Assert(read.Stdout, qt.Contains, " TO "+quoted+";")
			declaration := table + fmt.Sprintf(" GRANT SELECT ON t TO %s;", sqlident.Quote("postgres", roles.grantee))

			applied := applyReadBack(c, target, declaration, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s", applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "GRANT SELECT")
			var plainHasSelect, paddedHasSelect bool
			err = conn.QueryRowContext(c.Context(), "SELECT has_table_privilege($1, 't', 'SELECT'), has_table_privilege($2, 't', 'SELECT')",
				roles.grantee, padded).Scan(&plainHasSelect, &paddedHasSelect)
			c.Assert(err, qt.IsNil)
			c.Assert([]bool{plainHasSelect, paddedHasSelect}, qt.DeepEquals, []bool{true, true})

			revoked := applyReadBack(c, target, declaration+" REVOKE SELECT ON t FROM "+quoted+";", "--auto-approve")

			c.Assert(revoked.ExitCode, qt.Equals, 0, qt.Commentf("%s", revoked.Stderr))
			c.Assert(revoked.Stdout, qt.Contains, " FROM "+quoted+";")
			err = conn.QueryRowContext(c.Context(), "SELECT has_table_privilege($1, 't', 'SELECT'), has_table_privilege($2, 't', 'SELECT')",
				roles.grantee, padded).Scan(&plainHasSelect, &paddedHasSelect)
			c.Assert(err, qt.IsNil)
			c.Assert([]bool{plainHasSelect, paddedHasSelect}, qt.DeepEquals, []bool{true, false})
			synced := applyReadBack(c, target, declaration+" REVOKE SELECT ON t FROM "+quoted+";", "--dry-run")
			c.Assert(synced.ExitCode, qt.Equals, 0, qt.Commentf("%s", synced.Stderr))
			c.Assert(synced.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

func TestSchemaApplyMariaDBKeepsPrincipalNamesE2E(t *testing.T) {
	for _, test := range []struct{ name, prefix, suffix string }{
		{name: "leading space", prefix: " "},
		{name: "trailing tab", suffix: "\t"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			const table = "CREATE TABLE t1 (id INT PRIMARY KEY);\n"
			run := newColumnGrantRunFrom(c, dbtarget.MariaDBAdmin, table, nil)
			padded := test.prefix + run.role + test.suffix
			quoted := sqlident.Quote("mariadb", padded)
			_, err := run.scratch.admin.ExecContext(c.Context(), "CREATE ROLE "+quoted)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				_, dropErr := run.scratch.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoted)
				c.Check(dropErr, qt.IsNil)
			})
			_, err = run.scratch.admin.ExecContext(c.Context(), "GRANT SELECT ON "+sqlident.Quote("mariadb", run.database)+".t1 TO "+quoted)
			c.Assert(err, qt.IsNil)
			paddedRun := run
			paddedRun.role = padded
			read := runPtahIn(c, "db", "read", "--db-url", run.url)
			c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("%s", read.Stderr))
			c.Assert(read.Stdout, qt.Contains, " TO "+quoted+";")
			declaration := table + "GRANT SELECT ON t1 TO " + sqlident.Quote("mariadb", run.role) + ";\n"

			applied := applyReadBack(c, run.url, declaration, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s", applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "GRANT SELECT")
			c.Assert(run.privileges(c), qt.DeepEquals, []string{"table t1 SELECT NO"})
			c.Assert(paddedRun.privileges(c), qt.DeepEquals, []string{"table t1 SELECT NO"})

			revoked := applyReadBack(c, run.url, declaration+"REVOKE SELECT ON t1 FROM "+quoted+";\n", "--auto-approve")

			c.Assert(revoked.ExitCode, qt.Equals, 0, qt.Commentf("%s", revoked.Stderr))
			c.Assert(revoked.Stdout, qt.Contains, " FROM "+quoted+";")
			c.Assert(run.privileges(c), qt.DeepEquals, []string{"table t1 SELECT NO"})
			c.Assert(paddedRun.privileges(c), qt.IsNil)
			synced := applyReadBack(c, run.url, declaration+"REVOKE SELECT ON t1 FROM "+quoted+";\n", "--dry-run")
			c.Assert(synced.ExitCode, qt.Equals, 0, qt.Commentf("%s", synced.Stderr))
			c.Assert(synced.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

func TestSchemaApplyMySQLFamilyRefusesUnsupportedPrincipalSpellingsE2E(t *testing.T) {
	for _, engine := range []struct {
		name, prefix, suffix, refusal string
		admin                         dbtarget.Engine
	}{
		{name: "mysql", admin: dbtarget.MySQLAdmin, prefix: " ", refusal: "has surrounding white space, which Ptah does not support for MySQL"},
		{name: "mariadb", admin: dbtarget.MariaDBAdmin, suffix: " ", refusal: "has trailing ASCII spaces, which CREATE ROLE removes"},
	} {
		for _, test := range []struct{ name, statement string }{
			{name: "role", statement: "CREATE ROLE %s;"},
			{name: "grant", statement: "GRANT UPDATE ON t1 TO %s;"},
			{name: "revoke", statement: "REVOKE SELECT ON t1 FROM %s;"},
		} {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				const table = "CREATE TABLE t1 (id INT PRIMARY KEY);\n"
				run := newColumnGrantRunFrom(c, engine.admin, table, []string{"GRANT SELECT ON `%[1]s`.t1 TO %[2]s"})
				declared := table + fmt.Sprintf(test.statement, sqlident.Quote(engine.name, engine.prefix+run.role+engine.suffix))

				applied := applyReadBack(c, run.url, declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", applied.Stdout))
				c.Assert(applied.Stderr, qt.Contains, engine.refusal)
				c.Assert(run.privileges(c), qt.DeepEquals, []string{"table t1 SELECT NO"})
			})
		}
	}
}
