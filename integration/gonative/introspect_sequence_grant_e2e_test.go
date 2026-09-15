//go:build integration

package gonative_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/config"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/cli/root"
	"ptah.run/migration/schemadiff"
)

// TestIntrospectCommand_PostgresSequenceGrantRoundTrips runs `ptah introspect`
// over a schema holding a grant on a standalone sequence, parses the package it
// writes, and compares that package against the database it was read from.
//
// The package has to say on_sequence. PostgreSQL accepts GRANT ... ON TABLE for
// a sequence, so an on_table annotation naming the sequence would apply without
// error, but the comparison keys a grant by its object type and the server
// reports SEQUENCE: the package would never converge with its own source. The
// table grant to the same role is the control.
func TestIntrospectCommand_PostgresSequenceGrantRoundTrips(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	suffix := time.Now().UnixNano()
	schemaName := fmt.Sprintf("ptah_introspect_seq_grant_%d", suffix)
	roleName := fmt.Sprintf("ptah_introspect_seq_grant_role_%d", suffix)
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	roleIdent := pgx.Identifier{roleName}.Sanitize()

	db, err := openPostgres(t, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	c.Cleanup(func() { dropIntrospectSequenceGrantFixture(c, db, schemaIdent, roleIdent) })

	for _, statement := range []string{
		"CREATE ROLE " + roleIdent + " NOLOGIN",
		"CREATE SCHEMA " + schemaIdent,
		"CREATE SEQUENCE " + schemaIdent + ".order_seq",
		"CREATE TABLE " + schemaIdent + ".invoices (id bigint PRIMARY KEY)",
		"GRANT USAGE ON SEQUENCE " + schemaIdent + ".order_seq TO " + roleIdent,
		"GRANT SELECT ON TABLE " + schemaIdent + ".invoices TO " + roleIdent,
	} {
		_, execErr := db.Exec(statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("execute fixture statement: %s", statement))
	}

	outDir := filepath.Join(t.TempDir(), "models")
	cmd := root.NewRootCommand()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"introspect",
		"--db-url", dsn,
		"--schemas", schemaName,
		"--out", outDir,
		"--package", "models",
	})
	err = cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String()))

	desired, err := goschema.ParseDir(outDir)
	c.Assert(err, qt.IsNil)
	c.Assert(introspectedGrantTargets(desired.Grants, roleName), qt.ContentEquals, []introspectedGrantTarget{
		{Privileges: []string{"SELECT"}, OnTable: schemaName + ".invoices"},
		{Privileges: []string{"USAGE"}, OnSequence: schemaName + ".order_seq"},
	})

	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	compareOpts := config.DefaultCompareOptions()
	compareOpts.Dialect = conn.Info().Dialect
	diff := schemadiff.CompareWithOptions(desired, live, compareOpts)
	c.Assert(diff.GrantsAdded, qt.HasLen, 0, qt.Commentf("grants added: %#v", diff.GrantsAdded))
	c.Assert(diff.GrantsRemoved, qt.HasLen, 0, qt.Commentf("grants removed: %#v", diff.GrantsRemoved))
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

// introspectedGrantTarget is the part of a parsed grant annotation this test is
// about: which target attribute the object was written under.
type introspectedGrantTarget struct {
	Privileges []string
	OnTable    string
	OnSequence string
}

// introspectedGrantTargets keeps the grants to role. The caller compares them
// without regard to order, because the order follows how introspect lays out
// its files and this test is about the target attribute. The connecting user's
// owner privileges are introspected too, and which user that is depends on the
// environment, so only the fixture role is compared.
func introspectedGrantTargets(grants []schemamodel.Grant, role string) []introspectedGrantTarget {
	var out []introspectedGrantTarget
	for _, grant := range grants {
		if grant.Role != role {
			continue
		}
		out = append(out, introspectedGrantTarget{
			Privileges: grant.Privileges,
			OnTable:    grant.OnTable,
			OnSequence: grant.OnSequence,
		})
	}
	return out
}

// dropIntrospectSequenceGrantFixture drops the schema before the role, because
// a role that still holds privileges on an object cannot be dropped.
func dropIntrospectSequenceGrantFixture(c *qt.C, db *sql.DB, schemaIdent, roleIdent string) {
	c.Helper()
	_, err := db.Exec("DROP SCHEMA IF EXISTS " + schemaIdent + " CASCADE")
	c.Check(err, qt.IsNil)
	_, err = db.Exec("DROP ROLE IF EXISTS " + roleIdent)
	c.Check(err, qt.IsNil)
}
