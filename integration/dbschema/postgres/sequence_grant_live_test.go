//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// TestPostgresSequenceGrant_LiveDescriptionComparesEqualToItsRead describes a
// live database holding a grant on a standalone sequence and compares that
// description against the read it was made from.
//
// The comparison needs a server rather than a fixture: the object type is what
// the catalog reports for the relation, and the comparator keys a grant by it.
// PostgreSQL accepts GRANT ... ON TABLE for a sequence, so replaying a sequence
// described as a table succeeds and cannot show the defect. Keying can: a
// description naming the sequence under OnTable plans a GRANT ... ON TABLE and a
// REVOKE ... ON SEQUENCE for a privilege the server already holds, on every run.
//
// The table grant to the same role is the control. It converges either way, so
// the test measures the sequence arm and not the conversion as a whole.
func TestPostgresSequenceGrant_LiveDescriptionComparesEqualToItsRead(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()
	conn, schemaName, roleName := prepareSequenceGrantFixture(c, ctx, dbtarget.PostgreSQL)

	read, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(sequenceGrantReadTargets(read.Grants, roleName), qt.DeepEquals, []sequenceGrantReadTarget{
		{Privilege: "SELECT", ObjectType: "TABLE", Target: schemaName + ".invoices"},
		{Privilege: "USAGE", ObjectType: "SEQUENCE", Target: schemaName + ".order_seq"},
	})

	described := dbschematogo.ConvertDBSchemaToGoSchema(read, "postgres")
	c.Assert(sequenceGrantDescribedTargets(described.Grants, roleName), qt.DeepEquals, []sequenceGrantDescribedTarget{
		{Privileges: []string{"SELECT"}, OnTable: schemaName + ".invoices"},
		{Privileges: []string{"USAGE"}, OnSequence: schemaName + ".order_seq"},
	})

	opts := config.DefaultCompareOptions()
	opts.Dialect = conn.Info().Dialect
	diff := schemadiff.CompareWithOptions(described, read, opts)
	c.Assert(diff.GrantsAdded, qt.HasLen, 0, qt.Commentf("grants added: %#v", diff.GrantsAdded))
	c.Assert(diff.GrantsRemoved, qt.HasLen, 0, qt.Commentf("grants removed: %#v", diff.GrantsRemoved))
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

// sequenceGrantReadTarget is the part of a read grant this test is about.
type sequenceGrantReadTarget struct {
	Privilege  string
	ObjectType string
	Target     string
}

// sequenceGrantDescribedTarget is the part of a described grant this test is
// about: which target field the object landed in.
type sequenceGrantDescribedTarget struct {
	Privileges []string
	OnTable    string
	OnSequence string
}

// sequenceGrantReadTargets keeps the grants to role, in read order. The
// fixture role is the only one the test controls; the connecting user holds
// owner privileges on the same objects, and which user that is depends on the
// environment.
func sequenceGrantReadTargets(grants []catalog.Grant, role string) []sequenceGrantReadTarget {
	var out []sequenceGrantReadTarget
	for _, grant := range grants {
		if grant.Role != role {
			continue
		}
		out = append(out, sequenceGrantReadTarget{
			Privilege:  grant.Privilege,
			ObjectType: grant.ObjectType,
			Target:     grant.QualifiedTarget(),
		})
	}
	return out
}

// sequenceGrantDescribedTargets keeps the described grants to role, in order.
func sequenceGrantDescribedTargets(grants []schemamodel.Grant, role string) []sequenceGrantDescribedTarget {
	var out []sequenceGrantDescribedTarget
	for _, grant := range grants {
		if grant.Role != role {
			continue
		}
		out = append(out, sequenceGrantDescribedTarget{
			Privileges: grant.Privileges,
			OnTable:    grant.OnTable,
			OnSequence: grant.OnSequence,
		})
	}
	return out
}

// prepareSequenceGrantFixture creates one schema holding a standalone sequence
// and a table, and one role granted a privilege on each. A role belongs to the
// cluster rather than to the database, so it is dropped explicitly after the
// schema whose objects it holds privileges on.
func prepareSequenceGrantFixture(
	c *qt.C,
	ctx context.Context,
	engine dbtarget.Engine,
) (*dbschema.DatabaseConnection, string, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})

	suffix := time.Now().UnixNano()
	schemaName := fmt.Sprintf("ptah_seq_grant_%d", suffix)
	roleName := fmt.Sprintf("ptah_seq_grant_role_%d", suffix)
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	roleIdent := pgx.Identifier{roleName}.Sanitize()
	c.Cleanup(func() {
		dropSequenceGrantFixture(c, context.Background(), conn, schemaIdent, roleIdent)
	})

	statements := []string{
		"CREATE ROLE " + roleIdent + " NOLOGIN",
		"CREATE SCHEMA " + schemaIdent,
		"CREATE SEQUENCE " + schemaIdent + ".order_seq",
		"CREATE TABLE " + schemaIdent + ".invoices (id bigint PRIMARY KEY)",
		"GRANT USAGE ON SEQUENCE " + schemaIdent + ".order_seq TO " + roleIdent,
		"GRANT SELECT ON TABLE " + schemaIdent + ".invoices TO " + roleIdent,
	}
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("execute sequence grant fixture statement: %s", statement))
	}
	return conn, schemaName, roleName
}

func dropSequenceGrantFixture(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schemaIdent string,
	roleIdent string,
) {
	c.Helper()
	_, err := conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+schemaIdent+" CASCADE")
	c.Check(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, "DROP ROLE IF EXISTS "+roleIdent)
	c.Check(err, qt.IsNil)
}
