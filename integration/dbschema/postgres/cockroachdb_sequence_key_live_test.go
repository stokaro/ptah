//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestReaderSequenceKey_LiveLeavesOutTheSequencesKey reads a CockroachDB
// schema holding a sequence and a keyed table.
//
// CockroachDB stores a sequence as a one-row table, and pg_constraint and
// information_schema.table_constraints both list that table's key, named
// primary, against the sequence. Read as a table's constraint, it is one no
// declaration has, so every comparison of a schema with a sequence planned
// `ALTER TABLE "s" DROP CONSTRAINT IF EXISTS "primary"`. The table's own key
// is the control.
func TestReaderSequenceKey_LiveLeavesOutTheSequencesKey(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.CockroachDB))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	schemaName := fmt.Sprintf("ptah_sequence_key_%d", time.Now().UnixNano())
	schema := pgx.Identifier{schemaName}.Sanitize()
	defer func() {
		_, err := conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		c.Check(err, qt.IsNil)
	}()
	for _, statement := range []string{
		"CREATE SCHEMA " + schema,
		"CREATE SEQUENCE " + schema + ".s",
		"CREATE TABLE " + schema + ".t (id INT8 PRIMARY KEY)",
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	var sequenceKeys int
	c.Assert(conn.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_constraint con
		JOIN pg_class cls ON cls.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = cls.relnamespace
		WHERE n.nspname = $1 AND cls.relname = 's' AND con.contype = 'p'`,
		schemaName,
	).Scan(&sequenceKeys), qt.IsNil)
	c.Assert(sequenceKeys, qt.Equals, 1, qt.Commentf("the engine did not catalog a key against the sequence"))

	read, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})

	c.Assert(err, qt.IsNil)
	described := make(map[string]string)
	for _, constraint := range read.Constraints {
		described[constraint.TableName+"."+constraint.Name] = constraint.Type
	}
	c.Assert(described, qt.DeepEquals, map[string]string{"t.t_pkey": "PRIMARY KEY"})
}
