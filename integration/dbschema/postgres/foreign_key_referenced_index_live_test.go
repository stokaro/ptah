//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestReaderForeignKeyReferencedIndex_LiveReadsWhatTheServerBound reads which index
// each foreign key depends on, where two keys cover the column: PostgreSQL binds a
// foreign key to the oldest, so the same declaration reads differently by the order the
// keys were built in, and only the catalog can say which.
func TestReaderForeignKeyReferencedIndex_LiveReadsWhatTheServerBound(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), time.Minute)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	schemaName := fmt.Sprintf("ptah_fk_index_%d", time.Now().UnixNano())
	exec := func(statement string) {
		c.Helper()
		_, err := conn.ExecContext(context.WithoutCancel(ctx), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	exec(`CREATE SCHEMA "` + schemaName + `"`)
	c.Cleanup(func() { exec(`DROP SCHEMA "` + schemaName + `" CASCADE`) })
	exec(`CREATE TABLE "` + schemaName + `".pk_first (id bigint NOT NULL, CONSTRAINT pk_first_pkey PRIMARY KEY (id), CONSTRAINT pk_first_id_key UNIQUE (id))`)
	exec(`CREATE TABLE "` + schemaName + `".uq_first (id bigint NOT NULL, CONSTRAINT uq_first_id_key UNIQUE (id))`)
	exec(`ALTER TABLE "` + schemaName + `".uq_first ADD CONSTRAINT uq_first_pkey PRIMARY KEY (id)`)
	exec(`CREATE TABLE "` + schemaName + `".child (id bigint PRIMARY KEY,
		a bigint CONSTRAINT child_a REFERENCES "` + schemaName + `".pk_first (id),
		b bigint CONSTRAINT child_b REFERENCES "` + schemaName + `".uq_first (id))`)

	schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})

	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeyIndexes(schema.Constraints), qt.DeepEquals, map[string]string{"child_a": "pk_first_pkey", "child_b": "uq_first_id_key"})
}

// foreignKeyIndexes keys the index each foreign key of constraints depends on by the
// foreign key's name.
func foreignKeyIndexes(constraints []catalog.Constraint) map[string]string {
	bound := make(map[string]string)
	for _, constraint := range constraints {
		if constraint.Type == "FOREIGN KEY" {
			bound[constraint.Name] = constraint.ReferencedIndex
		}
	}
	return bound
}
