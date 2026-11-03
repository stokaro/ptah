//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

// TestAddColumnPrimaryUnique_LiveComparesEqual applies a column added with
// both PRIMARY KEY and UNIQUE, reads the table back, and compares the
// declaration with it. PostgreSQL builds one key for the two, so the
// declaration has to read as that one key; read as a primary key and a
// separate UNIQUE, every comparison planned a UNIQUE the database does not
// hold.
func TestAddColumnPrimaryUnique_LiveComparesEqual(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	schemaName := fmt.Sprintf("ptah_addpkuq_%d", time.Now().UnixNano())
	q := `"` + schemaName + `".`
	document := `CREATE TABLE ` + q + `t (a int);
ALTER TABLE ` + q + `t ADD COLUMN id int PRIMARY KEY UNIQUE;`
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	})
	_, err = conn.ExecContext(ctx, document)
	c.Assert(err, qt.IsNil)

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	declared, _, err := sqlschema.Read([]byte(document), conn.Info().Dialect)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, &declared, live, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
}
