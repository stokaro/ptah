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

// TestOverloadWithQuotedArgumentType_LiveComparesEqual applies two overloads of
// one function, one declaring its argument by a quoted built-in typname, reads
// them back and compares the declaration with them. The server reports the
// quoted "int4" as integer, so the declaration has to match the overload it
// built; kept as written, the overload matched none and every comparison
// planned it created and the stored one dropped.
func TestOverloadWithQuotedArgumentType_LiveComparesEqual(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	schemaName := fmt.Sprintf("ptah_quotedarg_%d", time.Now().UnixNano())
	q := `"` + schemaName + `".`
	document := `CREATE FUNCTION ` + q + `f(a "int4") RETURNS int LANGUAGE sql AS $$ SELECT a $$;
CREATE FUNCTION ` + q + `f(a text) RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;`
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
	c.Assert(diff.FunctionsAdded, qt.HasLen, 0)
	c.Assert(diff.FunctionsRemoved, qt.HasLen, 0)
	c.Assert(diff.FunctionsModified, qt.HasLen, 0)
}
