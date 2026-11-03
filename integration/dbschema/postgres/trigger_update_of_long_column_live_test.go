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

// longUpdateOfColumn is 72 bytes, past the 63 PostgreSQL keeps of a name.
const longUpdateOfColumn = "a_column_with_a_name_that_is_much_longer_than_sixty_three_bytes_in_total"

// TestTriggerUpdateOfLongColumn_LiveComparesEqual applies a trigger whose
// UPDATE OF names a column longer than PostgreSQL keeps, reads it back, and
// compares the declaration with it. The server stores the column, and the
// trigger's column list, under the first 63 bytes, so the declaration has to
// name the cut column for the comparison to find nothing to replace; reading
// it whole planned CREATE OR REPLACE TRIGGER on every comparison.
func TestTriggerUpdateOfLongColumn_LiveComparesEqual(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	schemaName := fmt.Sprintf("ptah_trigcut_%d", time.Now().UnixNano())
	q := `"` + schemaName + `".`
	document := `CREATE TABLE ` + q + `t (id bigint PRIMARY KEY, ` + longUpdateOfColumn + ` int, b int);
CREATE FUNCTION ` + q + `trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER t_upd BEFORE UPDATE OF ` + longUpdateOfColumn + ` ON ` + q + `t FOR EACH ROW EXECUTE FUNCTION ` + q + `trg();`
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
	c.Assert(live.Triggers, qt.HasLen, 1)
	c.Assert(live.Triggers[0].Event, qt.Equals, "UPDATE OF "+longUpdateOfColumn[:63])
	c.Assert(diff.TriggersModified, qt.HasLen, 0)
	c.Assert(diff.TriggersAdded, qt.HasLen, 0)
	c.Assert(diff.TriggersRemoved, qt.HasLen, 0)
	c.Assert(declared.Triggers[0].Event, qt.Not(qt.Contains), longUpdateOfColumn)
}
