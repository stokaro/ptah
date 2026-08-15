//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestMaterializedViewReadback_LiveUnqualifiedBodyRoundTrips measures what
// PostgreSQL records for a view body whose relation was authored without a
// schema qualifier, and compares the two view kinds side by side.
//
// pg_get_viewdef reports the definition the server resolved rather than the text
// the author wrote, so a body created as `FROM users` in a schema outside the
// reading session's search_path comes back as `FROM <schema>.users`. Measured on
// PostgreSQL 18.4 both kinds answer identically:
//
//	MATVIEW readback:     SELECT count(*) AS c FROM analytics.users
//	PLAIN VIEW readback:  SELECT count(*) AS c FROM analytics.users
//
// The plain view has normalized that readback for as long as the comparator has
// taken a dialect; the materialized view compared the raw strings and reported
// an unchanged declaration as modified, which a plan answers with a drop and a
// create. The plain view here is therefore the control: it asserts that this
// test measures the difference between the two kinds and not something about the
// server.
func TestMaterializedViewReadback_LiveUnqualifiedBodyRoundTrips(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), time.Minute)
	defer cancel()
	conn, schemaName := prepareMaterializedViewReadbackFixture(c, ctx, dbtarget.PostgreSQL)

	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.MatViews, qt.HasLen, 1)
	c.Assert(live.MatViews[0].Schema, qt.Equals, schemaName)
	c.Assert(live.MatViews[0].Body, qt.Contains, schemaName+".users")
	c.Assert(live.Views, qt.HasLen, 1)
	c.Assert(live.Views[0].Schema, qt.Equals, schemaName)
	c.Assert(live.Views[0].Body, qt.Contains, schemaName+".users")

	declared := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserCounts",
			Name:       schemaName + ".user_counts",
			Body:       "SELECT count(*) AS c FROM users",
		}},
		Views: []goschema.View{{
			StructName: "UserCountsPlain",
			Name:       schemaName + ".user_counts_plain",
			Body:       "SELECT count(*) AS c FROM users",
		}},
	}
	// Only the two view kinds, so this is a comparison about their bodies rather
	// than about the source table the desired state does not declare.
	viewLikes := &dbschematypes.DBSchema{Views: live.Views, MatViews: live.MatViews}

	settled := schemadiff.CompareWithDialect(declared, viewLikes, platform.Postgres)
	c.Assert(settled.MaterializedViewsModified, qt.HasLen, 0)
	c.Assert(settled.ViewsModified, qt.HasLen, 0)
	c.Assert(settled.HasChanges(), qt.IsFalse, qt.Commentf("settled diff: %+v", settled))

	// A body that really did change is still a change: what the normalization
	// removes is the qualifier the catalog added, not the author's edit.
	changed := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserCounts",
			Name:       schemaName + ".user_counts",
			Body:       "SELECT count(*) AS c FROM users WHERE enabled",
		}},
		Views: declared.Views,
	}
	changeDiff := schemadiff.CompareWithDialect(changed, viewLikes, platform.Postgres)
	c.Assert(changeDiff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(changeDiff.ViewsModified, qt.HasLen, 0)
}

// prepareMaterializedViewReadbackFixture creates the two view kinds over one
// table from a session whose search_path is the fixture schema, which is the
// only way to author an unqualified relation in a schema of its own, and returns
// a reading connection that does NOT carry that search_path.
func prepareMaterializedViewReadbackFixture(
	c *qt.C,
	ctx context.Context,
	engine dbtarget.Engine,
) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	rawURL := dbtarget.URL(c, engine)

	schemaName := fmt.Sprintf("ptah_matview_readback_%d", time.Now().UnixNano())
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()

	// A dedicated single connection, because search_path is session state and the
	// pooled connection below would not guarantee the same session twice.
	author, err := pgx.Connect(ctx, rawURL)
	c.Assert(err, qt.IsNil)
	statements := []string{
		"CREATE SCHEMA " + schemaIdent,
		"SET search_path TO " + schemaIdent,
		"CREATE TABLE users (id bigint PRIMARY KEY, enabled boolean NOT NULL DEFAULT true)",
		"CREATE MATERIALIZED VIEW user_counts AS SELECT count(*) AS c FROM users",
		"CREATE VIEW user_counts_plain AS SELECT count(*) AS c FROM users",
	}
	for _, statement := range statements {
		_, execErr := author.Exec(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("execute matview readback fixture statement: %s", statement))
	}
	c.Assert(author.Close(ctx), qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, rawURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dropMaterializedViewReadbackFixture(c, context.Background(), conn, schemaIdent)
		c.Check(conn.Close(), qt.IsNil)
	})
	return conn, schemaName
}

func dropMaterializedViewReadbackFixture(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schemaIdent string,
) {
	c.Helper()
	_, err := conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+schemaIdent+" CASCADE")
	c.Check(err, qt.IsNil)
}
