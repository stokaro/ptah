//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedpg"
)

// An authored spelling is not an identity, and only the server can say what a
// spelling names. `table: docs` and `schema: public, table: docs` are one
// pg_class row when search_path is public, and Ptah derived a separate source
// identity, outbox table, advisory lock and target pointer from each -- so one
// physical source had two outboxes and two lifecycle domains that did not know
// about each other (stokaro/ptah#2806).
//
// Measured against the CLI before the fix: preparing the two spellings left the
// database with `ptah_embedding_outbox_docs_364c487c9a86` and
// `ptah_embedding_outbox_docs_bfc439a6f41c`, and two distinct `source` values
// in ptah_embedding_run. After it, one of each.
//
// It has to be a live test. `to_regclass` answers against the session's
// search_path, and replacing an omitted schema with `public` offline would be
// wrong on any database that sets one.

// relationFixture creates a schema holding one table and removes it after.
func relationFixture(c *qt.C, ctx context.Context, db *sql.DB, schema string) {
	c.Helper()
	_, err := db.ExecContext(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema))
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA %q`, schema))
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q.relident (id int PRIMARY KEY)`, schema))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(),
			fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema))
	})
}

func TestEmbedPGResolveRelationLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })
	c.Assert(db.PingContext(ctx), qt.IsNil)

	relationFixture(c, ctx, db, "relident_one")
	relationFixture(c, ctx, db, "relident_two")

	t.Run("an omitted schema resolves through the search path", func(t *testing.T) {
		c := qt.New(t)
		conn, err := db.Conn(ctx)
		c.Assert(err, qt.IsNil)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, `SET search_path = relident_one`)
		c.Assert(err, qt.IsNil)

		bare, found, err := embedpg.ResolveRelation(ctx, conn, "", "relident")

		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(bare.Schema, qt.Equals, "relident_one")
		c.Assert(bare.Table, qt.Equals, "relident")
	})

	t.Run("the same search path resolves the qualified spelling to the same relation", func(t *testing.T) {
		c := qt.New(t)
		conn, err := db.Conn(ctx)
		c.Assert(err, qt.IsNil)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, `SET search_path = relident_one`)
		c.Assert(err, qt.IsNil)

		bare, _, err := embedpg.ResolveRelation(ctx, conn, "", "relident")
		c.Assert(err, qt.IsNil)
		qualified, _, err := embedpg.ResolveRelation(ctx, conn, "relident_one", "relident")
		c.Assert(err, qt.IsNil)

		c.Assert(qualified, qt.DeepEquals, bare)
		c.Assert(embedpg.SourceIdentity(qualified.Schema, qualified.Table), qt.Equals,
			embedpg.SourceIdentity(bare.Schema, bare.Table))
	})

	t.Run("a different search path resolves the same spelling elsewhere", func(t *testing.T) {
		c := qt.New(t)
		conn, err := db.Conn(ctx)
		c.Assert(err, qt.IsNil)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, `SET search_path = relident_two`)
		c.Assert(err, qt.IsNil)

		resolved, found, err := embedpg.ResolveRelation(ctx, conn, "", "relident")

		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(resolved.Schema, qt.Equals, "relident_two")
	})

	t.Run("two schemas holding one table name stay distinct", func(t *testing.T) {
		c := qt.New(t)

		one, _, err := embedpg.ResolveRelation(ctx, db, "relident_one", "relident")
		c.Assert(err, qt.IsNil)
		two, _, err := embedpg.ResolveRelation(ctx, db, "relident_two", "relident")
		c.Assert(err, qt.IsNil)

		c.Assert(one, qt.Not(qt.DeepEquals), two)
		c.Assert(embedpg.SourceIdentity(one.Schema, one.Table), qt.Not(qt.Equals),
			embedpg.SourceIdentity(two.Schema, two.Table))
	})

	t.Run("a spelling that names nothing comes back unchanged", func(t *testing.T) {
		c := qt.New(t)

		resolved, found, err := embedpg.ResolveRelation(ctx, db, "relident_one", "absent")

		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsFalse)
		c.Assert(resolved.Schema, qt.Equals, "relident_one")
		c.Assert(resolved.Table, qt.Equals, "absent")
	})
}

// The separation the fix rests on: a physical identity is resolved, and the
// generation's content address is not. `ptah inference describe` computes that
// digest with no database, so a digest that moved with a session's search_path
// would give one document two identities depending on where it was read.
func TestEmbedPGResolvedSpecLeavesTheIdentityAloneLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })
	c.Assert(db.PingContext(ctx), qt.IsNil)

	relationFixture(c, ctx, db, "relident_spec")

	spec := embedgen.Spec{}
	spec.Source.Schema = ""
	spec.Source.Table = "relident"
	spec.Target.Schema = "relident_spec"
	spec.Target.Table = "relident"
	spec.Target.Column = "embedding"

	conn, err := db.Conn(ctx)
	c.Assert(err, qt.IsNil)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, `SET search_path = relident_spec`)
	c.Assert(err, qt.IsNil)

	physical, err := embedpg.WithResolvedRelations(ctx, conn, spec)

	c.Assert(err, qt.IsNil)
	c.Assert(physical.Source.Schema, qt.Equals, "relident_spec")
	c.Assert(physical.Source.Table, qt.Equals, "relident")
	c.Assert(spec.Source.Schema, qt.Equals, "",
		qt.Commentf("the authored specification is not rewritten"))
	c.Assert(physical.Identity().Digest, qt.Not(qt.Equals), spec.Identity().Digest,
		qt.Commentf("the resolved copy is for physical identity; the authored digest is what a generation is"))
}
