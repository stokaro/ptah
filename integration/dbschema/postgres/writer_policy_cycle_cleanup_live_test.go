//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/dbtarget"
)

// policyCycleShapes are relations that row-security policies hold back from
// one another: each drop with RESTRICT is refused while the policy that reads
// the relation is there, and no relation of the cycle can go first.
var policyCycleShapes = []struct {
	name  string
	setup string
}{
	{
		name: "two tables whose policies read each other's table",
		setup: `
			CREATE TABLE public.a (id bigint PRIMARY KEY, who text);
			CREATE TABLE public.b (id bigint PRIMARY KEY, who text);
			ALTER TABLE public.a ENABLE ROW LEVEL SECURITY;
			ALTER TABLE public.b ENABLE ROW LEVEL SECURITY;
			CREATE POLICY pa ON public.a USING (EXISTS (SELECT 1 FROM public.b WHERE b.who = current_user));
			CREATE POLICY pb ON public.b USING (EXISTS (SELECT 1 FROM public.a WHERE a.who = current_user));`,
	},
	{
		name: "a cycle through a view",
		setup: `
			CREATE TABLE public.a (id bigint PRIMARY KEY, who text);
			CREATE TABLE public.b (id bigint PRIMARY KEY, who text);
			CREATE VIEW public.b_mine AS SELECT id FROM public.b WHERE who = current_user;
			ALTER TABLE public.a ENABLE ROW LEVEL SECURITY;
			ALTER TABLE public.b ENABLE ROW LEVEL SECURITY;
			CREATE POLICY pa ON public.a USING (id IN (SELECT id FROM public.b_mine));
			CREATE POLICY pb ON public.b USING (EXISTS (SELECT 1 FROM public.a WHERE a.who = current_user));`,
	},
	{
		name: "a table that its own policy reads, beside a cycle across schemas",
		setup: `
			CREATE SCHEMA app;
			CREATE TABLE public.a (id bigint PRIMARY KEY, who text);
			CREATE TABLE app.b (id bigint PRIMARY KEY, who text);
			ALTER TABLE public.a ENABLE ROW LEVEL SECURITY;
			ALTER TABLE app.b ENABLE ROW LEVEL SECURITY;
			CREATE POLICY pa ON public.a USING (EXISTS (SELECT 1 FROM app.b WHERE b.who = current_user));
			CREATE POLICY pb ON app.b USING (EXISTS (SELECT 1 FROM public.a WHERE a.who = current_user));
			CREATE POLICY self ON public.a FOR UPDATE USING (id IN (SELECT id FROM public.a));`,
	},
}

// postgresWriterLivePolicies counts the row-security policies left in the
// database.
func postgresWriterLivePolicies(c *qt.C, ctx context.Context, db *sql.DB) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_policy`).Scan(&count), qt.IsNil)
	return count
}

// TestWriterDropDatabaseRealm_LivePostgresDropsAPolicyCycle empties the realm
// of a database whose relations policies hold back from one another. The
// cleanup drops the policies of the tables it removes before any relation, so
// every relation goes with RESTRICT and no policy is left.
func TestWriterDropDatabaseRealm_LivePostgresDropsAPolicyCycle(t *testing.T) {
	for _, shape := range policyCycleShapes {
		t.Run(shape.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
			defer liveDatabase.cleanup()
			db := liveDatabase.db
			_, err := db.ExecContext(ctx, shape.setup)
			c.Assert(err, qt.IsNil)

			err = postgres.NewPostgreSQLWriter(db, "public").DropDatabaseRealm(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(postgresWriterLiveRelationCount(c, ctx, db, "public"), qt.Equals, 0)
			c.Assert(postgresWriterLiveSchemaCount(c, ctx, db, "app"), qt.Equals, 0)
			c.Assert(postgresWriterLivePolicies(c, ctx, db), qt.Equals, 0)
		})
	}
}

// TestWriterDropAllTables_LivePostgresDropsAPolicyCycle empties the public
// schema alone, which takes the same order: the first two shapes keep their
// whole cycle in public.
func TestWriterDropAllTables_LivePostgresDropsAPolicyCycle(t *testing.T) {
	for _, shape := range policyCycleShapes[:2] {
		t.Run(shape.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
			defer liveDatabase.cleanup()
			db := liveDatabase.db
			_, err := db.ExecContext(ctx, shape.setup)
			c.Assert(err, qt.IsNil)

			err = postgres.NewPostgreSQLWriter(db, "public").DropAllTables(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(postgresWriterLiveRelationCount(c, ctx, db, "public"), qt.Equals, 0)
			c.Assert(postgresWriterLivePolicies(c, ctx, db), qt.Equals, 0)
		})
	}
}

// TestWriterDropAllTables_LivePostgresKeepsARelationAnotherSchemaReads is the
// control: a policy in a schema the cleanup leaves alone still reads the
// table, so RESTRICT refuses its drop, the cleanup fails, and nothing is
// dropped, the policies of the managed tables included.
func TestWriterDropAllTables_LivePostgresKeepsARelationAnotherSchemaReads(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	defer liveDatabase.cleanup()
	db := liveDatabase.db
	_, err := db.ExecContext(ctx, `
		CREATE SCHEMA kept;
		CREATE TABLE public.a (id bigint PRIMARY KEY, who text);
		ALTER TABLE public.a ENABLE ROW LEVEL SECURITY;
		CREATE POLICY pa ON public.a USING (who = current_user);
		CREATE TABLE kept.t (id bigint PRIMARY KEY);
		ALTER TABLE kept.t ENABLE ROW LEVEL SECURITY;
		CREATE POLICY reads_a ON kept.t USING (id IN (SELECT id FROM public.a));`)
	c.Assert(err, qt.IsNil)

	err = postgres.NewPostgreSQLWriter(db, "public").DropAllTables(ctx)

	c.Assert(err, qt.ErrorMatches, `(?s)refusing to clean schema "public": failed to drop table a: .*SQLSTATE 2BP01.*`)
	c.Assert(postgresWriterLiveRelationsLike(c, ctx, db, "a"), qt.Equals, 1)
	c.Assert(postgresWriterLivePolicyCount(c, ctx, db, "public", "pa"), qt.Equals, 1)
	c.Assert(postgresWriterLivePolicyCount(c, ctx, db, "kept", "reads_a"), qt.Equals, 1)
}

// TestWriterDropDatabaseRealmKeeping_LivePostgresKeepsAnExtensionTablesPolicy
// keeps hstore and a table it owns, which holds a policy: the table keeps its
// policy, and the user's tables go with theirs.
func TestWriterDropDatabaseRealmKeeping_LivePostgresKeepsAnExtensionTablesPolicy(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	defer liveDatabase.cleanup()
	db := liveDatabase.db
	_, err := db.ExecContext(ctx, `
		CREATE EXTENSION hstore;
		CREATE TABLE public.member_table (id integer PRIMARY KEY);
		ALTER EXTENSION hstore ADD TABLE public.member_table;
		ALTER TABLE public.member_table ENABLE ROW LEVEL SECURITY;
		CREATE POLICY member_policy ON public.member_table USING (id > 0);
		CREATE TABLE public.user_table (id integer PRIMARY KEY);
		ALTER TABLE public.user_table ENABLE ROW LEVEL SECURITY;
		CREATE POLICY user_policy ON public.user_table USING (id IN (SELECT id FROM public.member_table));`)
	c.Assert(err, qt.IsNil)

	err = postgres.NewPostgreSQLWriter(db, "public").DropDatabaseRealmKeeping(ctx, dbreset.Kept{Extensions: []string{"hstore"}})

	c.Assert(err, qt.IsNil)
	c.Assert(postgresWriterLiveRelationsLike(c, ctx, db, "member_table"), qt.Equals, 1)
	c.Assert(postgresWriterLivePolicyCount(c, ctx, db, "public", "member_policy"), qt.Equals, 1)
	c.Assert(postgresWriterLiveRelationsLike(c, ctx, db, "user_table"), qt.Equals, 0)
	c.Assert(postgresWriterLivePolicies(c, ctx, db), qt.Equals, 1)
}
