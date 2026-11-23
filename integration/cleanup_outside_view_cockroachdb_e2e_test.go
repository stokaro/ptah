//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// outsideViewFixture is a CockroachDB database whose public schema holds
// three tables, z_data with three rows, a sequence and a function, and whose
// schema other holds a view that reads public.m_shared.
const outsideViewFixture = `
CREATE TABLE public.a_keep (id int PRIMARY KEY);
CREATE TABLE public.m_shared (id int PRIMARY KEY);
CREATE TABLE public.z_data (id int PRIMARY KEY);
INSERT INTO public.z_data VALUES (1), (2), (3);
CREATE SEQUENCE public.z_seq;
CREATE FUNCTION public.zz_fn() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE SCHEMA other;
CREATE VIEW other.v AS SELECT id FROM public.m_shared;
`

// publicSchemaObjects names the relations and routines in db's public schema,
// sorted.
func publicSchemaObjects(c *qt.C, ctx context.Context, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT r.relname FROM pg_class r JOIN pg_namespace n ON n.oid = r.relnamespace
		WHERE n.nspname = 'public' AND r.relkind IN ('r', 'v', 'm', 'S')
		UNION ALL
		SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public'`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(names)
	return names
}

// TestCleanupOfASchemaAnOutsideViewReadsE2E_FailurePath empties, on
// CockroachDB, a schema with a table that a view in another schema reads.
// The server commits each drop as it runs, so a refusal at that table's drop
// would keep every drop before it; the cleanup refuses before the first one
// instead, and the database keeps every object and z_data its rows. Both the
// native drop-all and the compatibility schema clean reach the same cleanup.
func TestCleanupOfASchemaAnOutsideViewReadsE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		target   clirun.Target
		verb     []string
		wantExit int
	}{
		{name: "ptah db drop-all", target: clirun.Ptah, verb: []string{"db", "drop-all", "--auto-approve", "--db-url"}, wantExit: 2},
		{name: "ptah-compat schema clean", target: clirun.Compat, verb: []string{"schema", "clean", "--auto-approve", "--url"}, wantExit: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
			admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
			dbURL, db := newReplayCleanupDatabase(c, ctx, admin, adminURL,
				fmt.Sprintf("ptah_cleanup_outside_view_%d", time.Now().UnixNano()))
			_, err = db.ExecContext(ctx, outsideViewFixture)
			c.Assert(err, qt.IsNil)

			got := clirun.Run(c, test.target, clirun.Options{Dir: c.TempDir()}, append(slices.Clone(test.verb), dbURL)...)

			c.Assert(got.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, `refusing to clean schema "public": a view the cleanup does not drop reads `+
				`a relation it drops: view "other"."v" reads table "public"."m_shared"; the server commits each drop as `+
				`it runs, so the cleanup refuses before it drops anything`)
			c.Assert(publicSchemaObjects(c, ctx, db), qt.DeepEquals, []string{"a_keep", "m_shared", "z_data", "z_seq", "zz_fn"})
			var rows int
			c.Assert(db.QueryRowContext(ctx, "SELECT count(*) FROM public.z_data").Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 3)
		})
	}
}
