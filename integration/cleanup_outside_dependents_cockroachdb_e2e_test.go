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

// publicSchemaObjects names the relations, routines and enum types in db's
// public schema, sorted.
func publicSchemaObjects(c *qt.C, ctx context.Context, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT r.relname FROM pg_class r JOIN pg_namespace n ON n.oid = r.relnamespace
		WHERE n.nspname = 'public' AND r.relkind IN ('r', 'v', 'm', 'S')
		UNION ALL
		SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public'
		UNION ALL
		SELECT t.typname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = 'public' AND t.typtype = 'e'`)
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
			c.Assert(got.Stderr, qt.Contains, `refusing to clean schema "public": an object the cleanup does not drop `+
				`depends on one it drops: view "other"."v" reads table "public"."m_shared"; the server commits each `+
				`drop as it runs, so the cleanup refuses before it drops anything`)
			c.Assert(publicSchemaObjects(c, ctx, db), qt.DeepEquals, []string{"a_keep", "m_shared", "z_data", "z_seq", "zz_fn"})
			var rows int
			c.Assert(db.QueryRowContext(ctx, "SELECT count(*) FROM public.z_data").Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 3)
		})
	}
}

// TestDropAllOfASchemaAnOutsideObjectDependsOnE2E_FailurePath empties, on
// CockroachDB, a schema holding z_data with 3 rows and an object that an
// object in schema other depends on, one row for each way it can. Each is
// refused before the first drop, the refusal names each dependency by the
// kind of the object that holds it, and the schema keeps every object and
// z_data its rows.
func TestDropAllOfASchemaAnOutsideObjectDependsOnE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		fixture     string
		wantObjects []string
		want        string
	}{
		{
			name: "a foreign key from another schema",
			fixture: `CREATE TABLE public.p (id int PRIMARY KEY);
CREATE SCHEMA other;
CREATE TABLE other.c (id int PRIMARY KEY, pid int REFERENCES public.p (id));`,
			wantObjects: []string{"p", "z_data"},
			want:        `foreign key "c_pid_fkey" on table "other"."c" references table "public"."p"`,
		},
		{
			name: "a function that reads the table",
			fixture: `CREATE TABLE public.t (id int);
CREATE SCHEMA other;
CREATE FUNCTION other.f() RETURNS int LANGUAGE sql AS $$ SELECT count(*)::int FROM public.t $$;`,
			wantObjects: []string{"t", "z_data"},
			want:        `the text of the body of function "other"."f" names table "public"."t"`,
		},
		{
			name: "a procedure that writes the table",
			fixture: `CREATE TABLE public.p (id int);
CREATE SCHEMA other;
CREATE PROCEDURE other.pc() LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.p VALUES (1); END $$;`,
			wantObjects: []string{"p", "z_data"},
			want:        `the text of the body of procedure "other"."pc" names table "public"."p"`,
		},
		{
			name: "a function that calls the function",
			fixture: `CREATE FUNCTION public.fn() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE SCHEMA other;
CREATE FUNCTION other.g() RETURNS int LANGUAGE sql AS $$ SELECT public.fn() $$;`,
			wantObjects: []string{"fn", "z_data"},
			want:        `function "other"."g" calls function "public"."fn"`,
		},
		{
			name: "a trigger that runs the function",
			fixture: `CREATE FUNCTION public.trig_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE SCHEMA other;
CREATE TABLE other.t2 (id int PRIMARY KEY);
CREATE TRIGGER tr BEFORE INSERT ON other.t2 FOR EACH ROW EXECUTE FUNCTION public.trig_fn();`,
			wantObjects: []string{"trig_fn", "z_data"},
			want:        `trigger "tr" on table "other"."t2" runs function "public"."trig_fn"`,
		},
		{
			name: "a trigger that runs the function, which writes the table",
			fixture: `CREATE TABLE public.p (id int);
CREATE FUNCTION public.trig_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.p VALUES (1); RETURN NEW; END $$;
CREATE SCHEMA other;
CREATE TABLE other.t2 (id int PRIMARY KEY);
CREATE TRIGGER tr BEFORE INSERT ON other.t2 FOR EACH ROW EXECUTE FUNCTION public.trig_fn();`,
			wantObjects: []string{"p", "trig_fn", "z_data"},
			want: `table "other"."t2" depends on table "public"."p"; trigger "tr" on table "other"."t2" runs function ` +
				`"public"."trig_fn"`,
		},
		{
			name: "a column default that calls the function",
			fixture: `CREATE FUNCTION public.fn() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE SCHEMA other;
CREATE TABLE other.dt (id int PRIMARY KEY, c int DEFAULT public.fn());`,
			wantObjects: []string{"fn", "z_data"},
			want:        `the default of column "c" of table "other"."dt" names function "public"."fn"`,
		},
		{
			name: "an ON UPDATE expression that calls the function",
			fixture: `CREATE FUNCTION public.fn() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE SCHEMA other;
CREATE TABLE other.ou (id int PRIMARY KEY, f int ON UPDATE public.fn());`,
			wantObjects: []string{"fn", "z_data"},
			want:        `the ON UPDATE expression of column "f" of table "other"."ou" names function "public"."fn"`,
		},
		{
			name: "an ON UPDATE expression that takes from the sequence",
			fixture: `CREATE SEQUENCE public.s;
CREATE SCHEMA other;
CREATE TABLE other.ou (id int PRIMARY KEY, g int ON UPDATE nextval('public.s'));`,
			wantObjects: []string{"s", "z_data"},
			want:        `the ON UPDATE expression of column "g" of table "other"."ou" names sequence "public"."s"`,
		},
		{
			name: "a check constraint that calls the function",
			fixture: `CREATE FUNCTION public.fn() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE SCHEMA other;
CREATE TABLE other.ct (id int PRIMARY KEY, d int CHECK (d < public.fn()));`,
			wantObjects: []string{"fn", "z_data"},
			want:        `check constraint "check_d" on table "other"."ct" names function "public"."fn"`,
		},
		{
			name: "a check constraint that casts to the type",
			fixture: `CREATE TYPE public.e AS ENUM ('a', 'b');
CREATE SCHEMA other;
CREATE TABLE other.et (id int PRIMARY KEY, d string CHECK (d::public.e = 'a'));`,
			wantObjects: []string{"e", "z_data"},
			want:        `check constraint "check_d" on table "other"."et" names type "public"."e"`,
		},
		{
			name: "a column default that takes from the sequence",
			fixture: `CREATE SEQUENCE public.s;
CREATE SCHEMA other;
CREATE TABLE other.t3 (id int PRIMARY KEY DEFAULT nextval('public.s'));`,
			wantObjects: []string{"s", "z_data"},
			want:        `the default of column "id" of table "other"."t3" names sequence "public"."s"`,
		},
		{
			name: "a function that takes from the sequence",
			fixture: `CREATE SEQUENCE public.s;
CREATE SCHEMA other;
CREATE FUNCTION other.k() RETURNS int LANGUAGE sql AS $$ SELECT nextval('public.s')::int $$;`,
			wantObjects: []string{"s", "z_data"},
			want:        `the text of the body of function "other"."k" names sequence "public"."s"`,
		},
		{
			name: "a column of the type",
			fixture: `CREATE TYPE public.e AS ENUM ('a', 'b');
CREATE SCHEMA other;
CREATE TABLE other.t4 (id int PRIMARY KEY, c public.e);`,
			wantObjects: []string{"e", "z_data"},
			want:        `column "c" of table "other"."t4" has type "public"."e"`,
		},
		{
			name: "a function that returns the type",
			fixture: `CREATE TYPE public.e AS ENUM ('a', 'b');
CREATE SCHEMA other;
CREATE FUNCTION other.h() RETURNS public.e LANGUAGE sql AS $$ SELECT 'a'::public.e $$;`,
			wantObjects: []string{"e", "z_data"},
			want: `function "other"."h" takes or returns type "public"."e"; the text of the body of function "other"."h" ` +
				`names type "public"."e"`,
		},
		{
			name: "a function that returns the table's rows",
			fixture: `CREATE TABLE public.t (id int PRIMARY KEY);
CREATE SCHEMA other;
CREATE TABLE other.src (id int PRIMARY KEY);
CREATE FUNCTION other.f() RETURNS SETOF public.t LANGUAGE sql AS $$ SELECT id FROM other.src $$;`,
			wantObjects: []string{"t", "z_data"},
			want:        `function "other"."f" takes or returns table "public"."t"`,
		},
		{
			name: "a view over the view",
			fixture: `CREATE TABLE public.t (id int);
CREATE VIEW public.v AS SELECT id FROM public.t;
CREATE SCHEMA other;
CREATE VIEW other.w AS SELECT id FROM public.v;`,
			wantObjects: []string{"t", "v", "z_data"},
			want:        `view "other"."w" reads view "public"."v"`,
		},
		{
			name: "a view that calls the function",
			fixture: `CREATE FUNCTION public.fn() RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$;
CREATE SCHEMA other;
CREATE VIEW other.vf AS SELECT public.fn() AS n;`,
			wantObjects: []string{"fn", "z_data"},
			want:        `the definition of view "other"."vf" names function "public"."fn"`,
		},
		{
			name: "a view that takes from the sequence",
			fixture: `CREATE SEQUENCE public.s;
CREATE SCHEMA other;
CREATE VIEW other.vs AS SELECT nextval('public.s') AS n;`,
			wantObjects: []string{"s", "z_data"},
			want:        `the definition of view "other"."vs" names sequence "public"."s"`,
		},
		{
			name: "a view that casts to the type",
			fixture: `CREATE TYPE public.e AS ENUM ('a', 'b');
CREATE SCHEMA other;
CREATE VIEW other.vty AS SELECT 'a'::public.e::string AS x;`,
			wantObjects: []string{"e", "z_data"},
			want:        `the definition of view "other"."vty" names type "public"."e"`,
		},
		{
			name: "a materialized view that calls the function",
			fixture: `CREATE FUNCTION public.fn() RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$;
CREATE SCHEMA other;
CREATE MATERIALIZED VIEW other.mvf AS SELECT public.fn() AS n;`,
			wantObjects: []string{"fn", "z_data"},
			want:        `the definition of materialized view "other"."mvf" names function "public"."fn"`,
		},
		{
			name: "a policy USING expression that calls the function",
			fixture: `CREATE FUNCTION public.fn() RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$;
CREATE SCHEMA other;
CREATE TABLE other.ot (id int PRIMARY KEY);
ALTER TABLE other.ot ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON other.ot USING (public.fn() = 1);`,
			wantObjects: []string{"fn", "z_data"},
			want:        `the USING expression of policy "p" on table "other"."ot" names function "public"."fn"`,
		},
		{
			name: "a policy WITH CHECK expression that casts to the type",
			fixture: `CREATE TYPE public.e AS ENUM ('a', 'b');
CREATE SCHEMA other;
CREATE TABLE other.ot (id int PRIMARY KEY, c string);
ALTER TABLE other.ot ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON other.ot FOR INSERT WITH CHECK (c::public.e = 'a');`,
			wantObjects: []string{"e", "z_data"},
			want:        `the WITH CHECK expression of policy "p" on table "other"."ot" names type "public"."e"`,
		},
		{
			name: "a policy that takes from the sequence",
			fixture: `CREATE SEQUENCE public.s;
CREATE SCHEMA other;
CREATE TABLE other.ot (id int PRIMARY KEY);
ALTER TABLE other.ot ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON other.ot USING (nextval('public.s') > 0);`,
			wantObjects: []string{"s", "z_data"},
			want:        `the USING expression of policy "p" on table "other"."ot" names sequence "public"."s"`,
		},
		{
			name: "an index expression that calls the function",
			fixture: `CREATE FUNCTION public.fn() RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$;
CREATE SCHEMA other;
CREATE TABLE other.ot (id int PRIMARY KEY);
CREATE INDEX ix ON other.ot ((public.fn() + id));`,
			wantObjects: []string{"fn", "z_data"},
			want:        `index "ix" on table "other"."ot" names function "public"."fn"`,
		},
		{
			name: "a partial index predicate that casts to the type",
			fixture: `CREATE TYPE public.e AS ENUM ('a', 'b');
CREATE SCHEMA other;
CREATE TABLE other.ot (id int PRIMARY KEY, c string);
CREATE INDEX ix ON other.ot (id) WHERE c::public.e = 'a';`,
			wantObjects: []string{"e", "z_data"},
			want:        `index "ix" on table "other"."ot" names type "public"."e"`,
		},
		{
			name: "a trigger WHEN condition that calls the function",
			fixture: `CREATE FUNCTION public.fn() RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$;
CREATE SCHEMA other;
CREATE TABLE other.ot (id int PRIMARY KEY);
CREATE FUNCTION other.tf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER tr BEFORE INSERT ON other.ot FOR EACH ROW WHEN (public.fn() = 1) EXECUTE FUNCTION other.tf();`,
			wantObjects: []string{"fn", "z_data"},
			want:        `the WHEN condition of trigger "tr" on table "other"."ot" names function "public"."fn"`,
		},
		{
			name: "a trigger WHEN condition that takes from the sequence",
			fixture: `CREATE SEQUENCE public.s;
CREATE SCHEMA other;
CREATE TABLE other.ot (id int PRIMARY KEY);
CREATE FUNCTION other.tf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER tr BEFORE INSERT ON other.ot FOR EACH ROW WHEN (nextval('public.s') > 0) EXECUTE FUNCTION other.tf();`,
			wantObjects: []string{"s", "z_data"},
			want:        `the WHEN condition of trigger "tr" on table "other"."ot" names sequence "public"."s"`,
		},
		{
			name: "a view whose definition loses the type",
			fixture: `CREATE TYPE public.e AS ENUM ('a', 'b');
CREATE SCHEMA other;
CREATE VIEW other.vn AS SELECT cardinality(NULL::public.e[]) AS n;`,
			wantObjects: []string{"e", "z_data"},
			want:        `CockroachDB records that type "public"."e" is used by other.vn`,
		},
		{
			name: "a function whose body loses the type",
			fixture: `CREATE TYPE public.e AS ENUM ('a', 'b');
CREATE SCHEMA other;
CREATE FUNCTION other.fa() RETURNS int LANGUAGE sql AS $$ SELECT cardinality(NULL::public.e[]) $$;`,
			wantObjects: []string{"e", "z_data"},
			want:        `CockroachDB records that type "public"."e" is used by other.fa`,
		},
		{
			name: "a column default that loses the type",
			fixture: `CREATE TYPE public.e AS ENUM ('a', 'b');
CREATE SCHEMA other;
CREATE TABLE other.dt (id int PRIMARY KEY, d string DEFAULT 'a'::public.e::string);`,
			wantObjects: []string{"e", "z_data"},
			want:        `CockroachDB records that type "public"."e" is used by other.dt`,
		},
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
				fmt.Sprintf("ptah_cleanup_outside_dependent_%d", time.Now().UnixNano()))
			_, err = db.ExecContext(ctx, "CREATE TABLE public.z_data (id int PRIMARY KEY); "+
				"INSERT INTO public.z_data VALUES (1), (2), (3);")
			c.Assert(err, qt.IsNil)
			_, err = db.ExecContext(ctx, test.fixture)
			c.Assert(err, qt.IsNil)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, "db", "drop-all", "--auto-approve", "--db-url", dbURL)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, `refusing to clean schema "public": an object the cleanup does not drop `+
				`depends on one it drops: `+test.want+`; the server commits each drop as it runs, so the cleanup `+
				`refuses before it drops anything`)
			c.Assert(publicSchemaObjects(c, ctx, db), qt.DeepEquals, test.wantObjects)
			var rows int
			c.Assert(db.QueryRowContext(ctx, "SELECT count(*) FROM public.z_data").Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 3)
		})
	}
}

// TestDropAllOfASchemaWhoseTypeOnlyItsOwnObjectsUseE2E_HappyPath empties, on
// CockroachDB, a schema whose type its own table, view and function use, the
// view and the function through a text the server folds the type out of. The
// server lists them when asked what depends on the type; they are all in the
// cleanup, so nothing is refused, the question leaves the cleanup's
// transaction open, and the schema ends empty.
func TestDropAllOfASchemaWhoseTypeOnlyItsOwnObjectsUseE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	dbURL, db := newReplayCleanupDatabase(c, ctx, admin, adminURL,
		fmt.Sprintf("ptah_cleanup_own_type_%d", time.Now().UnixNano()))
	_, err = db.ExecContext(ctx, `CREATE TYPE public.e AS ENUM ('a', 'b');
CREATE TABLE public.t (id int PRIMARY KEY, c public.e);
CREATE VIEW public.vn AS SELECT cardinality(NULL::public.e[]) AS n;
CREATE FUNCTION public.fa() RETURNS int LANGUAGE sql AS $$ SELECT cardinality(NULL::public.e[]) $$;`)
	c.Assert(err, qt.IsNil)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, "db", "drop-all", "--auto-approve", "--db-url", dbURL)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(publicSchemaObjects(c, ctx, db), qt.IsNil)
}
