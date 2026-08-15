//go:build integration

package atlasschema_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// A `schema inspect` document has to say which schema each object belongs to,
// and applying that document has to put the object there. This exercises the
// whole chain for one object kind at a time -- read, render, parse, plan,
// execute -- and then asks the CATALOG rather than the document, because the
// two disagreed exactly where this issue lives: the document named the right
// table and the wrong enum, and only pg_type.typnamespace says so.
//
// Measured on PostgreSQL 17.10 before the fix, against a database holding
// `probe.mood` and `probe.f_probe`:
//
//	ENUM     public|mood                      (source: probe|mood)
//	FUNCTION public|f_probe                   (source: probe|f_probe)
//	COLTYPE  probe.probe_enum.feeling -> public.mood
//
// A plain URL describes every schema it reaches (stokaro/ptah#1264), so before
// this every one of those objects was ABSENT from the document instead of
// wrong. Turning "not described" into "described incorrectly" is what
// stokaro/ptah#1276 exists to prevent, which is why each row here asserts the
// schema and not merely the presence of the object.
//
// Every row seeds its own database. A shared fixture would let one kind's
// success stand in for another's, and the whole point of a row per kind is
// that reverting one line of the fix reddens exactly the rows it broke.
//
// Kinds deliberately absent, and why: `role` is cluster-scoped and belongs to
// no schema; extension placement has its own create, move, and convergence
// controls; `grant` and `policy` name their target relation, so their schema is
// the table row's answer, and both are additionally suppressed on the
// Atlas-compatible surface.
func TestInspectLive_EveryObjectKindKeepsItsSchema(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		// setup seeds the source database with a control object in the
		// connected schema and the same kind of object in a schema the URL
		// never names.
		setup []string
		// omitRefused mirrors the Atlas-compatible surface, which leaves out
		// the block types the pinned Atlas community binary v1.3.0 refuses. A
		// row about one of those kinds turns it off, or there is no block to
		// carry a schema.
		omitRefused bool
		// probe is the catalog read that answers where this kind's objects
		// ended up, against the database the document was applied to.
		probe objectSchemaProbe
		want  []string
	}{
		{
			name: "table",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE TABLE public.t_ctl (id integer PRIMARY KEY)",
				"CREATE TABLE probe.t_probe (id integer PRIMARY KEY)",
			},
			omitRefused: true,
			probe:       relationProbe("r", "t_ctl", "t_probe"),
			want:        []string{"probe.t_probe", "public.t_ctl"},
		},
		{
			// The reported defect. The enum block's schema attribute is
			// mandatory -- the pinned binary refuses a block without one -- so
			// it was written unconditionally as the connected schema.
			name: "enum",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE TYPE public.e_ctl AS ENUM ('a', 'b')",
				"CREATE TYPE probe.e_probe AS ENUM ('x', 'y')",
			},
			omitRefused: true,
			probe:       typeProbe("e"),
			want:        []string{"probe.e_probe", "public.e_ctl"},
		},
		{
			// The damage the block header does to a column. The enum is
			// referenced from the table as `type = enum.e_probe`, so a block
			// naming the wrong schema retypes the column against a type in
			// that schema, and only information_schema.columns.udt_schema
			// reports it.
			name: "enum column type",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE TYPE public.e_ctl AS ENUM ('a', 'b')",
				"CREATE TYPE probe.e_probe AS ENUM ('x', 'y')",
				"CREATE TABLE public.t_ctl (id integer PRIMARY KEY, shade public.e_ctl)",
				"CREATE TABLE probe.t_probe (id integer PRIMARY KEY, feeling probe.e_probe)",
			},
			omitRefused: true,
			probe:       columnTypeProbe(),
			want: []string{
				"probe.t_probe.feeling -> probe.e_probe",
				"public.t_ctl.shade -> public.e_ctl",
			},
		},
		{
			// The reported defect's second half. A function block carried no
			// schema attribute at all, because the conversion left the schema
			// off the name the renderer reads it from.
			name: "function",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE FUNCTION public.f_ctl(n integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT n + 1 $$",
				"CREATE FUNCTION probe.f_probe(n integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT n + 2 $$",
			},
			omitRefused: true,
			probe:       functionProbe("f_ctl", "f_probe"),
			want:        []string{"probe.f_probe", "public.f_ctl"},
		},
		{
			name: "domain",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE DOMAIN public.d_ctl AS integer CHECK (VALUE > 0)",
				"CREATE DOMAIN probe.d_probe AS integer CHECK (VALUE >= 0)",
			},
			omitRefused: true,
			probe:       typeProbe("d"),
			want:        []string{"probe.d_probe", "public.d_ctl"},
		},
		{
			name: "composite",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE TYPE public.c_ctl AS (x integer, y integer)",
				"CREATE TYPE probe.c_probe AS (a integer, b integer)",
			},
			omitRefused: true,
			probe:       compositeProbe(),
			want:        []string{"probe.c_probe", "public.c_ctl"},
		},
		{
			name: "range",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE TYPE public.r_ctl AS RANGE (subtype = float8)",
				"CREATE TYPE probe.r_probe AS RANGE (subtype = int8)",
			},
			omitRefused: true,
			probe:       typeProbe("r"),
			want:        []string{"probe.r_probe", "public.r_ctl"},
		},
		{
			name: "view",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE TABLE public.t_ctl (id integer PRIMARY KEY)",
				"CREATE TABLE probe.t_probe (id integer PRIMARY KEY)",
				"CREATE VIEW public.v_ctl AS SELECT id FROM public.t_ctl",
				"CREATE VIEW probe.v_probe AS SELECT id FROM probe.t_probe",
			},
			omitRefused: true,
			probe:       relationProbe("v", "v_ctl", "v_probe"),
			want:        []string{"probe.v_probe", "public.v_ctl"},
		},
		{
			name: "materialized view",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE TABLE public.t_ctl (id integer PRIMARY KEY)",
				"CREATE TABLE probe.t_probe (id integer PRIMARY KEY)",
				"CREATE MATERIALIZED VIEW public.m_ctl AS SELECT id FROM public.t_ctl",
				"CREATE MATERIALIZED VIEW probe.m_probe AS SELECT id FROM probe.t_probe",
			},
			omitRefused: true,
			probe:       relationProbe("m", "m_ctl", "m_probe"),
			want:        []string{"probe.m_probe", "public.m_ctl"},
		},
		{
			// A trigger carries no schema of its own: it belongs to the
			// relation it fires on, which is what the `on` reference names.
			name: "trigger",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE TABLE public.t_ctl (id integer PRIMARY KEY)",
				"CREATE TABLE probe.t_probe (id integer PRIMARY KEY)",
				"CREATE FUNCTION public.g_ctl() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
				"CREATE FUNCTION probe.g_probe() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
				"CREATE TRIGGER x_ctl BEFORE INSERT ON public.t_ctl FOR EACH ROW EXECUTE FUNCTION public.g_ctl()",
				"CREATE TRIGGER x_probe BEFORE INSERT ON probe.t_probe FOR EACH ROW EXECUTE FUNCTION probe.g_probe()",
			},
			omitRefused: true,
			probe:       triggerProbe(),
			want:        []string{"probe.t_probe.x_probe", "public.t_ctl.x_ctl"},
		},
		{
			// A sequence block is one the pinned binary refuses, so the
			// Atlas-compatible surface leaves it out and this row has to ask
			// for the full document Ptah models.
			name: "sequence",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE SEQUENCE public.s_ctl",
				"CREATE SEQUENCE probe.s_probe",
			},
			probe: relationProbe("S", "s_ctl", "s_probe"),
			want:  []string{"probe.s_probe", "public.s_ctl"},
		},
		{
			// An index belongs to the schema of the table it is on, and the
			// catalog is where a mis-scoped one would show up.
			name: "index",
			setup: []string{
				"CREATE SCHEMA probe",
				"CREATE TABLE public.t_ctl (id integer PRIMARY KEY, label text)",
				"CREATE TABLE probe.t_probe (id integer PRIMARY KEY, label text)",
				"CREATE INDEX i_ctl ON public.t_ctl (label)",
				"CREATE INDEX i_probe ON probe.t_probe (label)",
			},
			omitRefused: true,
			probe:       relationProbe("i", "i_ctl", "i_probe"),
			want:        []string{"probe.i_probe", "public.i_ctl"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := newInspectLiveConnection(c, ctx, "", test.setup)

			document, err := atlasschema.Inspect(ctx, source, atlasschema.InspectOptions{
				Format:                 "hcl",
				OmitAtlasRefusedBlocks: test.omitRefused,
			})
			c.Assert(err, qt.IsNil)

			path := filepath.Join(c.TempDir(), "desired.hcl")
			c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)

			// The document put back against the database it describes has to
			// plan nothing. It is the other half of the same fact and it fails
			// differently: an object described in the wrong schema is BOTH
			// absent from the schema the read reports and present in one the
			// desired state does not name, so the plan creates it and drops it
			// in the same run. Measured before the fix, from a document of the
			// database it was applied back to:
			//
			//	CREATE TYPE "public"."p_color" AS ENUM ('red', 'green');
			//	DROP TYPE IF EXISTS "p_color" CASCADE;
			noop, err := atlasschema.PlanApply(ctx, source, atlasschema.ApplyOptions{
				ToURLs: []string{"file://" + path},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(noop.Statements(), qt.HasLen, 0,
				qt.Commentf("document:\n%s\nplan:\n%s", document, noop.SQL()))

			target := newInspectLiveConnection(c, ctx, "", nil)
			plan, err := atlasschema.PlanApply(ctx, target, atlasschema.ApplyOptions{
				ToURLs: []string{"file://" + path},
			})
			c.Assert(err, qt.IsNil, qt.Commentf("document:\n%s", document))
			c.Assert(
				atlasschema.ApplyStatements(ctx, target, migrator.MigrationTxModeNone, plan.Statements()),
				qt.IsNil,
				qt.Commentf("document:\n%s\nplan:\n%s", document, plan.SQL()),
			)

			c.Assert(readObjectSchemas(c, ctx, target, test.probe), qt.DeepEquals, test.want,
				qt.Commentf("document:\n%s", document))
		})
	}
}

// relationProbe reads pg_class for one relkind, narrowed to the two names the
// row created so that an unrelated relation cannot make a row pass or fail.
func relationProbe(relkind string, names ...string) objectSchemaProbe {
	return catalogProbe(`
		SELECT n.nspname || '.' || c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind::text = $1 AND c.relname::text = ANY($2)
		ORDER BY 1`, relkind, names)
}

// typeProbe reads pg_type.typnamespace, which is the fact an `enum`, `domain`
// or `range` block's schema attribute decides.
func typeProbe(typtype string) objectSchemaProbe {
	return catalogProbe(`
		SELECT n.nspname || '.' || t.typname
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE t.typtype::text = $1 AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
		ORDER BY 1`, typtype)
}

// compositeProbe reads pg_type for standalone composite types only. Every
// table, view and materialized view has a composite type of its own, so
// typtype = 'c' alone answers a different question.
func compositeProbe() objectSchemaProbe {
	return catalogProbe(`
		SELECT n.nspname || '.' || t.typname
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_class c ON c.oid = t.typrelid
		WHERE t.typtype = 'c' AND c.relkind = 'c'
		  AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
		ORDER BY 1`)
}

// functionProbe reads pg_proc.pronamespace, narrowed to the row's own names:
// a range type contributes constructor functions and a trigger contributes the
// function Ptah generates for its body, and neither is what this row is about.
func functionProbe(names ...string) objectSchemaProbe {
	return catalogProbe(`
		SELECT n.nspname || '.' || p.proname
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE p.proname::text = ANY($1)
		ORDER BY 1`, names)
}

// triggerProbe reports each user trigger as schema.table.trigger, which is the
// only spelling that separates a trigger recreated on the right relation from
// one recreated on a same-named relation in another schema.
func triggerProbe() objectSchemaProbe {
	return catalogProbe(`
		SELECT n.nspname || '.' || c.relname || '.' || t.tgname
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE NOT t.tgisinternal
		ORDER BY 1`)
}

// columnTypeProbe reads information_schema.columns.udt_schema, the catalog's
// answer to "which schema's type is this column declared against".
func columnTypeProbe() objectSchemaProbe {
	return catalogProbe(`
		SELECT table_schema || '.' || table_name || '.' || column_name
		       || ' -> ' || udt_schema || '.' || udt_name
		FROM information_schema.columns
		WHERE data_type = 'USER-DEFINED'
		  AND table_schema NOT LIKE 'pg\_%' AND table_schema <> 'information_schema'
		ORDER BY 1`)
}

// objectSchemaProbe is the catalog read that reports one kind's (schema, name)
// pairs: the query, and the arguments that narrow it to the names a row created.
type objectSchemaProbe struct {
	query string
	args  []any
}

func catalogProbe(query string, args ...any) objectSchemaProbe {
	return objectSchemaProbe{query: query, args: args}
}

// readObjectSchemas returns what the probe found, asserting only that the read
// itself ran: a probe that cannot reach the catalog answers nothing about the
// schema an object landed in, and an empty result would otherwise read as one.
func readObjectSchemas(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	probe objectSchemaProbe,
) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, probe.query, probe.args...)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()

	found := []string{}
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		found = append(found, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}
