//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A column default can name a sequence as text: nextval('rebase.ids'). The
// server reads the string as a regclass constant as it reads the statement,
// and refuses `relation "rebase.ids" does not exist` while the database lacks
// the sequence. So a scoped rehearsal whose comparison leaves the sequence out
// recreates it on the dev database from the target, as it recreates a function
// a policy calls, whether the plan adds the default or the target already
// holds it. The plan does not create, change or drop the sequence: it stays
// where the target holds it, and a second run plans nothing.
//
// Each row applies on both binaries, against a URL that reaches the whole
// database and one bound to public by search_path, and reads the target and
// the dev database back.

// scopedSequenceIDs is a sequence in a schema of its own, which the rows
// exclude.
const scopedSequenceIDs = "CREATE SCHEMA rebase;\nCREATE SEQUENCE rebase.ids;\n"

// scopedSequenceCase is a scoped rehearsal and what the target holds after
// the apply.
type scopedSequenceCase struct {
	tc scopedRehearsalCase
	// defaults are the columns of public.tasks with their defaults, as
	// [scopedRehearsalDefaults] lists them.
	defaults string
	// sequences are the target's sequences, as [scopedRehearsalSequences]
	// lists them.
	sequences string
}

var scopedSequenceCases = []scopedSequenceCase{
	{
		tc: scopedRehearsalCase{
			name:    "a planned default calls nextval of a sequence of an excluded schema",
			target:  scopedSequenceIDs + "CREATE TABLE public.tasks (id int PRIMARY KEY);\n",
			desired: "CREATE TABLE public.tasks (id int PRIMARY KEY, n bigint DEFAULT nextval('rebase.ids'));\n",
			args:    scopedRehearsalExcludeRebase,
			want:    `ADD COLUMN "n" bigint`,
		},
		defaults:  "id=,n=nextval('rebase.ids'::regclass)",
		sequences: "rebase.ids",
	},
	{
		tc: scopedRehearsalCase{
			name:    "a planned default casts the sequence's name to regclass",
			target:  scopedSequenceIDs + "CREATE TABLE public.tasks (id int PRIMARY KEY);\n",
			desired: "CREATE TABLE public.tasks (id int PRIMARY KEY, n bigint DEFAULT nextval('rebase.ids'::regclass));\n",
			args:    scopedRehearsalExcludeRebase,
			want:    `ADD COLUMN "n" bigint`,
		},
		defaults:  "id=,n=nextval('rebase.ids'::regclass)",
		sequences: "rebase.ids",
	},
	{
		tc: scopedRehearsalCase{
			name:    "a planned default names a sequence and its schema quoted",
			target:  "CREATE SCHEMA \"Re Base\";\nCREATE SEQUENCE \"Re Base\".\"Ids\";\nCREATE TABLE public.tasks (id int PRIMARY KEY);\n",
			desired: "CREATE TABLE public.tasks (id int PRIMARY KEY, n bigint DEFAULT nextval('\"Re Base\".\"Ids\"'));\n",
			args:    []string{"--exclude", "Re Base", "--exclude", "Re Base.*"},
			want:    `ADD COLUMN "n" bigint`,
		},
		defaults:  `id=,n=nextval('"Re Base"."Ids"'::regclass)`,
		sequences: "Re Base.Ids",
	},
	{
		tc: scopedRehearsalCase{
			name:    "a planned default names a sequence of public the selection leaves out bare",
			target:  "CREATE SEQUENCE public.ids;\nCREATE TABLE public.tasks (id int PRIMARY KEY);\n",
			desired: "CREATE TABLE public.tasks (id int PRIMARY KEY, n bigint DEFAULT nextval('ids'));\n",
			args:    []string{"--include", "public.tasks"},
			want:    `ADD COLUMN "n" bigint`,
		},
		defaults:  "id=,n=nextval('ids'::regclass)",
		sequences: "public.ids",
	},
	{
		tc: scopedRehearsalCase{
			name:    "a kept default casts nextval of a sequence of an excluded schema",
			target:  scopedSequenceIDs + "CREATE TABLE public.tasks (id int PRIMARY KEY, code text DEFAULT nextval('rebase.ids')::text);\n",
			desired: "CREATE TABLE public.tasks (id int PRIMARY KEY, code text DEFAULT nextval('rebase.ids')::text, done boolean);\n",
			args:    scopedRehearsalExcludeRebase,
			want:    `ADD COLUMN "done" boolean`,
		},
		defaults:  "id=,code=(nextval('rebase.ids'::regclass))::text,done=",
		sequences: "rebase.ids",
	},
	scopedSequenceKeptBigint,
}

// scopedSequenceKeptBigint is a kept default of a bigint column. A baseline
// that lacks the sequence writes such a column as a bigserial drawing from a
// sequence of its own, which a direct apply's rehearsal accepts and the
// comparison after the replay of a saved plan does not.
var scopedSequenceKeptBigint = scopedSequenceCase{
	tc: scopedRehearsalCase{
		name:    "a kept bigint default calls nextval of a sequence of an excluded schema",
		target:  scopedSequenceIDs + "CREATE TABLE public.tasks (id int PRIMARY KEY, n bigint DEFAULT nextval('rebase.ids'));\n",
		desired: "CREATE TABLE public.tasks (id int PRIMARY KEY, n bigint DEFAULT nextval('rebase.ids'), done boolean);\n",
		args:    scopedRehearsalExcludeRebase,
		want:    `ADD COLUMN "done" boolean`,
	},
	defaults:  "id=,n=nextval('rebase.ids'::regclass),done=",
	sequences: "rebase.ids",
}

// scopedRehearsalAppliers are the binaries of [scopedRehearsalSurfaces],
// applying the plan to the target once the rehearsal passed.
var scopedRehearsalAppliers = []scopedRehearsalSurface{
	{
		name:   "ptah-compat",
		target: clirun.Compat,
		args: func(target, desired, dev string) []string {
			return []string{"schema", "apply", "--url", target, "--to", "file://" + filepath.ToSlash(desired), "--dev-url", dev, "--auto-approve"}
		},
	},
	{
		name:   "ptah",
		target: clirun.Ptah,
		args: func(target, desired, dev string) []string {
			return []string{"schema", "apply", "--db-url", target, "--schema-file", desired, "--dev-url", dev, "--auto-approve"}
		},
	},
}

// scopedRehearsalSynced is what both binaries print for a run that plans
// nothing.
const scopedRehearsalSynced = "Schema is synced, no changes to be made"

// TestScopedRehearsalRecreatesTheSequenceANextvalNames_HappyPathE2E applies
// each plan, then runs it again. The rehearsal passes, so the target gets the
// column; the sequence stays the one the target held, since the plan creates
// and drops no sequence; the dev database holds public alone, empty, after each
// run; and the second run plans nothing.
func TestScopedRehearsalRecreatesTheSequenceANextvalNames_HappyPathE2E(t *testing.T) {
	for _, test := range scopedSequenceCases {
		for _, surface := range scopedRehearsalAppliers {
			for _, form := range scopedRehearsalForms {
				t.Run(test.tc.name+"/"+surface.name+"/"+form.name, func(t *testing.T) {
					c := qt.New(t)
					env := newScopedRehearsalEnv(c, test.tc, form)

					first := env.apply(c, surface, test.tc.args)
					second := env.apply(c, surface, test.tc.args)

					c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
					c.Assert(first.Stdout, qt.Contains, test.tc.want)
					c.Assert(first.Stdout, qt.Not(qt.Contains), "SEQUENCE")
					c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
					c.Assert(second.Stdout, qt.Contains, scopedRehearsalSynced)
					c.Assert(scopedRehearsalDefaults(c, env.target), qt.Equals, test.defaults)
					c.Assert(scopedRehearsalSequences(c, env.target), qt.Equals, test.sequences)
					c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
					c.Assert(scopedRehearsalRelations(c, env.dev), qt.Equals, 0)
				})
			}
		}
	}
}

// TestScopedRehearsalReplaysASavedPlanBesideAKeptNextvalDefault_HappyPathE2E
// saves the plan of the kept bigint default with schema plan and applies it
// with schema apply --plan, which replays the plan on the dev database and
// compares the state it reaches with the desired one. The plan is the native
// JSON one, since the .plan.hcl format has no field for the excludes. Without
// the sequence, the dev database would hold the column as a bigserial drawing
// from a sequence of its own, and the comparison would report that drift and
// refuse the plan.
func TestScopedRehearsalReplaysASavedPlanBesideAKeptNextvalDefault_HappyPathE2E(t *testing.T) {
	test := scopedSequenceKeptBigint
	for _, form := range scopedRehearsalForms {
		t.Run(form.name, func(t *testing.T) {
			c := qt.New(t)
			env := newScopedRehearsalEnv(c, test.tc, form)
			plan := filepath.ToSlash(filepath.Join(c.TempDir(), "tasks.plan.json"))
			options := clirun.Options{Dir: c.TempDir(), Env: append(environmentWithoutPtahVariables(), scopedRehearsalDisposable...)}
			desired := "file://" + filepath.ToSlash(env.desired)

			planned := clirun.Run(c, clirun.Compat, options, append([]string{
				"schema", "plan", "--from", env.target, "--to", desired, "--dev-url", env.dev, "--output", plan,
			}, test.tc.args...)...)
			applied := clirun.Run(c, clirun.Compat, options,
				"schema", "apply", "--url", env.target, "--to", desired, "--dev-url", env.dev, "--plan", "file://"+plan, "--auto-approve")

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(scopedRehearsalDefaults(c, env.target), qt.Equals, test.defaults)
			c.Assert(scopedRehearsalSequences(c, env.target), qt.Equals, test.sequences)
			c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
		})
	}
}

// TestScopedRehearsalReportsASequenceTheTargetLacks_FailurePathE2E rehearses a
// default that names a sequence the target does not hold either. Nothing is
// recreated, the dev database refuses the plan by the sequence's name, as the
// target would, and the target is left as it was.
func TestScopedRehearsalReportsASequenceTheTargetLacks_FailurePathE2E(t *testing.T) {
	tc := scopedRehearsalCase{
		target:  scopedSequenceIDs + "CREATE TABLE public.tasks (id int PRIMARY KEY);\n",
		desired: "CREATE TABLE public.tasks (id int PRIMARY KEY, n bigint DEFAULT nextval('rebase.missing'));\n",
		args:    scopedRehearsalExcludeRebase,
	}
	for _, surface := range scopedRehearsalAppliers {
		for _, form := range scopedRehearsalForms {
			t.Run(surface.name+"/"+form.name, func(t *testing.T) {
				c := qt.New(t)
				env := newScopedRehearsalEnv(c, tc, form)

				got := env.apply(c, surface, tc.args)

				c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stderr, qt.Contains, `relation "rebase.missing" does not exist`)
				c.Assert(got.Stderr, qt.Contains, "the plan was not applied to the target database")
				c.Assert(scopedRehearsalColumns(c, env.target), qt.Equals, "id")
				c.Assert(scopedRehearsalSchemas(c, env.dev), qt.Equals, "public")
			})
		}
	}
}

// scopedSequenceHeldRebase is a dev database that holds a rebase schema of its
// own before the run.
const scopedSequenceHeldRebase = "CREATE SCHEMA rebase;\nCREATE TABLE rebase.bystander (id int);\n"

// TestScopedRehearsalRefusesASequenceInASchemaTheDevDatabaseHeld_FailurePathE2E
// rehearses the planned nextval default through a URL bound to public on a dev
// database that holds a rebase schema of its own. Every reset leaves that
// schema as the claim found it, so the sequence would outlive the run there.
// The rehearsal refuses before it writes anything, the target keeps its
// columns, and rebase holds what it held.
func TestScopedRehearsalRefusesASequenceInASchemaTheDevDatabaseHeld_FailurePathE2E(t *testing.T) {
	tc := scopedSequenceCases[0].tc
	tc.dev = scopedSequenceHeldRebase
	for _, surface := range scopedRehearsalAppliers {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			env := newScopedRehearsalEnv(c, tc, scopedRehearsalBound)

			got := env.apply(c, surface, tc.args)

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, `failed during baseline: the dev database held schema "rebase" before the run`)
			c.Assert(got.Stderr, qt.Contains, `would outlive the run: sequence "rebase.ids";`)
			c.Assert(scopedRehearsalColumns(c, env.target), qt.Equals, "id")
			c.Assert(scopedRehearsalRebaseRelations(c, env.dev), qt.Equals, "bystander")
		})
	}
}

// TestScopedRehearsalRecreatesNothingForAStringDefault_HappyPathE2E is the
// control for the refusal above: a default that only spells the sequence's
// name as text, and a column comment that quotes a nextval call, need no
// sequence, so nothing is written into the schema the dev database held and
// the plan applies.
func TestScopedRehearsalRecreatesNothingForAStringDefault_HappyPathE2E(t *testing.T) {
	tc := scopedRehearsalCase{
		target: scopedSequenceIDs + "CREATE TABLE public.tasks (id int PRIMARY KEY);\n",
		desired: "CREATE TABLE public.tasks (id int PRIMARY KEY, label text DEFAULT 'rebase.ids');\n" +
			"COMMENT ON COLUMN public.tasks.label IS 'not nextval(''rebase.ids'')';\n",
		args: scopedRehearsalExcludeRebase,
		dev:  scopedSequenceHeldRebase,
	}
	for _, surface := range scopedRehearsalAppliers {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			env := newScopedRehearsalEnv(c, tc, scopedRehearsalBound)

			got := env.apply(c, surface, tc.args)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(scopedRehearsalDefaults(c, env.target), qt.Equals, "id=,label='rebase.ids'::text")
			c.Assert(scopedRehearsalRebaseRelations(c, env.dev), qt.Equals, "bystander")
		})
	}
}

// TestScopedRehearsalFindsABareSequenceNameAsTheTargetDoes_HappyPathE2E
// applies, on a realm pair whose current schemas differ, a default that names a
// sequence of the target's current schema without a schema, with that schema
// outside the selection. The target resolves nextval('ids') to the sequence of
// the role's own schema, and so must the rehearsal, whose dev session resolves
// names as the target does: the sequence is recreated there, not looked for in
// public.
func TestScopedRehearsalFindsABareSequenceNameAsTheTargetDoes_HappyPathE2E(t *testing.T) {
	desired := `CREATE TABLE "public"."notes" (id bigint PRIMARY KEY, body text NOT NULL, n bigint DEFAULT nextval('ids'));` + "\n"
	for _, surface := range scopedRehearsalAppliers {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newRealmCurrentSchemaFixture(c, realmNotes, "CREATE SEQUENCE ids")
			options := clirun.Options{Dir: realmCurrentSchemaRoot(c, desired), Env: environmentWithoutPtahVariables()}
			args := append(fixture.applyArgs(surface.target), "--include", "public.notes")

			first := clirun.Run(c, surface.target, options, args...)
			second := clirun.Run(c, surface.target, options, args...)

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
			c.Assert(first.Stdout, qt.Not(qt.Contains), "SEQUENCE")
			c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
			c.Assert(second.Stdout, qt.Contains, scopedRehearsalSynced)
			c.Assert(realmCurrentSchemaNames(c, fixture.target, `
				SELECT column_name || '=' || column_default FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = 'notes' AND column_default IS NOT NULL`),
				qt.DeepEquals, []string{"n=nextval('ids'::regclass)"})
			c.Assert(realmCurrentSchemaNames(c, fixture.target, `
				SELECT schemaname || '.' || sequencename FROM pg_sequences ORDER BY 1`),
				qt.DeepEquals, []string{fixture.role + ".ids"})
			c.Assert(fixture.rows(c), qt.DeepEquals, []string{"kept"})
			c.Assert(fixture.devSchemas(c), qt.DeepEquals, []string{"public"})
		})
	}
}

// scopedRehearsalDefaults lists the columns of public.tasks in database with
// their defaults as the server writes them back, name=default, comma-separated
// in column order; a column without a default is name=.
func scopedRehearsalDefaults(c *qt.C, database string) string {
	c.Helper()
	var columns string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(column_name || '=' || COALESCE(column_default, ''), ',' ORDER BY ordinal_position), '')
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'tasks'`).Scan(&columns)
	c.Assert(err, qt.IsNil)
	return columns
}

// scopedRehearsalSequences lists the sequences of database as schema.name,
// comma-separated in name order.
func scopedRehearsalSequences(c *qt.C, database string) string {
	c.Helper()
	var sequences string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(schemaname || '.' || sequencename, ',' ORDER BY schemaname, sequencename), '')
		FROM pg_sequences`).Scan(&sequences)
	c.Assert(err, qt.IsNil)
	return sequences
}

// scopedRehearsalRebaseRelations lists the relations of the rebase schema of
// database, comma-separated in name order.
func scopedRehearsalRebaseRelations(c *qt.C, database string) string {
	c.Helper()
	var relations string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(c.relname, ',' ORDER BY c.relname), '')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'rebase'`).Scan(&relations)
	c.Assert(err, qt.IsNil)
	return relations
}
