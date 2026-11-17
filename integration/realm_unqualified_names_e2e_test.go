//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// The tests in this file run on the realm pair of
// [newRealmCurrentSchemaFixture]: the target resolves an unqualified name to
// the role's schema and the dev database resolves it to public. The pinned
// community binary v1.3.0 reads the desired state on the dev database, so an
// unqualified name means public there, and every statement it plans names the
// schema. Measured on PostgreSQL 18 with that layout: `CREATE TABLE notes`
// beside an existing public.notes plans nothing for the table, and a new
// unqualified table is created as "public"."fresh" by `schema apply`, `schema
// diff` and `migrate diff` alike.

// realmUnqualifiedSchema writes notes, an index on it, a new table and a view
// without a schema. public.notes is what [realmNotes] creates.
const realmUnqualifiedSchema = "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL);\n" +
	"CREATE INDEX notes_body ON notes (body);\n" +
	"CREATE TABLE fresh (id bigint PRIMARY KEY);\n" +
	"CREATE VIEW recent AS SELECT id FROM notes;\n"

// relations lists the tables and views of the target that
// [realmUnqualifiedSchema] names, as schema.name, in order. The log native
// `ptah schema apply` keeps is not one of them.
func (f realmCurrentSchemaFixture) relations(c *qt.C) []string {
	c.Helper()
	return realmCurrentSchemaNames(c, f.target, `
		SELECT table_schema || '.' || table_name
		FROM information_schema.tables
		WHERE table_name IN ('notes', 'fresh', 'recent')
		ORDER BY 1`)
}

// indexes lists every index of the target named notes_body, as schema.index.
func (f realmCurrentSchemaFixture) indexes(c *qt.C) []string {
	c.Helper()
	return realmCurrentSchemaNames(c, f.target,
		"SELECT schemaname || '.' || indexname FROM pg_indexes WHERE indexname = 'notes_body' ORDER BY 1")
}

// rowsIn lists the bodies of the notes table in schema, in id order.
func (f realmCurrentSchemaFixture) rowsIn(c *qt.C, schema string) []string {
	c.Helper()
	return realmCurrentSchemaNames(c, f.target, "SELECT body FROM "+quoteE2EIdent(schema)+".notes ORDER BY id")
}

// TestSchemaApplyPlacesAnUnqualifiedNameWhereTheDevDatabaseResolvesIt_HappyPathE2E
// applies [realmUnqualifiedSchema] to a target that holds public.notes with a
// row. The names are read where the dev database resolves them, so notes is
// the table the target holds and keeps its row, the index is created on it,
// and the new table and the view land in public rather than in the role's
// schema. A second apply plans nothing. The target is read back for each.
func TestSchemaApplyPlacesAnUnqualifiedNameWhereTheDevDatabaseResolvesIt_HappyPathE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		target clirun.Target
	}{
		{name: "ptah-compat", target: clirun.Compat},
		{name: "ptah", target: clirun.Ptah},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newRealmCurrentSchemaFixture(c, realmNotes)
			root := realmCurrentSchemaRoot(c, realmUnqualifiedSchema)
			options := clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}

			applied := clirun.Run(c, test.target, options, fixture.applyArgs(test.target)...)
			again := clirun.Run(c, test.target, options, fixture.applyArgs(test.target)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(fixture.relations(c), qt.DeepEquals, []string{"public.fresh", "public.notes", "public.recent"})
			c.Assert(fixture.indexes(c), qt.DeepEquals, []string{"public.notes_body"})
			c.Assert(fixture.rows(c), qt.DeepEquals, []string{"kept"})
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made")
		})
	}
}

// TestSchemaApplyRefusesAnUnqualifiedTableItsTwoReadingsPlaceApart_FailurePathE2E
// applies `CREATE TABLE notes` where the plan would create the table in one
// reading of the name and drop the table the other reading names, with its
// row. With a dev database the name is read as public, and the target holds
// notes in its current schema; the pinned community binary v1.3.0 plans that
// move, and drops the role's whole schema with it. Without one the name is read
// as the role's schema, and the target holds public.notes, where the community
// binary reads it. The apply is refused, and the row is read back where it was.
func TestSchemaApplyRefusesAnUnqualifiedTableItsTwoReadingsPlaceApart_FailurePathE2E(t *testing.T) {
	const fromDev = `the desired schema names table "notes" without a schema, which the dev database resolves to "public" ` +
		`and this connection to its current schema "{role}", and the plan would create it in "public" and drop ` +
		`"{role}"."notes" with its rows; write the table as "{role}"."notes" to keep it, or as "public"."notes" to move it`
	const withoutDev = `the desired schema names table "notes" without a schema, which this connection resolves to its ` +
		`current schema "{role}", and the plan would create it there and drop "public"."notes" with its rows; ` +
		`write the table as "public"."notes", or pin search_path in the URL`
	const currentNotes = "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL); INSERT INTO notes VALUES (1, 'kept')"
	for _, test := range []struct {
		name     string
		target   clirun.Target
		args     func(realmCurrentSchemaFixture, clirun.Target) []string
		seed     string
		schema   string
		wantExit int
		want     string
	}{
		{
			name:     "ptah-compat, the current schema holds the table",
			target:   clirun.Compat,
			args:     realmCurrentSchemaFixture.applyArgs,
			seed:     currentNotes,
			schema:   "{role}",
			wantExit: 1,
			want:     "Error: " + fromDev,
		},
		{
			name:     "ptah, the current schema holds the table",
			target:   clirun.Ptah,
			args:     realmCurrentSchemaFixture.applyArgs,
			seed:     currentNotes,
			schema:   "{role}",
			wantExit: 2,
			want:     "error: " + fromDev,
		},
		{
			name:     "ptah without a dev database, public holds the table",
			target:   clirun.Ptah,
			args:     realmCurrentSchemaFixture.applyArgsWithoutDev,
			seed:     realmNotes,
			schema:   "public",
			wantExit: 2,
			want:     "error: " + withoutDev,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newRealmCurrentSchemaFixture(c, test.seed)
			root := realmCurrentSchemaRoot(c, "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL);\n")
			role := strings.NewReplacer("{role}", fixture.role)
			schema := role.Replace(test.schema)

			result := clirun.Run(c, test.target, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
				test.args(fixture, test.target)...)

			c.Assert(result.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stderr, qt.Contains, role.Replace(test.want))
			c.Assert(fixture.columns(c), qt.DeepEquals, []string{schema + ".id", schema + ".body"})
			c.Assert(fixture.rowsIn(c, schema), qt.DeepEquals, []string{"kept"})
		})
	}
}

// TestSchemaDiffPlacesAnUnqualifiedNameWhereTheDevDatabaseResolvesIt_HappyPathE2E
// diffs the target that holds public.notes against a file naming notes and a
// new table without a schema. The diff creates the new table in public and
// leaves notes alone, where reading the names as the role's schema would create
// notes there and drop public.notes. The community binary prints the same
// statements and, in front of them, DROP SCHEMA for the role's schema, which
// its desired state does not hold; Ptah drops no schema the file leaves out.
func TestSchemaDiffPlacesAnUnqualifiedNameWhereTheDevDatabaseResolvesIt_HappyPathE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		target clirun.Target
		to     string
		want   string
	}{
		{
			name:   "ptah-compat",
			target: clirun.Compat,
			to:     "file://schema.sql",
			want: "-- Create \"fresh\" table\n" +
				"CREATE TABLE \"public\".\"fresh\" (\n  \"id\" bigint NOT NULL,\n  PRIMARY KEY (\"id\")\n);\n",
		},
		{
			name:   "ptah",
			target: clirun.Ptah,
			to:     "schema.sql",
			want:   "CREATE TABLE \"public\".\"fresh\" (\n  \"id\" bigint PRIMARY KEY NOT NULL\n);\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newRealmCurrentSchemaFixture(c, realmNotes)
			root := realmCurrentSchemaRoot(c, "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL);\n"+
				"CREATE TABLE fresh (id bigint PRIMARY KEY);\n")

			result := clirun.Run(c, test.target, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
				"schema", "diff", "--from", fixture.targetURL, "--to", test.to, "--dev-url", fixture.devURL)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stdout, qt.Equals, test.want)
			c.Assert(fixture.tables(c), qt.DeepEquals, []string{"public.notes"})
		})
	}
}

// TestMigrateDiffQualifiesAnUnqualifiedName_HappyPathE2E writes a migration
// for a table the desired state names without a schema, on a dev database
// that describes every schema of its database. The file names the schema the
// dev database resolves the name to, as the community binary's file does, so
// applying it does not depend on the search path of whoever runs it. A second
// run against the directory finds nothing to write.
func TestMigrateDiffQualifiesAnUnqualifiedName_HappyPathE2E(t *testing.T) {
	for _, test := range []struct {
		name       string
		target     clirun.Target
		first      []string
		second     []string
		file       string
		wantSynced string
	}{
		{
			name:       "ptah-compat migrate diff",
			target:     clirun.Compat,
			first:      []string{"migrate", "diff", "init", "--dir", "file://migrations", "--to", "file://schema.sql"},
			second:     []string{"migrate", "diff", "again", "--dir", "file://migrations", "--to", "file://schema.sql"},
			file:       "*_init.sql",
			wantSynced: "The migration directory is synced with the desired state, no changes to be made",
		},
		{
			name:   "ptah migrations generate --replay",
			target: clirun.Ptah,
			first: []string{"migrations", "generate", "--replay", "--schema-file", "schema.sql",
				"--migrations-dir", "migrations", "--name", "init"},
			second: []string{"migrations", "generate", "--replay", "--schema-file", "schema.sql",
				"--migrations-dir", "migrations", "--name", "again"},
			file:       "*_init.up.sql",
			wantSynced: "no migration files generated",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newRealmCurrentSchemaFixture(c)
			root := realmCurrentSchemaRoot(c, "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL);\n")
			c.Assert(os.Mkdir(filepath.Join(root, "migrations"), 0o750), qt.IsNil)
			options := clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}

			first := clirun.Run(c, test.target, options, append(test.first, "--dev-url", fixture.devURL)...)
			second := clirun.Run(c, test.target, options, append(test.second, "--dev-url", fixture.devURL)...)

			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
			written, err := filepath.Glob(filepath.Join(root, "migrations", test.file))
			c.Assert(err, qt.IsNil)
			c.Assert(written, qt.HasLen, 1)
			content, err := os.ReadFile(written[0])
			c.Assert(err, qt.IsNil)
			c.Assert(string(content), qt.Contains, `CREATE TABLE "public"."notes" (`)
			c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", second.Stdout, second.Stderr))
			c.Assert(second.Stdout, qt.Contains, test.wantSynced)
		})
	}
}

// TestSchemaApplyLeavesAnUndeclaredTriggerFunctionWhereTheSessionFindsIt_HappyPathE2E
// applies a table and a trigger that runs suppress_redundant_updates_trigger,
// a built-in function the desired state does not declare. The table is placed
// where the dev database resolves its name, and the function is not: it lives
// in pg_catalog, so a trigger naming it in public would not be created. The
// trigger is read back running the pg_catalog function, and a second apply
// plans nothing.
func TestSchemaApplyLeavesAnUndeclaredTriggerFunctionWhereTheSessionFindsIt_HappyPathE2E(t *testing.T) {
	const schema = "CREATE TABLE docs (id bigint PRIMARY KEY, body text);\n" +
		"CREATE TRIGGER nored BEFORE UPDATE ON docs FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger();\n"
	for _, test := range []struct {
		name   string
		target clirun.Target
	}{
		{name: "ptah-compat", target: clirun.Compat},
		{name: "ptah", target: clirun.Ptah},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newRealmCurrentSchemaFixture(c)
			options := clirun.Options{Dir: realmCurrentSchemaRoot(c, schema), Env: environmentWithoutPtahVariables()}

			applied := clirun.Run(c, test.target, options, fixture.applyArgs(test.target)...)
			again := clirun.Run(c, test.target, options, fixture.applyArgs(test.target)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(realmCurrentSchemaNames(c, fixture.target, `
				SELECT c.relnamespace::regnamespace::text || '.' || c.relname || ' ' || p.pronamespace::regnamespace::text || '.' || p.proname
				FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_proc p ON p.oid = t.tgfoid
				WHERE t.tgname = 'nored'`), qt.DeepEquals, []string{"public.docs pg_catalog.suppress_redundant_updates_trigger"})
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made")
		})
	}
}

// TestSchemaApplyGrantsOnTheSequenceOfAPlacedSerialColumn_HappyPathE2E grants
// on the sequence a serial column creates, named without a schema. The table
// is placed in public, and its sequence is created beside it, so the grant is
// placed there too; left bare, it resolved along the role's search path to a
// sequence that does not exist. The grant is read back, and a second run is
// synced.
func TestSchemaApplyGrantsOnTheSequenceOfAPlacedSerialColumn_HappyPathE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		target clirun.Target
	}{
		{name: "ptah-compat", target: clirun.Compat},
		{name: "ptah", target: clirun.Ptah},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			grantee := newRealmGrantee(c)
			fixture := newRealmCurrentSchemaFixture(c)
			schema := "CREATE TABLE notes (id serial PRIMARY KEY);\n" +
				"GRANT USAGE ON SEQUENCE notes_id_seq TO " + grantee + ";\n"
			options := clirun.Options{Dir: realmCurrentSchemaRoot(c, schema), Env: environmentWithoutPtahVariables()}

			applied := clirun.Run(c, test.target, options, fixture.applyArgs(test.target)...)
			again := clirun.Run(c, test.target, options, fixture.applyArgs(test.target)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(realmCurrentSchemaNames(c, fixture.target, `
				SELECT c.relnamespace::regnamespace::text || '.' || c.relname
				FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a
				WHERE c.relkind = 'S' AND a.grantee = '`+grantee+`'::regrole AND a.privilege_type = 'USAGE'`),
				qt.DeepEquals, []string{"public.notes_id_seq"})
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made")
		})
	}
}

// newRealmGrantee creates a role a desired schema grants to, and drops it when
// the test ends. Called before the databases are created, its drop runs after
// theirs, once no database holds a grant to it.
func newRealmGrantee(c *qt.C) string {
	c.Helper()
	admin, err := sql.Open("pgx", dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	role := fmt.Sprintf("ptah_realm_grantee_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+role)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(dropErr, qt.IsNil)
	})
	return role
}

// TestSchemaApplyPlacesASavedPlansNamesWhereThePlanPlacedThem_HappyPathE2E
// saves a native JSON plan of an unqualified table and applies it with a dev
// database other than the plan's, or with none. The plan records the schema it
// placed the name in, and the rehearsal and the end-state verification place
// it there too, so neither reports the plan's own result as drift. The target
// is read back.
func TestSchemaApplyPlacesASavedPlansNamesWhereThePlanPlacedThem_HappyPathE2E(t *testing.T) {
	for _, test := range []struct {
		name     string
		planDev  bool
		applyDev bool
		// wantTable is schema.table, with {role} standing for the role's
		// schema.
		wantTable string
	}{
		{name: "planned on the dev database, applied without one", planDev: true, applyDev: false,
			wantTable: "public.fresh"},
		{name: "planned without a dev database, applied with one", planDev: false, applyDev: true,
			wantTable: "{role}.fresh"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newRealmCurrentSchemaFixture(c)
			options := clirun.Options{Dir: realmCurrentSchemaRoot(c, "CREATE TABLE fresh (id bigint PRIMARY KEY);\n"), Env: environmentWithoutPtahVariables()}
			dev := map[bool][]string{true: {"--dev-url", fixture.devURL}, false: nil}

			planned := clirun.Run(c, clirun.Compat, options, append([]string{"schema", "plan", "--from", fixture.targetURL,
				"--to", "file://schema.sql", "--output", "realm.plan.json"}, dev[test.planDev]...)...)
			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			applied := clirun.Run(c, clirun.Compat, options, append([]string{"schema", "apply", "--url", fixture.targetURL,
				"--to", "file://schema.sql", "--plan", "file://realm.plan.json", "--auto-approve"}, dev[test.applyDev]...)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(realmCurrentSchemaNames(c, fixture.target,
				"SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_name = 'fresh'"),
				qt.DeepEquals, []string{strings.ReplaceAll(test.wantTable, "{role}", fixture.role)})
		})
	}
}

// TestSchemaApplyRefusesAnUnqualifiedSequenceItsTwoReadingsPlaceApart_FailurePathE2E
// applies `CREATE SEQUENCE s1` where the target holds s1 in its current
// schema, at 42, and the dev database resolves the name to public. The plan
// would create public.s1 and drop the role's s1 with its value, so the apply
// is refused, and the value is read back where it was.
func TestSchemaApplyRefusesAnUnqualifiedSequenceItsTwoReadingsPlaceApart_FailurePathE2E(t *testing.T) {
	const want = `the desired schema names sequence "s1" without a schema, which the dev database resolves to "public" ` +
		`and this connection to its current schema "{role}", and the plan would create it in "public" and drop ` +
		`"{role}"."s1" with its current value; write the sequence as "{role}"."s1" to keep it, or as "public"."s1" to move it`
	for _, test := range []struct {
		name     string
		target   clirun.Target
		wantExit int
		prefix   string
	}{
		{name: "ptah-compat", target: clirun.Compat, wantExit: 1, prefix: "Error: "},
		{name: "ptah", target: clirun.Ptah, wantExit: 2, prefix: "error: "},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newRealmCurrentSchemaFixture(c, "CREATE SEQUENCE s1", "SELECT setval('s1', 42)")
			root := realmCurrentSchemaRoot(c, "CREATE SEQUENCE s1;\n")
			role := strings.NewReplacer("{role}", fixture.role)

			result := clirun.Run(c, test.target, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
				fixture.applyArgs(test.target)...)

			c.Assert(result.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stderr, qt.Contains, role.Replace(test.prefix+want))
			c.Assert(realmCurrentSchemaNames(c, fixture.target,
				"SELECT schemaname || '.' || sequencename || ' ' || last_value FROM pg_sequences WHERE sequencename = 's1'"),
				qt.DeepEquals, []string{fixture.role + ".s1 42"})
		})
	}
}

// TestSchemaApplyRendersASequenceOwnerInTheSequencesSchema_HappyPathE2E
// adds a column beside a sequence in schema app that a column of app.t owns.
// The read writes the owner as t.id, without the schema it has to share with
// the sequence, and the rehearsal recreates the sequence from the read with
// the owner in app: rendered bare, the server resolves t along the session's
// search path, which does not reach app, and the rehearsal fails with
// SQLSTATE 42P01. The owner is read back from the catalog, and a second apply
// plans nothing.
func TestSchemaApplyRendersASequenceOwnerInTheSequencesSchema_HappyPathE2E(t *testing.T) {
	const schema = "CREATE SCHEMA app;\n" +
		"CREATE TABLE app.t (id bigint PRIMARY KEY, note text);\n" +
		"CREATE SEQUENCE app.s OWNED BY app.t.id;\n"
	for _, test := range []struct {
		name   string
		target clirun.Target
	}{
		{name: "ptah-compat", target: clirun.Compat},
		{name: "ptah", target: clirun.Ptah},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newRealmCurrentSchemaFixture(c, "CREATE SCHEMA app", "CREATE TABLE app.t (id bigint PRIMARY KEY)",
				"CREATE SEQUENCE app.s OWNED BY app.t.id")
			options := clirun.Options{Dir: realmCurrentSchemaRoot(c, schema), Env: environmentWithoutPtahVariables()}

			applied := clirun.Run(c, test.target, options, fixture.applyArgs(test.target)...)
			again := clirun.Run(c, test.target, options, fixture.applyArgs(test.target)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(realmCurrentSchemaNames(c, fixture.target, `
				SELECT s.relnamespace::regnamespace::text || '.' || s.relname || ' ' || t.relnamespace::regnamespace::text || '.' || t.relname
				FROM pg_depend d JOIN pg_class s ON s.oid = d.objid JOIN pg_class t ON t.oid = d.refobjid
				WHERE s.relkind = 'S' AND d.deptype = 'a' AND s.relname = 's'`), qt.DeepEquals, []string{"app.s app.t"})
			c.Assert(realmCurrentSchemaNames(c, fixture.target,
				"SELECT table_schema || '.' || column_name FROM information_schema.columns WHERE table_name = 't' ORDER BY ordinal_position"),
				qt.DeepEquals, []string{"app.id", "app.note"})
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made")
		})
	}
}
