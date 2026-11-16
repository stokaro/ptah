//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
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
