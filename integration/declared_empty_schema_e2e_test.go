//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// declaredSchemaEngine is one engine whose schemas are namespaces inside one
// database. Each creates a declared schema and reads it back the same way.
type declaredSchemaEngine struct {
	name   string
	engine dbtarget.Engine
}

var declaredSchemaEngines = []declaredSchemaEngine{
	{name: "postgresql", engine: dbtarget.PostgreSQL},
	{name: "cockroachdb", engine: dbtarget.CockroachDB},
	{name: "yugabytedb", engine: dbtarget.YugabyteDB},
}

// declaredSchemaApp is the schema these tests declare. Each test has a database
// of its own.
const declaredSchemaApp = "es_app"

// declaredSchemaTarget is a database for one test.
func declaredSchemaTarget(c *qt.C, engine dbtarget.Engine) string {
	c.Helper()
	return newPolicyClauseDatabase(c, engine)
}

// writeDeclaredSchemaFile writes body to a file of that name and returns its path.
func writeDeclaredSchemaFile(c *qt.C, name, body string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// declaredSchemaHolds reads back from the catalog whether the database holds
// schema.
func declaredSchemaHolds(c *qt.C, database, schema string) bool {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, database))
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	var count int
	c.Assert(db.QueryRowContext(c.Context(),
		`SELECT count(*) FROM information_schema.schemata WHERE schema_name = $1`, schema).Scan(&count), qt.IsNil)
	return count == 1
}

// declaredSchemaExec runs statement on database.
func declaredSchemaExec(c *qt.C, database, statement string) {
	c.Helper()
	execPostgresFamilySQL(c, c.Context(), database, statement)
}

// declaredSchemaURL is target with its search_path pinned to schema, and target
// itself for no schema.
func declaredSchemaURL(c *qt.C, target, schema string) string {
	c.Helper()
	if schema == "" {
		return target
	}
	return planE2ESearchPath(c, target, schema)
}

// generatedMigrationVersion reads the version out of the one up file dir holds.
func generatedMigrationVersion(c *qt.C, dir string) string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	version, _, found := strings.Cut(filepath.Base(files[0]), "_")
	c.Assert(found, qt.IsTrue)
	return version
}

// TestDeclaredEmptySchemaIsCreatedE2E declares a schema with nothing in it, in
// HCL and in SQL, on each engine. The plan creates it, the catalog holds it, and
// the next plan is empty.
func TestDeclaredEmptySchemaIsCreatedE2E(t *testing.T) {
	documents := []struct {
		name string
		body func(schema string) string
	}{
		{name: "schema.hcl", body: func(schema string) string { return fmt.Sprintf("schema %q {\n}\n", schema) }},
		{name: "schema.sql", body: func(schema string) string { return fmt.Sprintf("CREATE SCHEMA %s;\n", schema) }},
	}
	for _, engine := range declaredSchemaEngines {
		for _, document := range documents {
			t.Run(engine.name+"/"+document.name, func(t *testing.T) {
				c := qt.New(t)
				schema := declaredSchemaApp
				target := declaredSchemaTarget(c, engine.engine)
				file := writeDeclaredSchemaFile(c, document.name, document.body(schema))
				c.Assert(declaredSchemaHolds(c, target, schema), qt.IsFalse)

				planned := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

				c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
				c.Assert(planned.Stdout, qt.Contains, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, schema))
				c.Assert(declaredSchemaHolds(c, target, schema), qt.IsFalse)

				applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(declaredSchemaHolds(c, target, schema), qt.IsTrue)

				settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

				c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
				c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			})
		}
	}
}

// TestDeclaredEmptySchemaTakesItsCommentAndItsPrivilegeE2E pins what the
// declaration says about the schema. A plan that grants a privilege on a schema
// it does not create is a GRANT the server refuses. The privilege and the comment
// are read back from the server.
func TestDeclaredEmptySchemaTakesItsCommentAndItsPrivilegeE2E(t *testing.T) {
	for _, engine := range declaredSchemaEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			schema := declaredSchemaApp
			target := declaredSchemaTarget(c, engine.engine)
			file := writeDeclaredSchemaFile(c, "schema.hcl", fmt.Sprintf(
				"schema %q {\n  comment = \"application\"\n}\n\npermission {\n  to = \"PUBLIC\"\n  for = schema.%s\n  privileges = [\"USAGE\"]\n}\n",
				schema, schema))

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			db, err := sql.Open("pgx", postgresFamilyDriverURL(c, target))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Assert(db.Close(), qt.IsNil) })
			var comment string
			var usage bool
			c.Assert(db.QueryRowContext(c.Context(), `SELECT obj_description(n.oid, 'pg_namespace'), has_schema_privilege('public', n.nspname, 'USAGE')
				FROM pg_namespace n WHERE n.nspname = $1`, schema).Scan(&comment, &usage), qt.IsNil)
			c.Assert(comment, qt.Equals, "application")
			c.Assert(usage, qt.IsTrue)

			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestDeclaredSchemaOtherFrontendsE2E holds the other two ways a declaration
// reaches the comparison to the same answer, on PostgreSQL.
func TestDeclaredSchemaOtherFrontendsE2E(t *testing.T) {
	c := qt.New(t)
	schema := declaredSchemaApp
	target := declaredSchemaTarget(c, dbtarget.PostgreSQL)
	dir := c.TempDir()
	body := fmt.Sprintf("package models\n\n//ptah:schema:schema name=%q\ntype Namespace struct{}\n", schema)
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(body), 0o600), qt.IsNil)

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--root-dir", dir, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(declaredSchemaHolds(c, target, schema), qt.IsTrue)
}

// TestDeclaredSchemaPlansNothingWhereItShouldNotE2E is the control. Each row is a
// schema the plan must leave alone, and the first three are other commits' rules:
// the schema the connection resolved (stokaro/ptah#5233), a schema the document
// records as not described (stokaro/ptah#5227), and the operator's own scope. The
// last two are the rule for a schema the document does not mention: Ptah drops
// nothing, where the community binary plans DROP SCHEMA ... CASCADE, so it stays.
func TestDeclaredSchemaPlansNothingWhereItShouldNotE2E(t *testing.T) {
	const app = "es_app"
	tests := []struct {
		name string
		// setup runs on the database first.
		setup []string
		// document is the schema file, and pin a schema for the URL's search_path.
		document string
		pin      string
		args     []string
		// wantHolds is whether the catalog holds app afterward.
		wantHolds bool
	}{
		{
			name:      "declared and held",
			setup:     []string{"CREATE SCHEMA es_app"},
			document:  `schema "es_app" {` + "\n}\n",
			wantHolds: true,
		},
		{
			name:      "the schema the connection resolved",
			setup:     []string{"CREATE SCHEMA es_app"},
			document:  `schema "es_app" {` + "\n}\n",
			pin:       app,
			wantHolds: true,
		},
		{
			name:     "public, which the connection resolved",
			document: `schema "public" {` + "\n}\n",
		},
		{
			name:     "recorded as not described, and declared",
			document: `// ptah:not-described schema "es_app"` + "\n\n" + `schema "es_app" {` + "\n}\n",
		},
		{
			name:     "outside the schemas the operator selected",
			document: `schema "public" {` + "\n}\n\n" + `schema "es_app" {` + "\n}\n",
			args:     []string{"--schemas", "public"},
		},
		{
			name:      "held and empty, and the document does not mention it",
			setup:     []string{"CREATE SCHEMA es_app"},
			document:  `schema "public" {` + "\n}\n",
			wantHolds: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := declaredSchemaTarget(c, dbtarget.PostgreSQL)
			for _, statement := range test.setup {
				declaredSchemaExec(c, target, statement)
			}
			args := append([]string{"schema", "apply", "--db-url", declaredSchemaURL(c, target, test.pin), "--auto-approve",
				"--schema-file", writeDeclaredSchemaFile(c, "schema.hcl", test.document)}, test.args...)

			got := runPtahIn(c, args...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(declaredSchemaHolds(c, target, app), qt.Equals, test.wantHolds)
		})
	}
}

// TestDeclaredEmptySchemaSurvivesAnInspectE2E reads a database holding an empty
// schema into each form a schema takes, and plans from it against the same
// database. Declared and held is synced, on each engine, and the schema stays.
func TestDeclaredEmptySchemaSurvivesAnInspectE2E(t *testing.T) {
	for _, engine := range declaredSchemaEngines {
		t.Run(engine.name+"/hcl", func(t *testing.T) {
			c := qt.New(t)
			target := declaredSchemaTarget(c, engine.engine)
			declaredSchemaExec(c, target, "CREATE SCHEMA es_app")

			inspected := runPtahIn(c, "schema", "inspect", "--db-url", target)

			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			c.Assert(inspected.Stdout, qt.Contains, `schema "es_app"`)
			planned := runPtahIn(c, "schema", "apply", "--db-url", target, "--dry-run",
				"--schema-file", writeDeclaredSchemaFile(c, "schema.hcl", inspected.Stdout))
			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
		t.Run(engine.name+"/sql", func(t *testing.T) {
			c := qt.New(t)
			target := declaredSchemaTarget(c, engine.engine)
			declaredSchemaExec(c, target, "CREATE SCHEMA es_app")

			inspected := runPtahIn(c, "schema", "inspect", "--db-url", target, "--format", "sql")

			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			planned := runPtahIn(c, "schema", "apply", "--db-url", target, "--dry-run",
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", inspected.Stdout))
			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
		t.Run(engine.name+"/compat hcl", func(t *testing.T) {
			c := qt.New(t)
			target := declaredSchemaTarget(c, engine.engine)
			declaredSchemaExec(c, target, "CREATE SCHEMA es_app")

			inspected := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "schema", "inspect", "-u", target)

			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			document := writeDeclaredSchemaFile(c, "schema.hcl", inspected.Stdout)
			planned := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
				"schema", "apply", "-u", target, "--to", "file://"+filepath.ToSlash(document), "--dry-run")
			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestDeclaredEmptySchemaRollsBackE2E generates the migration that creates a
// schema. Its down file drops the schema, read back from the catalog, because the
// comparison established that the database did not hold it.
func TestDeclaredEmptySchemaRollsBackE2E(t *testing.T) {
	for _, engine := range declaredSchemaEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := declaredSchemaTarget(c, engine.engine)
			dir := c.TempDir()
			file := writeDeclaredSchemaFile(c, "schema.hcl", `schema "es_app" {`+"\n}\n")

			generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", file, "--migrations-dir", dir)

			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			c.Assert(readFirstMatchingFile(c, dir, "*.down.sql"), qt.Contains, `DROP SCHEMA "es_app"`)

			up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir)

			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(declaredSchemaHolds(c, target, "es_app"), qt.IsTrue)

			down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

			c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
			c.Assert(declaredSchemaHolds(c, target, "es_app"), qt.IsFalse)
		})
	}
}

// TestDeclaredEmptySchemaRollbackStopsAtATableE2E puts a table in the schema after
// the migration that created it. The drop has no CASCADE, so the rollback stops
// and the table survives. Once the operator moves the table, the repair the status
// names finishes the rollback.
func TestDeclaredEmptySchemaRollbackStopsAtATableE2E(t *testing.T) {
	for _, engine := range declaredSchemaEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := declaredSchemaTarget(c, engine.engine)
			dir := c.TempDir()
			file := writeDeclaredSchemaFile(c, "schema.hcl", `schema "es_app" {`+"\n}\n")
			generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", file, "--migrations-dir", dir)
			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir)
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			declaredSchemaExec(c, target, "CREATE TABLE es_app.since (id integer PRIMARY KEY)")

			stopped := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

			c.Assert(stopped.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stopped.Stdout, stopped.Stderr))
			c.Assert(declaredSchemaHolds(c, target, "es_app"), qt.IsTrue)
			declaredSchemaExec(c, target, "DROP TABLE es_app.since")

			repaired := runPtahIn(c, "migrations", "repair", "--db-url", target, "--migrations-dir", dir,
				"--version", generatedMigrationVersion(c, dir), "--resume-from", "1")

			c.Assert(repaired.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", repaired.Stdout, repaired.Stderr))
			c.Assert(declaredSchemaHolds(c, target, "es_app"), qt.IsFalse)
		})
	}
}

// TestDeclaredEmptySchemaIsRehearsedE2E takes both ways an empty schema reaches a
// rehearsal on a dev server the run does not own. A plan that creates one runs
// there. A target that holds one, with a comment and a privilege, has it rebuilt
// first, and the document leaves out a table so that the plan is not empty and a
// rehearsal happens. Without the schema's creation, that rebuild's GRANT fails on
// the dev database. The dev database holds neither afterward.
func TestDeclaredEmptySchemaIsRehearsedE2E(t *testing.T) {
	const declared = "schema \"public\" {\n}\n\nschema \"es_app\" {\n  comment = \"application\"\n}\n"
	tests := []struct {
		name    string
		setup   []string
		wantOut string
	}{
		{name: "a plan that creates it", wantOut: `CREATE SCHEMA IF NOT EXISTS "es_app"`},
		{
			name: "a target that holds it",
			setup: []string{
				"CREATE SCHEMA es_app", "COMMENT ON SCHEMA es_app IS 'application'", "GRANT USAGE ON SCHEMA es_app TO PUBLIC",
				"CREATE TABLE docs (id integer PRIMARY KEY)",
			},
			wantOut: `DROP TABLE "docs"`,
		},
	}
	for _, surface := range devSchemaStateSurfaces {
		for _, test := range tests {
			t.Run(surface.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := newEmptyReadScopeDatabase(c, "declared_target")
				dev := newEmptyReadScopeDatabase(c, "declared_dev")
				for _, statement := range test.setup {
					declaredSchemaExec(c, target, statement)
				}
				document := writeDeclaredSchemaFile(c, "schema.hcl", declared)
				claimed := devPublicSchemaState(c, dev)

				got := clirun.Run(c, surface.target, clirun.Options{Dir: c.TempDir()}, surface.apply(target, document, dev)...)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stdout, qt.Contains, test.wantOut)
				c.Assert(declaredSchemaHolds(c, target, "es_app"), qt.IsTrue)
				c.Assert(declaredSchemaHolds(c, dev, "es_app"), qt.IsFalse)
				c.Assert(devPublicSchemaState(c, dev), qt.Equals, claimed)
			})
		}
	}
}

// TestDeclaredSchemaOnTheCommunityBinaryE2E holds the measurements behind the
// compatibility surface's answers, on one document at a time. The two binaries
// agree on a schema the document declares and the database lacks, and on one it
// declares and holds, and on a document naming a schema a pinned URL cannot reach.
// They differ on a schema the document does not mention, deliberately: the binary
// drops it with everything in it, and ptah-compat keeps it.
func TestDeclaredSchemaOnTheCommunityBinaryE2E(t *testing.T) {
	const appBlock = `schema "es_app" {` + "\n}\n"
	const publicBlock = `schema "public" {` + "\n}\n"
	tests := []struct {
		name     string
		setup    []string
		pin      string
		document string
		// wantExit is what both binaries exit with.
		wantExit int
		// wantCommunity and wantCompat are what each binary answers.
		wantCommunity, wantCompat string
	}{
		{
			name: "declared and absent", document: publicBlock + appBlock,
			wantCommunity: `CREATE SCHEMA "es_app"`, wantCompat: `CREATE SCHEMA IF NOT EXISTS "es_app"`,
		},
		{
			name: "declared and held", setup: []string{"CREATE SCHEMA es_app"}, document: publicBlock + appBlock,
			wantCommunity: "synced", wantCompat: "synced",
		},
		{
			name: "a pinned URL that cannot reach it", pin: "public", document: appBlock, wantExit: 1,
			wantCommunity: `mismatched HCL and database schemas: "public" <> "es_app"`,
			wantCompat:    `mismatched HCL and database schemas: "public" <> "es_app"`,
		},
		{
			name: "held and not mentioned", setup: []string{"CREATE SCHEMA es_app"}, document: publicBlock,
			wantCommunity: `DROP SCHEMA "es_app" CASCADE`, wantCompat: "synced",
		},
	}
	reference := atlasReferenceBinary(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newEmptyReadScopeDatabase(c, "declared_reference")
			for _, statement := range test.setup {
				declaredSchemaExec(c, target, statement)
			}
			to := "file://" + filepath.ToSlash(writeDeclaredSchemaFile(c, "schema.hcl", test.document))
			args := []string{"schema", "apply", "-u", declaredSchemaURL(c, target, test.pin), "--to", to, "--dry-run"}

			community, err := exec.CommandContext(c.Context(), reference, args...).CombinedOutput() // #nosec G204 G702 -- operator-provided pinned reference binary
			compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, args...)

			c.Assert(exitCodeOf(err), qt.Equals, test.wantExit, qt.Commentf("%s", community))
			c.Assert(string(community), qt.Contains, test.wantCommunity)
			c.Assert(compat.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
			c.Assert(compat.Stdout+compat.Stderr, qt.Contains, test.wantCompat)
		})
	}
}

// TestDeclaredDatabaseOnAMySQLServerE2E pins the answer on an engine where a schema
// is a database, which the URL decides. A URL naming the database finds it synced,
// a URL naming another refuses the document, and a URL naming no database plans the
// whole server, so it creates the database with nothing in it. The catalog says
// which databases the server holds.
func TestDeclaredDatabaseOnAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name+"/a URL naming it", func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			name, url := scratch.database(c, "declared")
			file := writeDeclaredSchemaFile(c, "schema.hcl", fmt.Sprintf("schema %q {\n}\n", name))

			got := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, "--dry-run")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
		t.Run(engine.name+"/a URL naming another", func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			_, url := scratch.database(c, "declared")
			absent := fmt.Sprintf("ptah_declared_new_%d", time.Now().UnixNano())
			file := writeDeclaredSchemaFile(c, "schema.hcl", fmt.Sprintf("schema %q {\n}\n", absent))

			got := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, "--auto-approve")

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, fmt.Sprintf("the document declares database %q", absent))
			c.Assert(databaseCollation(c, scratch, absent), qt.Equals, "")
		})
		t.Run(engine.name+"/a URL naming no database", func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			server, err := atlasurl.WithDatabaseName(scratch.adminURL, "")
			c.Assert(err, qt.IsNil)
			absent := fmt.Sprintf("ptah_declared_new_%d", time.Now().UnixNano())
			c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), scratch.admin, absent) })
			file := writeDeclaredSchemaFile(c, "schema.hcl", fmt.Sprintf("schema %q {\n}\n", absent))

			applied := runPtahIn(c, "schema", "apply", "--db-url", server, "--schema-file", file, "--schemas", absent, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(databaseCollation(c, scratch, absent), qt.Not(qt.Equals), "")

			settled := runPtahIn(c, "schema", "apply", "--db-url", server, "--schema-file", file, "--schemas", absent, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// exitCodeOf is the exit code an exec error carries, and 0 for none.
func exitCodeOf(err error) int {
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return exit.ExitCode()
	}
	return 0
}
