//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// viewOwnerEngine is one engine of the PostgreSQL family whose view owner Ptah
// manages. The engines answer alike about what the owner does: a view reads
// its tables with its owner's rights, so the reader sees what the owner's
// row-level security policy shows, measured on PostgreSQL 18, CockroachDB 26.4
// and YugabyteDB 2026.1. They differ in how they store a quoted role name:
// CockroachDB folds it, and a role name there holds no space.
type viewOwnerEngine struct {
	name   string
	engine dbtarget.Engine
	// auditors is the quoted role a schema names, with %d for the test's
	// suffix, and storedAuditors is the name the server stores for it.
	auditors, storedAuditors string
}

var viewOwnerEngines = []viewOwnerEngine{
	{name: "postgresql", engine: dbtarget.PostgreSQL, auditors: `"Ptah Auditors %d"`, storedAuditors: "Ptah Auditors %d"},
	{name: "cockroachdb", engine: dbtarget.CockroachDB, auditors: `"Ptah_Auditors_%d"`, storedAuditors: "ptah_auditors_%d"},
	{name: "yugabytedb", engine: dbtarget.YugabyteDB, auditors: `"Ptah Auditors %d"`, storedAuditors: "Ptah Auditors %d"},
}

const (
	viewOwnerBody   = "SELECT id, body FROM docs"
	viewOwnerNarrow = "SELECT id FROM docs"
)

// viewOwnerTarget is a database holding the table docs with row-level
// security and two rows, a policy that shows the role {owner} one of them, the
// view my_docs, which the role {reader} reads, and the materialized view "Doc
// Totals". The roles are the test's own and are dropped when it ends.
type viewOwnerTarget struct {
	url string
	db  *sql.DB
	// names maps each placeholder a fixture, a schema or an expectation
	// writes to what it stands for. {auditors} is the quoted role as a schema
	// names it, and {auditors.stored} the name the server stores for it.
	names map[string]string
}

// newViewOwnerTarget creates the database and the roles, and runs held after
// the fixture, one statement each: YugabyteDB answered a conflict to a fixture
// sent as one call and kept part of it.
func newViewOwnerTarget(c *qt.C, engine viewOwnerEngine, held ...string) viewOwnerTarget {
	c.Helper()
	adminURL := dbtarget.URL(c, engine.engine)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
	suffix := time.Now().UnixNano()
	target := viewOwnerTarget{names: map[string]string{
		"{owner}":           fmt.Sprintf("ptah_vo_owner_%d", suffix),
		"{other}":           fmt.Sprintf("ptah_vo_other_%d", suffix),
		"{reader}":          fmt.Sprintf("ptah_vo_reader_%d", suffix),
		"{auditors}":        fmt.Sprintf(engine.auditors, suffix),
		"{auditors.stored}": fmt.Sprintf(engine.storedAuditors, suffix),
	}}
	var connected string
	c.Assert(admin.QueryRowContext(c.Context(), "SELECT current_user").Scan(&connected), qt.IsNil)
	target.names["{admin}"] = connected
	for _, role := range []string{"{owner}", "{other}", "{reader}", "{auditors}"} {
		_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+target.names[role])
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() {
			_, err := admin.ExecContext(context.Background(), "DROP ROLE "+target.names[role])
			c.Assert(err, qt.IsNil)
		})
	}
	name := fmt.Sprintf("ptah_view_owner_%d", suffix)
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })

	target.url = replaceDatabaseName(c, adminURL, name)
	fixture := []string{
		"CREATE TABLE docs (id integer PRIMARY KEY, body text NOT NULL)",
		"INSERT INTO docs VALUES (1, 'a'), (2, 'b')",
		"ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
		"CREATE POLICY docs_owner_sees_one ON docs FOR SELECT TO {owner} USING (id = 1)",
		"GRANT SELECT ON docs TO {owner}, {other}, {auditors}",
		"CREATE VIEW my_docs AS " + viewOwnerBody,
		"GRANT SELECT ON my_docs TO {reader}",
		`CREATE MATERIALIZED VIEW "Doc Totals" AS SELECT count(*) AS total FROM docs`,
	}
	for _, statement := range append(fixture, held...) {
		execPostgresFamilySQL(c, c.Context(), target.url, target.expand(statement))
	}
	target.db, err = sql.Open("pgx", postgresFamilyDriverURL(c, target.url))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(target.db.Close(), qt.IsNil) })
	return target
}

// expand is text with each placeholder replaced by what it stands for.
func (t viewOwnerTarget) expand(text string) string {
	for placeholder, name := range t.names {
		text = strings.ReplaceAll(text, placeholder, name)
	}
	return text
}

// schemaFile writes a SQL schema that declares what the fixture holds, with
// the view's body as given and owners appended, and returns its path.
func (t viewOwnerTarget) schemaFile(c *qt.C, body, owners string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	schema := "CREATE TABLE docs (id integer PRIMARY KEY, body text NOT NULL);\n" +
		"ALTER TABLE docs ENABLE ROW LEVEL SECURITY;\n" +
		"CREATE POLICY docs_owner_sees_one ON docs FOR SELECT TO {owner} USING (id = 1);\n" +
		"CREATE VIEW my_docs AS " + body + ";\n" +
		`CREATE MATERIALIZED VIEW "Doc Totals" AS SELECT count(*) AS total FROM docs;` + "\n" + owners
	c.Assert(os.WriteFile(path, []byte(t.expand(schema)), 0o600), qt.IsNil)
	return path
}

// owners reads back the owner of my_docs and of "Doc Totals", as the server
// stores the role's name.
func (t viewOwnerTarget) owners(c *qt.C) []string {
	c.Helper()
	return queryStrings(c, t.db, `SELECT c.relname || '=' || pg_get_userbyid(c.relowner)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm') ORDER BY 1`)
}

// ownersWant is want with its placeholders expanded.
func (t viewOwnerTarget) ownersWant(want ...string) []string {
	expanded := make([]string, len(want))
	for i, owner := range want {
		expanded[i] = t.expand(owner)
	}
	return expanded
}

// objectIDs reads back the oid of my_docs and of "Doc Totals", which a drop
// and a create change and an ALTER keeps.
func (t viewOwnerTarget) objectIDs(c *qt.C) []string {
	c.Helper()
	return queryStrings(c, t.db, `SELECT c.relname || ' ' || c.oid::text
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm') ORDER BY 1`)
}

// rowsReaderSees counts the rows {reader} sees through my_docs, on one
// connection. The view reads docs with its owner's rights: an owner the table
// belongs to skips its row-level security and shows both rows, {owner} sees
// the one row its policy shows, and a role no policy names sees none.
func (t viewOwnerTarget) rowsReaderSees(c *qt.C) int {
	c.Helper()
	conn, err := t.db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(conn.Close(), qt.IsNil) }()
	_, err = conn.ExecContext(c.Context(), "SET ROLE "+t.names["{reader}"])
	c.Assert(err, qt.IsNil)
	defer func() {
		_, err := conn.ExecContext(c.Context(), "RESET ROLE")
		c.Assert(err, qt.IsNil)
	}()
	var rows int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM my_docs").Scan(&rows), qt.IsNil)
	return rows
}

// apply runs `ptah schema apply` of schemaFile against the target.
func (t viewOwnerTarget) apply(c *qt.C, schemaFile string, extra ...string) clirun.Result {
	c.Helper()
	return runPtahIn(c, append([]string{"schema", "apply", "--db-url", t.url, "--schema-file", schemaFile, "--auto-approve"}, extra...)...)
}

// viewOwnerChange is one apply that gives a view an owner, in place or
// through a rebuild.
type viewOwnerChange struct {
	name string
	// held are the fixture statements that give the views their owners
	// before the apply. Without one, the role the test connects as owns them.
	held []string
	// body is the view's declared body, and owners the ownership statements
	// the schema declares.
	body, owners string
	// statements are what the plan writes, in order.
	statements []string
	// after is what [viewOwnerTarget.owners] reads back afterwards, and rows
	// what [viewOwnerTarget.rowsReaderSees] counts.
	after []string
	rows  int
	// inPlace says the views keep their oids.
	inPlace bool
}

var viewOwnerChanges = []viewOwnerChange{
	{
		name:       "an owner given in place",
		body:       viewOwnerBody,
		owners:     "ALTER VIEW my_docs OWNER TO {owner};\n",
		statements: []string{`ALTER VIEW "my_docs" OWNER TO "{owner}"`},
		after:      []string{"Doc Totals={admin}", "my_docs={owner}"},
		rows:       1,
		inPlace:    true,
	},
	{
		name:       "an owner taken from another role",
		held:       []string{"ALTER VIEW my_docs OWNER TO {other}"},
		body:       viewOwnerBody,
		owners:     "ALTER VIEW my_docs OWNER TO {owner};\n",
		statements: []string{`ALTER VIEW "my_docs" OWNER TO "{owner}"`},
		after:      []string{"Doc Totals={admin}", "my_docs={owner}"},
		rows:       1,
		inPlace:    true,
	},
	{
		name:       "a quoted role and a quoted materialized view",
		body:       viewOwnerBody,
		owners:     `ALTER MATERIALIZED VIEW "Doc Totals" OWNER TO {auditors};` + "\n",
		statements: []string{`ALTER MATERIALIZED VIEW "Doc Totals" OWNER TO "{auditors.stored}"`},
		after:      []string{"Doc Totals={auditors.stored}", "my_docs={admin}"},
		rows:       2,
		inPlace:    true,
	},
	{
		name:       "a rebuild gives the declared owner",
		body:       viewOwnerNarrow,
		owners:     "ALTER VIEW my_docs OWNER TO {owner};\n",
		statements: []string{`DROP VIEW "my_docs"`, "CREATE VIEW \"my_docs\" AS\n" + viewOwnerNarrow, `ALTER VIEW "my_docs" OWNER TO "{owner}"`},
		after:      []string{"Doc Totals={admin}", "my_docs={owner}"},
		rows:       1,
	},
	{
		// Without the owner the read found, the rebuilt view would belong to
		// the role that runs the plan and show the reader both rows.
		name:       "a rebuild gives an undeclared owner back",
		held:       []string{"ALTER VIEW my_docs OWNER TO {other}"},
		body:       viewOwnerNarrow,
		statements: []string{`DROP VIEW "my_docs"`, "CREATE VIEW \"my_docs\" AS\n" + viewOwnerNarrow, `ALTER VIEW "my_docs" OWNER TO "{other}"`},
		after:      []string{"Doc Totals={admin}", "my_docs={other}"},
		rows:       0,
	},
}

// TestSchemaApplyGivesAViewItsOwnerE2E applies each of [viewOwnerChanges] on
// each engine through the shipped binary and reads the result back from the
// server: the owner each view has, the rows the reader sees through my_docs,
// which the owner decides, and whether the views kept their oids. A second
// apply plans nothing, and a drift check finds nothing.
func TestSchemaApplyGivesAViewItsOwnerE2E(t *testing.T) {
	for _, engine := range viewOwnerEngines {
		for _, test := range viewOwnerChanges {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := newViewOwnerTarget(c, engine, test.held...)
				objects := target.objectIDs(c)
				schema := target.schemaFile(c, test.body, test.owners)

				got := target.apply(c, schema)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				for _, statement := range test.statements {
					c.Assert(got.Stdout, qt.Contains, target.expand(statement)+";\n")
				}
				c.Assert(target.owners(c), qt.DeepEquals, target.ownersWant(test.after...))
				c.Assert(target.rowsReaderSees(c), qt.Equals, test.rows)
				c.Assert(slices.Equal(target.objectIDs(c), objects), qt.Equals, test.inPlace)

				again := target.apply(c, schema)

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
				drift := runPtahIn(c, "schema", "drift", "--db-url", target.url, "--schema-file", schema)
				c.Assert(drift.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", drift.Stdout, drift.Stderr))
			})
		}
	}
}

// TestSchemaApplyLeavesAnUndeclaredOwnerAloneE2E applies a schema that
// declares no owner to views other roles own. An owner nobody declared is not
// managed: the plan is empty, the owners and the rows the reader sees are what
// they were, and drift reports nothing.
func TestSchemaApplyLeavesAnUndeclaredOwnerAloneE2E(t *testing.T) {
	for _, engine := range viewOwnerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewOwnerTarget(c, engine,
				"ALTER VIEW my_docs OWNER TO {owner}", `ALTER MATERIALIZED VIEW "Doc Totals" OWNER TO {auditors}`)
			schema := target.schemaFile(c, viewOwnerBody, "")

			got := target.apply(c, schema)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(target.owners(c), qt.DeepEquals, target.ownersWant("Doc Totals={auditors.stored}", "my_docs={owner}"))
			c.Assert(target.rowsReaderSees(c), qt.Equals, 1)
			drift := runPtahIn(c, "schema", "drift", "--db-url", target.url, "--schema-file", schema)
			c.Assert(drift.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", drift.Stdout, drift.Stderr))
		})
	}
}

// TestSchemaDriftReportsADeclaredOwnerE2E checks a declared owner the view
// does not have: drift exits 1 and counts the view as modified, and the owner
// is what it was.
func TestSchemaDriftReportsADeclaredOwnerE2E(t *testing.T) {
	for _, engine := range viewOwnerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewOwnerTarget(c, engine)

			got := runPtahIn(c, "schema", "drift", "--db-url", target.url,
				"--schema-file", target.schemaFile(c, viewOwnerBody, "ALTER VIEW my_docs OWNER TO {owner};\n"))

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "- views_modified: 1 (warning)\n")
			c.Assert(target.owners(c), qt.DeepEquals, target.ownersWant("Doc Totals={admin}", "my_docs={admin}"))
		})
	}
}

// missingOwnerRefusal is the part of the refusal every place a plan runs
// writes, for a view given to the role ptah_vo_missing.
const missingOwnerRefusal = `ALTER VIEW "my_docs" OWNER TO "ptah_vo_missing" gives the object to role "ptah_vo_missing", ` +
	`which the server does not hold and no statement before it creates; a role belongs to the server, not to a database, ` +
	`so create the role on the server or declare it in the desired schema`

// TestSchemaApplyRefusesAnOwnerTheServerDoesNotHoldE2E declares an owner no
// role on the server is, beside a table the plan would create first. The
// refusal comes before any statement is sent, so the table is not created, and
// it names the statement and the role rather than the server's SQLSTATE 42704.
func TestSchemaApplyRefusesAnOwnerTheServerDoesNotHoldE2E(t *testing.T) {
	for _, engine := range viewOwnerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewOwnerTarget(c, engine)

			got := target.apply(c, target.schemaFile(c, viewOwnerBody,
				"CREATE TABLE extra (id integer PRIMARY KEY);\nALTER VIEW my_docs OWNER TO ptah_vo_missing;\n"))

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, missingOwnerRefusal)
			c.Assert(got.Stderr, qt.Not(qt.Contains), "42704")
			c.Assert(queryStrings(c, target.db, "SELECT COALESCE(to_regclass('extra')::text, 'absent')"), qt.DeepEquals, []string{"absent"})
			c.Assert(target.owners(c), qt.DeepEquals, target.ownersWant("Doc Totals={admin}", "my_docs={admin}"))
		})
	}
}

// TestSchemaApplyRefusesAnOwnerOnANamedDevDatabaseE2E rehearses the same plan
// on a dev database Ptah does not start. Its roles are the dev server's own, so
// the rehearsal refuses first, says where to create the role or that a
// docker:// dev URL brings the target's roles, and the target is untouched.
func TestSchemaApplyRefusesAnOwnerOnANamedDevDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	target := newViewOwnerTarget(c, viewOwnerEngines[0])
	dev := postgresScratchDevURL(c, "ptah_view_owner_dev")

	got := target.apply(c, target.schemaFile(c, viewOwnerBody, "ALTER VIEW my_docs OWNER TO ptah_vo_missing;\n"), "--dev-url", dev)

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, "dev database simulation failed during plan: "+missingOwnerRefusal+
		"; a dev server Ptah does not start keeps its own roles: create the role there, or use a docker:// dev URL")
	c.Assert(got.Stderr, qt.Contains, "the plan was not applied to the target database")
	c.Assert(target.owners(c), qt.DeepEquals, target.ownersWant("Doc Totals={admin}", "my_docs={admin}"))
}

// TestSchemaApplyReportsAnOwnerTheConnectionMayNotGiveE2E applies as a role
// that owns the view and is not a member of the declared owner. PostgreSQL
// refuses the change with SQLSTATE 42501, and Ptah reports the statement and
// the code with the server's reason; the transaction leaves the owner as it
// was.
func TestSchemaApplyReportsAnOwnerTheConnectionMayNotGiveE2E(t *testing.T) {
	c := qt.New(t)
	applier := fmt.Sprintf("ptah_vo_applier_%d", time.Now().UnixNano())
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbtarget.URL(c, dbtarget.PostgreSQL)))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+applier+" LOGIN PASSWORD 'ptah_vo_applier'")
	c.Assert(err, qt.IsNil)
	// Registered before the target's, so it runs after the database that
	// holds the applier's view is gone.
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP ROLE "+applier)
		c.Assert(err, qt.IsNil)
	})
	target := newViewOwnerTarget(c, viewOwnerEngines[0])
	for _, statement := range []string{
		"GRANT USAGE, CREATE ON SCHEMA public TO " + applier,
		"GRANT SELECT ON docs TO " + applier,
		"ALTER VIEW my_docs OWNER TO " + applier,
	} {
		execPostgresFamilySQL(c, c.Context(), target.url, statement)
	}
	asApplier, err := url.Parse(target.url)
	c.Assert(err, qt.IsNil)
	asApplier.User = url.UserPassword(applier, "ptah_vo_applier")

	got := runPtahIn(c, "schema", "apply", "--db-url", asApplier.String(), "--auto-approve",
		"--schema-file", target.schemaFile(c, viewOwnerBody, "ALTER VIEW my_docs OWNER TO {owner};\n"))

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, target.expand(`(SQLSTATE 42501)`+"\n"+`SQL: ALTER VIEW "my_docs" OWNER TO "{owner}"`))
	c.Assert(target.owners(c), qt.DeepEquals, target.ownersWant("Doc Totals={admin}", "my_docs="+applier))
}
