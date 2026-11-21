//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/dbschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/dbtarget"
)

// bareBooleanModels writes every PostgreSQL boolean attribute whose SQL a
// bare spelling changes as a bare word. ROLE stands for the role's name,
// which is unique per run because a role belongs to the whole server.
const bareBooleanModels = `package models

//ptah:schema:table name="docs"
type Doc struct {
	//ptah:schema:field name="id" type="INTEGER" primary
	ID int
}

//ptah:schema:rls:enable table="docs" force
type DocsRowSecurity struct{}

//ptah:schema:sequence name="ticket_seq" cycle
type TicketSequence struct{}

//ptah:schema:sequence name="shared_seq" if_not_exists
type SharedSequence struct{}

//ptah:schema:role name="ROLE" login createdb createrole replication
type App struct{}

//ptah:schema:grant role="ROLE" privilege="SELECT" on_table="docs" with_option
type AppGrant struct{}

//ptah:schema:function name="bare_twice" params="a integer" returns="integer" language="sql" body="SELECT a * 2" strict leakproof
type Twice struct{}

//ptah:schema:extension name="pgcrypto" if_not_exists
type Crypto struct{}
`

// bareBooleanReadBack reads what each bare attribute asks of the server: the
// role's attributes, the sequence's CYCLE, the table's FORCE ROW LEVEL
// SECURITY, the grant option and the function's STRICT and LEAKPROOF. ROLE
// stands for the role's name.
var bareBooleanReadBack = []string{
	`SELECT 'login=' || rolcanlogin || ' createdb=' || rolcreatedb || ' createrole=' || rolcreaterole ||
	   ' replication=' || rolreplication || ' superuser=' || rolsuper FROM pg_roles WHERE rolname = 'ROLE'`,
	`SELECT sequencename || ' cycle=' || cycle FROM pg_sequences WHERE schemaname = 'public'`,
	`SELECT relname || ' force=' || relforcerowsecurity FROM pg_class WHERE relname = 'docs'`,
	`SELECT 'grant option=' || has_table_privilege('ROLE', 'docs', 'SELECT WITH GRANT OPTION')`,
	`SELECT proname || ' strict=' || proisstrict || ' leakproof=' || proleakproof FROM pg_proc WHERE proname = 'bare_twice'`,
	`SELECT extname FROM pg_extension WHERE extname = 'pgcrypto'`,
}

// TestMigrationsUpAppliesBareBooleanAnnotationsE2E plans models whose boolean
// attributes are written bare into a migration against an empty PostgreSQL
// database, creates the sequence and the extension written with a bare
// if_not_exists out of band, and runs the migration. It succeeds only because
// the migration creates them IF NOT EXISTS, and the catalog holds every other
// attribute as true. Dropped, each bare word would leave its default: the
// sequence and extension statements would fail on the objects that exist, and
// the role would be created NOLOGIN NOCREATEDB NOCREATEROLE NOREPLICATION. The
// next plan writes no migration.
func TestMigrationsUpAppliesBareBooleanAnnotationsE2E(t *testing.T) {
	c := qt.New(t)
	admin := dbtarget.URL(c, dbtarget.PostgreSQL)
	role := devDialectScratchName() + "_app"
	// Registered first, this runs after the database is dropped, which takes
	// the role's grant with it: a role holding a privilege cannot be dropped.
	c.Cleanup(func() {
		conn, err := dbschema.ConnectToDatabase(context.Background(), admin)
		c.Check(err, qt.IsNil)
		_, dropErr := conn.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+role+`"`)
		c.Check(dropErr, qt.IsNil)
		c.Check(conn.Close(), qt.IsNil)
	})
	database := createdDevDialectDatabase(c, admin, "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
	models := filepath.Join(c.TempDir(), "models")
	migrations := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(models, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(models, "models.go"), []byte(strings.ReplaceAll(bareBooleanModels, "ROLE", role)), 0o600), qt.IsNil)

	generated := runPtahIn(c, "migrations", "generate", "--db-url", database, "--root-dir", models,
		"--migrations-dir", migrations, "--name", "bare")
	applyEngineFile(c, database, "postgres", "CREATE SEQUENCE shared_seq; CREATE EXTENSION pgcrypto;")
	up := runPtahIn(c, "migrations", "up", "--db-url", database, "--migrations-dir", migrations)
	again := runPtahIn(c, "migrations", "generate", "--db-url", database, "--root-dir", models,
		"--migrations-dir", migrations, "--name", "again")

	c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))
	c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Contains, "no migration files generated")
	reads := make([]string, 0, len(bareBooleanReadBack))
	for _, read := range bareBooleanReadBack {
		reads = append(reads, strings.ReplaceAll(read, "ROLE", role))
	}
	c.Assert(catalogReadBack(c, database, reads), qt.DeepEquals, [][]string{
		{"login=true createdb=true createrole=true replication=true superuser=false"},
		{"shared_seq cycle=false", "ticket_seq cycle=true"},
		{"docs force=true"},
		{"grant option=true"},
		{"bare_twice strict=true leakproof=true"},
		{"pgcrypto"},
	})
}

// TestTimescaleBareIfNotExistsSkipsAHypertableE2E plans a hypertable whose
// if_not_exists is written bare against a table that is still an ordinary
// one, turns the table into a hypertable out of band, and runs the plan. The
// server answers the call with a notice only because the plan asks for
// if_not_exists => TRUE; dropped, the bare word would leave the call that
// answers that the table is already a hypertable. The next plan is empty.
func TestTimescaleBareIfNotExistsSkipsAHypertableE2E(t *testing.T) {
	c := qt.New(t)
	ctx, conn, schemaName := timescaleSchemaFixture(c, "ts_bare")
	execTimescale(ctx, c, conn, fmt.Sprintf(`CREATE TABLE %s.readings ("time" TIMESTAMPTZ NOT NULL, "value" INTEGER NOT NULL)`, schemaName))
	source := fmt.Sprintf("package models\n\n"+
		"//ptah:schema:table name=\"readings\" schema=%q\ntype Reading struct {\n"+
		"\t//ptah:schema:field name=\"time\" type=\"TIMESTAMPTZ\" not_null\n\tTime string\n"+
		"\t//ptah:schema:field name=\"value\" type=\"INTEGER\" not_null\n\tValue int\n}\n\n"+
		"//ptah:schema:hypertable table=\"%s.readings\" column=\"time\" if_not_exists\ntype ReadingsHypertable struct{}\n", schemaName, schemaName)
	declared, err := goschema.ParseSource(builtintest.Annotations(), "readings.go", source)
	c.Assert(err, qt.IsNil)

	statements := planTimescale(c, conn, &declared, schemaName)
	execTimescale(ctx, c, conn, fmt.Sprintf(`SELECT create_hypertable('%s.readings', by_range('time'), create_default_indexes => FALSE)`, schemaName))
	for _, statement := range statements {
		execTimescale(ctx, c, conn, statement)
	}

	c.Assert(strings.Join(statements, "\n"), qt.Contains, "if_not_exists => TRUE")
	c.Assert(planTimescale(c, conn, &declared, schemaName), qt.HasLen, 0)
}

// TestMigrationsUpAppliesABareStoredGeneratedColumnE2E generates and runs a
// migration for a generated column whose stored is written bare, on SQLite,
// whose default is VIRTUAL, and reads the column back: SQLite reports a STORED
// generated column hidden=3 and a VIRTUAL one hidden=2. Dropped, the bare word
// would leave the column VIRTUAL. The next generate writes nothing.
func TestMigrationsUpAppliesABareStoredGeneratedColumnE2E(t *testing.T) {
	c := qt.New(t)
	database := sqliteScratchURL(c)
	models := filepath.Join(c.TempDir(), "models")
	migrations := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(models, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(models, "models.go"), []byte(`package models

//ptah:schema:table name="totals"
type Total struct {
	//ptah:schema:field name="id" type="INTEGER" primary
	ID int
	//ptah:schema:field name="amount" type="INTEGER" not_null
	Amount int
	//ptah:schema:field name="doubled" type="INTEGER" generated="amount * 2" stored
	Doubled int
}
`), 0o600), qt.IsNil)

	generated := runPtahIn(c, "migrations", "generate", "--db-url", database, "--root-dir", models,
		"--migrations-dir", migrations, "--name", "stored")
	up := runPtahIn(c, "migrations", "up", "--db-url", database, "--migrations-dir", migrations)
	again := runPtahIn(c, "migrations", "generate", "--db-url", database, "--root-dir", models,
		"--migrations-dir", migrations, "--name", "again")

	c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))
	c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))
	c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
	c.Assert(again.Stdout, qt.Contains, "no migration files generated")
	c.Assert(catalogReadBack(c, database, []string{
		`SELECT name || ' hidden=' || hidden FROM pragma_table_xinfo('totals') WHERE name = 'doubled'`,
	}), qt.DeepEquals, [][]string{{"doubled hidden=3"}})
}
