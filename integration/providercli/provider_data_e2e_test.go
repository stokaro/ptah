//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/provider"
	// The test reads the fixture's database file back through a driver of its
	// own, so the answer does not pass through the provider the command used.
	_ "modernc.org/sqlite"

	"ptah.run/core/query"
	"ptah.run/dbschema"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// The fixture's SQLite is the engine here: the provider reads, types and
// writes declared rows and runs seed files, and the expected values come from
// reading the database file back.

const dataEntities = `package entities

//ptah:schema:data table="regions" key="code" file="regions.yaml"
//ptah:schema:table name="regions"
type Region struct {
	//ptah:schema:field name="code" type="TEXT" primary="true" not_null="true"
	Code string

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string

	//ptah:schema:field name="rank" type="INTEGER"
	Rank int
}
`

type dataCLI struct {
	providerCLI
	entities, rows, seeds, migrations string
}

func dataSpec(c *qt.C) providerhost.Spec {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, "data")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"}
}

func newDataCLI(c *qt.C, spec providerhost.Spec) dataCLI {
	c.Helper()
	f := dataCLI{providerCLI: newProviderCLI(c, spec)}
	dir := filepath.Dir(f.manifestPath)
	f.entities, f.seeds, f.migrations = filepath.Join(dir, "entities"), filepath.Join(dir, "seeds"), filepath.Join(dir, "migrations")
	f.rows = filepath.Join(f.entities, "regions.yaml")
	for _, created := range []string{f.entities, f.seeds} {
		c.Assert(os.MkdirAll(created, 0o700), qt.IsNil)
	}
	c.Assert(os.WriteFile(filepath.Join(f.entities, "schema.go"), []byte(dataEntities), 0o600), qt.IsNil)
	return f
}

func (f dataCLI) declare(c *qt.C, rows string) {
	c.Helper()
	c.Assert(os.WriteFile(f.rows, []byte(rows), 0o600), qt.IsNil)
}

// command runs one ptah command line against the fixture and waits for the
// provider process to end.
func (f dataCLI) command(c *qt.C, args ...string) (stdout, stderr string, code int) {
	c.Helper()
	args = append(args, "--provider-config", f.manifestPath, "--db-url", f.address)
	stdout, stderr, code = executeProviderCLI(c.Context(), args)
	f.assertStopped(c)
	return stdout, stderr, code
}

func (f dataCLI) planRows(c *qt.C) string {
	c.Helper()
	out, diagnostics, code := f.command(c, "schema", "plan", "--root-dir", f.entities, "--output", f.planPath, "--json")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	return out
}

func (f dataCLI) applyPlan(c *qt.C) (stdout, stderr string, code int) {
	c.Helper()
	return f.command(c, "schema", "apply", "--plan", f.planPath, "--auto-approve", "--json")
}

// stored reads the regions table back, code to name and rank.
func (f dataCLI) stored(c *qt.C) map[string]string {
	c.Helper()
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `SELECT code, name, coalesce(rank, -1) FROM regions ORDER BY code`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	result := make(map[string]string)
	for rows.Next() {
		var code, name string
		var rank int64
		c.Assert(rows.Scan(&code, &name, &rank), qt.IsNil)
		result[code] = name + "/" + strconv.FormatInt(rank, 10)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return result
}

func TestProviderWritesDeclaredRows(t *testing.T) {
	c := qt.New(t)
	f := newDataCLI(c, dataSpec(c))
	f.declare(c, "- code: NO\n  name: Norway\n  rank: 2\n- code: CZ\n  name: Czechia\n  rank: \"1\"\n")
	planned := f.planRows(c)
	c.Assert(planned, qt.Contains, `INSERT INTO \"regions\" (\"code\", \"name\", \"rank\") VALUES ('CZ', 'Czechia', 1);`)
	out, diagnostics, code := f.applyPlan(c)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(f.stored(c), qt.DeepEquals, map[string]string{"CZ": "Czechia/1", "NO": "Norway/2"})

	// The declared "1" and the stored 1 are one value once the provider typed
	// them, so a converged table plans nothing.
	c.Assert(f.planRows(c), qt.Contains, `"outcome": "no-changes"`)

	f.declare(c, "- code: NO\n  name: Norge\n  rank: 2\n- code: SE\n  name: Sweden\n  rank: 3\n")
	planned = f.planRows(c)
	c.Assert(planned, qt.Contains, `UPDATE \"regions\" SET \"name\" = 'Norge', \"rank\" = 2 WHERE \"code\" = 'NO';`)
	c.Assert(planned, qt.Contains, `DELETE FROM \"regions\" WHERE \"code\" = 'CZ';`)
	out, diagnostics, code = f.applyPlan(c)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(f.stored(c), qt.DeepEquals, map[string]string{"NO": "Norge/2", "SE": "Sweden/3"})
}

// The data stage runs in the apply's transaction: a row the engine refuses
// rolls back the rows written before it and the table the plan created.
func TestProviderRollsBackDeclaredRowsWithTheApply(t *testing.T) {
	c := qt.New(t)
	f := newDataCLI(c, dataSpec(c))
	f.declare(c, "- code: AA\n  name: First\n- code: ZZ\n  name: null\n")
	f.planRows(c)
	_, diagnostics, code := f.applyPlan(c)
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Not(qt.Equals), "")
	inspected := f.success(c, "inspect", "--format", "json")
	c.Assert(inspected, qt.Not(qt.Contains), "regions")
}

func (f dataCLI) seed(c *qt.C, name, text string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(f.seeds, name), []byte(text), 0o600), qt.IsNil)
}

func (f dataCLI) runSeeds(c *qt.C, extra ...string) (string, int) {
	c.Helper()
	_, diagnostics, code := f.command(c, append([]string{"seed", "--seeds-dir", f.seeds, "--env", "dev"}, extra...)...)
	return diagnostics, code
}

func (f dataCLI) createRegions(c *qt.C) {
	c.Helper()
	f.declare(c, "[]\n")
	f.planRows(c)
	out, diagnostics, code := f.applyPlan(c)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
}

func TestProviderRunsSeedFilesThroughItsLedger(t *testing.T) {
	c := qt.New(t)
	f := newDataCLI(c, dataSpec(c))
	f.createRegions(c)
	f.seed(c, "001_regions.dev.sql", "INSERT INTO \"regions\" VALUES ('NO', 'Norway', 2);\n")
	diagnostics, code := f.runSeeds(c)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(f.stored(c), qt.DeepEquals, map[string]string{"NO": "Norway/2"})

	// A second run finds the file in the ledger and does not run it again.
	diagnostics, code = f.runSeeds(c)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(f.stored(c), qt.DeepEquals, map[string]string{"NO": "Norway/2"})

	// A protected table the provider lists refuses the run.
	diagnostics, code = f.runSeeds(c, "--protected-table", "regions")
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "protected tables exist: regions")

	// An applied file that changed is refused, by the checksum the ledger keeps.
	f.seed(c, "001_regions.dev.sql", "INSERT INTO \"regions\" VALUES ('NO', 'Norge', 2);\n")
	diagnostics, code = f.runSeeds(c)
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "checksum")
	c.Assert(f.stored(c), qt.DeepEquals, map[string]string{"NO": "Norway/2"})
}

func TestProviderSeedKeyConflictIsToleratedOnlyWhenIdempotent(t *testing.T) {
	c := qt.New(t)
	f := newDataCLI(c, dataSpec(c))
	f.createRegions(c)
	f.seed(c, "001_regions.dev.sql", "INSERT INTO \"regions\" VALUES ('NO', 'Norway', 2);\n")
	diagnostics, code := f.runSeeds(c)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	f.seed(c, "002_more.dev.sql", "INSERT INTO \"regions\" VALUES ('SE', 'Sweden', 3);\nINSERT INTO \"regions\" VALUES ('NO', 'Norge', 2);\n")

	diagnostics, code = f.runSeeds(c)
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "existing key")
	c.Assert(f.stored(c), qt.DeepEquals, map[string]string{"NO": "Norway/2"})

	diagnostics, code = f.runSeeds(c, "--idempotent")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(f.stored(c), qt.DeepEquals, map[string]string{"NO": "Norway/2"})
	diagnostics, code = f.runSeeds(c)
	c.Assert(code, qt.Equals, 0, qt.Commentf("the conflicting file is recorded: %s", diagnostics))
}

func TestProviderRefusesASeedFileBeforeItRuns(t *testing.T) {
	c := qt.New(t)
	f := newDataCLI(c, dataSpec(c))
	f.createRegions(c)
	f.seed(c, "001_regions.dev.sql", "INSERT INTO \"regions\" VALUES ('NO', 'Norway', 2);\nVACUUM;\n")
	diagnostics, code := f.runSeeds(c)
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "line 2")
	c.Assert(f.stored(c), qt.DeepEquals, make(map[string]string))
}

// A seed reply that is lost after the provider committed is reported as an
// outcome nobody confirmed, and the file is not sent again: the row it
// inserts exists once.
func TestProviderSeedLostReplyIsNotReplayed(t *testing.T) {
	c := qt.New(t)
	f := newDataCLI(c, dataSpec(c))
	f.createRegions(c)
	f.seed(c, "001_regions.dev.sql", "INSERT INTO \"regions\" VALUES ('NO', 'Norway', 2);\n")
	f.address += "?fault=lost-seed-reply"
	diagnostics, code := f.runSeeds(c)
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "not run again")
	c.Assert(f.stored(c), qt.DeepEquals, map[string]string{"NO": "Norway/2"})
}

// A data migration holds the statements the provider rendered, as text.
func TestProviderRendersDataMigrationFiles(t *testing.T) {
	c := qt.New(t)
	f := newDataCLI(c, dataSpec(c))
	f.createRegions(c)
	f.declare(c, "- code: NO\n  name: Norway\n  rank: 2\n")
	out, diagnostics, code := f.command(c, "migrations", "data", "--root-dir", f.entities, "--migrations-dir", f.migrations, "--dry-run")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains, `INSERT INTO "regions" ("code", "name", "rank") VALUES ('NO', 'Norway', 2);`)
	c.Assert(out, qt.Contains, `DELETE FROM "regions" WHERE "code" = 'NO';`)
}

// The query builder renders for the engine with the spelling its provider
// declares, once the host started the provider.
func TestProviderQueryDialectReachesTheQueryBuilder(t *testing.T) {
	c := qt.New(t)
	host, err := providerhost.Start(c.Context(), dataSpec(c))
	c.Assert(err, qt.IsNil)
	defer host.Close()
	insert := query.InsertInto("regions").Columns("code", "name").Values("NO", "Norway").OnConflictDoNothing("code")
	text, args, err := query.RenderInsert(insert.Build(), "fixture")
	c.Assert(err, qt.IsNil)
	c.Assert(text, qt.Equals, `INSERT INTO "regions" ("code", "name") VALUES (?, ?) ON CONFLICT ("code") DO NOTHING`)
	c.Assert(args, qt.DeepEquals, []any{"NO", "Norway"})
}

// A provider that offers no data service refuses declared rows and seed files
// rather than let the host write its own statements and tracker table.
func TestProviderWithoutDataServicesRefusesRowsAndSeeds(t *testing.T) {
	c := qt.New(t)
	binary := testutils.BuildFixtureProvider(c, "provider")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	f := newDataCLI(c, providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"})
	f.declare(c, "- code: NO\n  name: Norway\n")
	_, diagnostics, code := f.command(c, "schema", "plan", "--root-dir", f.entities, "--output", f.planPath, "--json")
	c.Assert(code, qt.Equals, 2)
	c.Assert(diagnostics, qt.Matches, `(?s).*the fixture provider does not offer data\.type-rows.*`)
	_, err = os.Stat(f.planPath)
	c.Assert(os.IsNotExist(err), qt.IsTrue)

	f.seed(c, "001_regions.dev.sql", "CREATE TABLE \"regions\" (\"code\" TEXT NOT NULL PRIMARY KEY);\n")
	diagnostics, code = f.runSeeds(c)
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Matches, `(?s).*the fixture provider does not offer data\.seeds.*`)
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), "regions")
}

// A table larger than one page is read to its end: a read that stopped after
// the first page would plan the rows of the second as inserts.
func TestProviderReadsDeclaredRowsPageByPage(t *testing.T) {
	c := qt.New(t)
	f := newDataCLI(c, dataSpec(c))
	f.createRegions(c)
	const count = provider.MaxRowsPerPage + 1
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(c.Context(), `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
		INSERT INTO regions (code, name, rank) SELECT printf('R%05d', i), 'region', i FROM n`, count)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
	var declared strings.Builder
	for i := 1; i <= count; i++ {
		fmt.Fprintf(&declared, "- code: R%05d\n  name: region\n  rank: %d\n", i, i)
	}
	f.declare(c, declared.String())
	c.Assert(f.planRows(c), qt.Contains, `"outcome": "no-changes"`)
}

// Rows are written as statement text, so a provider that renders a write only
// as a native operation is refused rather than sent the host's own text.
func TestProviderRefusesNativeRowWrites(t *testing.T) {
	c := qt.New(t)
	spec := dataSpec(c)
	spec.Environment = append(spec.Environment, "FIXTURE_ROW_WRITES=native")
	f := newDataCLI(c, spec)
	f.declare(c, "- code: NO\n  name: Norway\n")
	_, diagnostics, code := f.command(c, "schema", "plan", "--root-dir", f.entities, "--output", f.planPath, "--json")
	c.Assert(code, qt.Equals, 2)
	c.Assert(diagnostics, qt.Contains, "the fixture provider renders a row write as a native operation, and rows are written as statement text")
}

// The unkeyed read refuses a provider's session, whose owner reads in key
// order.
func TestProviderSessionNeedsAKeyedRead(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "data")
	conn, _ := transactionConnection(c, host, "")
	rows, err := dbschema.ReadTableRows(c.Context(), conn, "", "regions", []string{"code"})
	c.Assert(err, qt.ErrorMatches, `dbschema: the owner of this connection reads rows in key order; read them with ReadKeyedTableRows`)
	c.Assert(rows, qt.IsNil)
	rows, err = dbschema.ReadKeyedTableRows(c.Context(), conn, "", "regions", []string{"code"}, []string{"code"})
	c.Assert(err, qt.ErrorMatches, `dbschema: read rows of table "regions": the provider reports that table "regions" does not exist`)
	c.Assert(rows, qt.IsNil)
}
