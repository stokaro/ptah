//go:build integration

package providercli_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	// The test writes the tables the provider reads, and reads back what an
	// apply left, through a driver of its own, so neither passes through the
	// provider the command used.
	_ "modernc.org/sqlite"

	"ptah.run/internal/testutils"
)

// constraintsFixture is the CLI over a fixture database the test wrote with
// statements, served by the provider that reads, compares and plans the
// foreign keys, CHECKs and index NULLS orders of its tables. mode selects the
// provider's features.
func constraintsFixture(c *qt.C, mode string, statements ...string) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "constraints", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_CONSTRAINT_MODE="+mode)
	f := newProviderCLI(c, spec)
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	for _, statement := range statements {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return f
}

// declare writes the desired schema the next command reads.
func (f providerCLI) declare(c *qt.C, schema string) {
	c.Helper()
	c.Assert(os.WriteFile(f.schemaPath, []byte(schema), 0o600), qt.IsNil)
}

// storedTable is the statement SQLite keeps for a table, or for an index.
func (f providerCLI) storedTable(c *qt.C, name string) string {
	c.Helper()
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	var statement string
	c.Assert(db.QueryRowContext(c.Context(), `SELECT sql FROM sqlite_schema WHERE name = ?`, name).Scan(&statement), qt.IsNil)
	return statement
}

// foreignKeys are the foreign keys SQLite enforces on a table, each as
// "<column> -> <table>(<column>) ON DELETE <rule>".
func (f providerCLI) foreignKeys(c *qt.C, table string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `SELECT "from" || ' -> ' || "table" || '(' || "to" || ') ON DELETE ' || on_delete FROM pragma_foreign_key_list(?) ORDER BY id`, table)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var result []string
	for rows.Next() {
		var key string
		c.Assert(rows.Scan(&key), qt.IsNil)
		result = append(result, key)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return result
}

// insert runs one INSERT on a connection that enforces foreign keys and
// returns its error.
func (f providerCLI) insert(c *qt.C, statement string) error {
	c.Helper()
	db, err := sql.Open("sqlite", f.databasePath+"?_pragma=foreign_keys(1)")
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	_, err = db.ExecContext(c.Context(), statement)
	return err
}

const (
	// heldScoreCheck is a table whose CHECK SQLite keeps as written, with the
	// number as a string literal.
	heldScoreCheck = `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "score" INTEGER NOT NULL, CONSTRAINT "items_score_check" CHECK (score >= '0'))`
	// heldOwnersAndPosts are two tables with no foreign key between them.
	heldOwners = `CREATE TABLE "owners" ("id" INTEGER NOT NULL PRIMARY KEY)`
	heldPosts  = `CREATE TABLE "posts" ("id" INTEGER NOT NULL PRIMARY KEY, "owner_id" INTEGER NOT NULL)`
)

// declaredScoreCheck declares items with the CHECK expression check under
// the name the table holds.
func declaredScoreCheck(check string) string {
	return "tables:\n  items:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n" +
		"      score: {type: INTEGER, not_null: true, check: \"" + check + "\", check_name: items_score_check}\n"
}

// declaredPosts declares owners and posts, with owner as the YAML of
// posts.owner_id.
func declaredPosts(owner string) string {
	return "tables:\n  owners:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n" +
		"  posts:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n      owner_id: " + owner + "\n"
}

const (
	// referencingOwner is posts.owner_id referencing owners.id through
	// posts_owner_fk.
	referencingOwner = `{type: INTEGER, not_null: true, foreign: "owners(id)", foreign_key_name: posts_owner_fk, on_delete: CASCADE}`
	// plainOwner is posts.owner_id with no reference.
	plainOwner = `{type: INTEGER, not_null: true}`
)

// A CHECK the database keeps in a spelling of its own, `score >= '0'`, is the
// declared `score >= 0` by the provider's reading, and the plan leaves it.
// Compared by its text, it would be dropped and added on every run.
func TestProviderCheckEquivalence_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := constraintsFixture(c, "", heldScoreCheck)
	f.declare(c, declaredScoreCheck("score >= 0"))

	planned := f.plan(c)

	c.Assert(planned, qt.Contains, `"outcome": "no-changes"`)
	c.Assert(f.storedTable(c, "items"), qt.Equals, heldScoreCheck)
}

// A CHECK the provider reads as another constraint is replaced: the plan
// hands the table to the provider, which rebuilds it, the database then
// refuses a row the new CHECK forbids, and a second plan leaves the table.
func TestProviderCheckChange_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := constraintsFixture(c, "", heldScoreCheck)
	c.Assert(f.insert(c, `INSERT INTO "items" ("id", "score") VALUES (1, 0)`), qt.IsNil)
	f.declare(c, declaredScoreCheck("score >= 0 AND score < 10"))

	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(f.storedTable(c, "items"), qt.Equals,
		`CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "score" INTEGER NOT NULL, CONSTRAINT "items_score_check" CHECK (score >= 0 AND score < 10))`)
	c.Assert(f.insert(c, `INSERT INTO "items" ("id", "score") VALUES (2, 10)`), qt.ErrorMatches, `.*CHECK constraint failed.*`)
	c.Assert(f.insert(c, `INSERT INTO "items" ("id", "score") VALUES (3, 9)`), qt.IsNil)
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}

// A foreign key declared on a column of a kept table is added by the
// provider, the database enforces it, and the read reports it, so a second
// plan leaves it; declared away, it is dropped.
func TestProviderForeignKeyChanges_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := constraintsFixture(c, "", heldOwners, heldPosts)
	f.declare(c, declaredPosts(referencingOwner))

	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(f.foreignKeys(c, "posts"), qt.DeepEquals, []string{"owner_id -> owners(id) ON DELETE CASCADE"})
	c.Assert(f.insert(c, `INSERT INTO "posts" ("id", "owner_id") VALUES (1, 7)`), qt.ErrorMatches, `.*FOREIGN KEY constraint failed.*`)
	read := f.success(c, "inspect", "--format", "json")
	c.Assert(read, qt.Contains, `"posts_owner_fk"`)
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)

	f.declare(c, declaredPosts(plainOwner))
	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")
	c.Assert(f.foreignKeys(c, "posts"), qt.IsNil)
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}

// A provider that reads constraints and does not plan their change has the
// change of a kept table's constraint refused by the table, and the database
// keeps what it held.
func TestProviderConstraintChangesWithoutThePlanner_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := constraintsFixture(c, "unplanned", heldOwners, heldPosts)
	f.declare(c, declaredPosts(referencingOwner))

	_, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")

	c.Assert(code, qt.Equals, 2)
	c.Assert(diagnostics, qt.Matches, `(?s).*changing constraint posts_owner_fk of table (main\.)?posts on the fixture engine, whose provider does not plan it.*`)
	c.Assert(f.foreignKeys(c, "posts"), qt.IsNil)
}

// declaredScoreIndex declares items with an index on score whose key states
// nulls, an HCL index key attribute.
func declaredScoreIndex(nulls string) string {
	return `schema "main" {}
table "items" {
  schema = schema.main
  column "id" {
    type = INTEGER
    null = false
  }
  column "score" {
    type = INTEGER
    null = false
  }
  primary_key {
    columns = [column.id]
  }
  index "items_by_score" {
    on {
      column = column.score
      ` + nulls + `
    }
  }
}
`
}

// heldScoreIndex is items with an ascending index on score, whose NULLS
// SQLite sorts first.
var heldScoreIndex = []string{`CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "score" INTEGER NOT NULL)`,
	`CREATE INDEX "items_by_score" ON "items" ("score")`}

// scoreIndexFixture is the CLI over heldScoreIndex, with the HCL document
// declaring items' index key with nulls.
func scoreIndexFixture(c *qt.C, nulls string) providerCLI {
	c.Helper()
	f := constraintsFixture(c, "", heldScoreIndex...)
	f.schemaPath = filepath.Join(filepath.Dir(f.schemaPath), "schema.hcl")
	f.declare(c, declaredScoreIndex(nulls))
	return f
}

// An index key declared NULLS FIRST is the ascending key SQLite holds, whose
// NULLS it sorts first, and the plan leaves the index.
func TestProviderIndexNullsOrder_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := scoreIndexFixture(c, "nulls_first = true")

	planned := f.plan(c)

	c.Assert(planned, qt.Contains, `"outcome": "no-changes"`)
}

// An index key declared NULLS LAST is another key than the one SQLite holds.
// The provider is handed the index and refuses it in its words, since SQLite
// cannot build it, and the index stays. Without the order compared, the plan
// would report the index synced.
func TestProviderIndexNullsOrder_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := scoreIndexFixture(c, "nulls_last = true")

	_, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")

	c.Assert(code, qt.Equals, 2)
	c.Assert(diagnostics, qt.Contains, `SQLite keeps no NULLS order in an index key, so the fixture cannot build index items_by_score with NULLS LAST`)
	c.Assert(f.storedTable(c, "items_by_score"), qt.Equals, heldScoreIndex[1])
}

// `migrations generate` of two tables, one referencing the other through a
// table constraint, writes their creation to the up file and their removal to
// the down file. The down file's plan drops the tables the up file creates,
// starting from each table as the host projects it from its declaration and
// the foreign key the up file adds, in the form a provider read reports.
// Without that projection the command has no capture of the referencing
// table and refuses to plan the down file.
func TestProviderForeignKeyMigration_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := constraintsFixture(c, "text")
	f.declare(c, declaredPosts(plainOwner)+"constraints:\n  posts_owner_fk:\n    table: posts\n    type: FOREIGN KEY\n    columns: [owner_id]\n"+
		"    foreign_table: owners\n    foreign_column: id\n    on_delete: CASCADE\n")

	_, diagnostics, code, dir := f.generateMigration(c)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(upMigration(c, dir), qt.Contains,
		`CREATE TABLE "posts" ("id" INTEGER NOT NULL PRIMARY KEY, "owner_id" INTEGER NOT NULL, CONSTRAINT "posts_owner_fk" FOREIGN KEY ("owner_id") REFERENCES "owners" ("id") ON DELETE CASCADE);`)
	down := downMigration(c, dir)
	c.Assert(down, qt.Contains, `DROP TABLE "posts";`)
	c.Assert(down, qt.Contains, `DROP TABLE "owners";`)
}

// downMigration returns the text of the one down migration in dir.
func downMigration(c *qt.C, dir string) string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	data, err := os.ReadFile(files[0])
	c.Assert(err, qt.IsNil)
	return string(data)
}

const (
	// heldAccounts is a table another table references. It sorts before the
	// table that references it, which is the order a comparison lists
	// removals in.
	heldAccounts = `CREATE TABLE "accounts" ("id" INTEGER NOT NULL PRIMARY KEY)`
	// heldOrders references accounts through a foreign key the read reports.
	heldOrders = `CREATE TABLE "orders" ("id" INTEGER NOT NULL PRIMARY KEY, "account_id" INTEGER NOT NULL, ` +
		`CONSTRAINT "orders_account_fk" FOREIGN KEY ("account_id") REFERENCES "accounts" ("id"))`
	// heldKept is a table the declaration keeps.
	heldKept = `CREATE TABLE "kept" ("id" INTEGER NOT NULL PRIMARY KEY)`
	// declaredKept declares kept alone, so a plan removes accounts and orders.
	declaredKept = "tables:\n  kept:\n    columns:\n      id: {type: INTEGER, primary: true, not_null: true}\n"
)

// referencedRemovalFixture is the CLI over accounts, orders and kept, with a
// row in orders that references a row in accounts, served by a provider whose
// sessions enforce foreign keys. mode selects the provider's features.
func referencedRemovalFixture(c *qt.C, mode string) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "constraints", testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_CONSTRAINT_MODE="+mode, "FIXTURE_FOREIGN_KEYS=on")
	f := newProviderCLI(c, spec)
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	for _, statement := range []string{heldAccounts, heldOrders, heldKept, `INSERT INTO "accounts" ("id") VALUES (1)`,
		`INSERT INTO "orders" ("id", "account_id") VALUES (1, 1)`} {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return f
}

// Two removed tables, one referencing the other through a foreign key the
// read reports, are dropped referencing table first when the provider drops
// each table in its own step, in place of the host's. The host chains those
// steps in its removal order. In the order the comparison lists the removals,
// accounts goes first, and SQLite refuses that drop while orders still
// references its row, so the apply fails and both tables stay.
func TestProviderRemovesAReferencedTableAfterItsReferences_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := referencedRemovalFixture(c, "text")
	f.declare(c, declaredKept)

	c.Assert(f.plan(c), qt.Contains, `"outcome": "changes"`)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	c.Assert(f.tableNames(c), qt.DeepEquals, []string{"kept"})
	c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
}
