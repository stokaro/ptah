//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"
)

// statementStyleStep is one desired state `migrate diff` is run against, and
// the files it is expected to write.
type statementStyleStep struct {
	name   string
	schema string
	up     string
	down   string
}

// statementStyleFixture is the three steps stokaro/ptah#4921 measured against
// the pinned community binary v1.3.0 on PostgreSQL 15.19, with owners.name
// nullable: from PostgreSQL 18 the server names a column's NOT NULL, and the
// rollback that adds the column back restates that name. The files that binary
// wrote differ from these only where the issue leaves them out of scope: it
// groups the changes of one table into one ALTER TABLE, spells varchar
// `character varying`, writes a new table's keys and checks inside its CREATE
// TABLE, and orders the statements of a plan its own way. The change comments,
// the absence of guards and the absence of CASCADE are its.
var statementStyleFixture = []statementStyleStep{
	{
		name: "v1",
		schema: `CREATE TABLE owners (id bigint PRIMARY KEY, name varchar);
CREATE TABLE items (
  id bigint PRIMARY KEY,
  owner_id bigint NOT NULL REFERENCES owners (id) ON DELETE CASCADE,
  note text DEFAULT 'a',
  CONSTRAINT items_note_len CHECK (length(note) < 100)
);
CREATE INDEX items_owner_idx ON items (owner_id);
`,
		up: `-- create "owners" table
CREATE TABLE "owners" (
  "id" bigint PRIMARY KEY NOT NULL,
  "name" varchar
);
-- create "items" table
CREATE TABLE "items" (
  "id" bigint PRIMARY KEY NOT NULL,
  "owner_id" bigint NOT NULL,
  "note" text DEFAULT 'a'
);
-- create index "items_owner_idx" to table: "items"
CREATE INDEX "items_owner_idx" ON "items" ("owner_id");
-- modify "items" table
ALTER TABLE "items" ADD CONSTRAINT "items_note_len" CHECK (length(note) < 100);
-- modify "items" table
ALTER TABLE "items" ADD CONSTRAINT "items_owner_id_fkey" FOREIGN KEY ("owner_id") REFERENCES "owners"("id") ON DELETE CASCADE;
`,
		down: `-- reverse: modify "items" table
ALTER TABLE "items" DROP CONSTRAINT "items_owner_id_fkey";
-- reverse: create index "items_owner_idx" to table: "items"
DROP INDEX "items_owner_idx";
-- reverse: modify "items" table
ALTER TABLE "items" DROP CONSTRAINT "items_note_len";
-- reverse: create "items" table
DROP TABLE "items";
-- reverse: create "owners" table
DROP TABLE "owners";
`,
	},
	{
		name: "v2",
		schema: `CREATE TABLE owners (id bigint PRIMARY KEY, name varchar, email varchar);
CREATE TABLE items (
  id bigint PRIMARY KEY,
  owner_id bigint NOT NULL REFERENCES owners (id) ON DELETE CASCADE,
  note text DEFAULT 'a',
  CONSTRAINT items_note_len CHECK (length(note) < 100)
);
CREATE INDEX items_owner_idx ON items (owner_id);
`,
		up: `-- modify "owners" table
ALTER TABLE "owners" ADD COLUMN "email" varchar;
`,
		down: `-- reverse: modify "owners" table
ALTER TABLE "owners" DROP COLUMN "email";
`,
	},
	{
		name: "v3",
		schema: `CREATE TABLE owners (id bigint PRIMARY KEY, email varchar);
CREATE TABLE items (
  id bigint PRIMARY KEY,
  owner_id bigint NOT NULL REFERENCES owners (id) ON DELETE CASCADE,
  note varchar(200) DEFAULT 'a',
  CONSTRAINT items_note_len CHECK (length(note) < 100)
);
CREATE INDEX items_owner_idx ON items (owner_id, id);
CREATE TABLE tags (id bigint PRIMARY KEY, item_id bigint REFERENCES items (id), label text NOT NULL);
`,
		up: `-- create "tags" table
CREATE TABLE "tags" (
  "id" bigint PRIMARY KEY NOT NULL,
  "item_id" bigint,
  "label" text NOT NULL
);
-- modify "items" table
ALTER TABLE "items" ALTER COLUMN "note" TYPE varchar(200);
-- drop index "items_owner_idx" from table: "items"
DROP INDEX "items_owner_idx";
-- create index "items_owner_idx" to table: "items"
CREATE INDEX "items_owner_idx" ON "items" ("owner_id", "id");
-- modify "tags" table
ALTER TABLE "tags" ADD CONSTRAINT "tags_item_id_fkey" FOREIGN KEY ("item_id") REFERENCES "items"("id");
-- modify "owners" table
ALTER TABLE "owners" DROP COLUMN "name";
`,
		down: `-- reverse: modify "tags" table
ALTER TABLE "tags" DROP CONSTRAINT "tags_item_id_fkey";
-- reverse: modify "items" table
ALTER TABLE "items" ALTER COLUMN "note" TYPE text;
-- reverse: modify "owners" table
ALTER TABLE "owners" ADD COLUMN "name" character varying;
-- reverse: create index "items_owner_idx" to table: "items"
DROP INDEX "items_owner_idx";
-- reverse: drop index "items_owner_idx" from table: "items"
CREATE INDEX "items_owner_idx" ON "items" ("owner_id");
-- reverse: create "tags" table
DROP TABLE "tags";
`,
	},
}

// chargesTables are the tables of the OpenMeter shapes: a charge kind per
// table, and the tax codes a charge names.
const chargesTables = `CREATE TABLE tax_codes (id character(26) PRIMARY KEY, name character varying NOT NULL, external_reference character varying);
CREATE TABLE charge_flat_fees (id character(26) PRIMARY KEY, namespace character varying NOT NULL, feature_key %s, status_detailed character varying);
CREATE TABLE charge_usage_based (id character(26) PRIMARY KEY, namespace character varying NOT NULL, feature_key character varying, status_detailed character varying);
`

// chargesSearch is the view over both charge tables, the shape OpenMeter's
// charges_search_v1s view has: a UNION ALL, which the view replace test
// cannot read, so a rollback rebuilds the view.
const chargesSearch = `CREATE VIEW charges_search AS
SELECT id, namespace, feature_key, 'flat_fee' AS type, status_detailed FROM charge_flat_fees
UNION ALL
SELECT id, namespace, feature_key, 'usage_based' AS type, status_detailed FROM charge_usage_based;
`

// chargesSearchHeld is the view as PostgreSQL 15 prints it back, which is the
// body a rollback recreates it from.
const chargesSearchHeld = `SELECT charge_flat_fees.id,
    charge_flat_fees.namespace,
    charge_flat_fees.feature_key,
    'flat_fee'::text AS type,
    charge_flat_fees.status_detailed
   FROM charge_flat_fees
UNION ALL
 SELECT charge_usage_based.id,
    charge_usage_based.namespace,
    charge_usage_based.feature_key,
    'usage_based'::text AS type,
    charge_usage_based.status_detailed
   FROM charge_usage_based;
`

const chargesSearchDeclared = `SELECT id, namespace, feature_key, 'flat_fee' AS type, status_detailed FROM charge_flat_fees
UNION ALL
SELECT id, namespace, feature_key, 'usage_based' AS type, status_detailed FROM charge_usage_based;
`

// chargesFixture is the OpenMeter shapes of stokaro/ptah#4921, each as a step
// of its own: a column a table gains, a column a view gains, and a column a
// view reads changing its type. The last step removes the view together with
// a column it reads, the one shape here where the plan relies on CASCADE.
var chargesFixture = []statementStyleStep{
	{
		name: "base",
		schema: `CREATE TABLE tax_codes (id character(26) PRIMARY KEY, name character varying NOT NULL);
CREATE TABLE charge_flat_fees (id character(26) PRIMARY KEY, namespace character varying NOT NULL, feature_key character varying, status_detailed character varying);
CREATE TABLE charge_usage_based (id character(26) PRIMARY KEY, namespace character varying NOT NULL, feature_key character varying, status_detailed character varying);
CREATE VIEW charges_search AS
SELECT id, namespace, feature_key, 'flat_fee' AS type FROM charge_flat_fees
UNION ALL
SELECT id, namespace, feature_key, 'usage_based' AS type FROM charge_usage_based;
`,
	},
	{
		name:   "add_tax_code_external_reference",
		schema: strings.Replace(chargesTables, "%s", "character varying", 1) + strings.Replace(chargesSearch, ", status_detailed FROM", " FROM", 2),
		up: `-- modify "tax_codes" table
ALTER TABLE "tax_codes" ADD COLUMN "external_reference" character varying;
`,
		down: `-- reverse: modify "tax_codes" table
ALTER TABLE "tax_codes" DROP COLUMN "external_reference";
`,
	},
	{
		name:   "add_status_detailed_to_charges_search",
		schema: strings.Replace(chargesTables, "%s", "character varying", 1) + chargesSearch,
		up: `-- modify "charges_search" view
CREATE OR REPLACE VIEW "charges_search" AS
` + chargesSearchDeclared,
		down: `-- reverse: create "charges_search" view
DROP VIEW "charges_search";
-- reverse: drop "charges_search" view
CREATE VIEW "charges_search" AS
SELECT charge_flat_fees.id,
    charge_flat_fees.namespace,
    charge_flat_fees.feature_key,
    'flat_fee'::text AS type
   FROM charge_flat_fees
UNION ALL
 SELECT charge_usage_based.id,
    charge_usage_based.namespace,
    charge_usage_based.feature_key,
    'usage_based'::text AS type
   FROM charge_usage_based;
`,
	},
	{
		name:   "limit_flat_fee_feature_key",
		schema: strings.Replace(chargesTables, "%s", "character varying(128)", 1) + chargesSearch,
		up: `-- drop "charges_search" view
DROP VIEW "charges_search";
-- modify "charge_flat_fees" table
ALTER TABLE "charge_flat_fees" ALTER COLUMN "feature_key" TYPE character varying(128);
-- create "charges_search" view
CREATE VIEW "charges_search" AS
` + chargesSearchDeclared,
		down: `-- reverse: create "charges_search" view
DROP VIEW "charges_search";
-- reverse: modify "charge_flat_fees" table
ALTER TABLE "charge_flat_fees" ALTER COLUMN "feature_key" TYPE character varying;
-- reverse: drop "charges_search" view
CREATE VIEW "charges_search" AS
` + chargesSearchHeld,
	},
	{
		name: "drop_flat_fee_feature_key",
		schema: `CREATE TABLE tax_codes (id character(26) PRIMARY KEY, name character varying NOT NULL, external_reference character varying);
CREATE TABLE charge_flat_fees (id character(26) PRIMARY KEY, namespace character varying NOT NULL, status_detailed character varying);
CREATE TABLE charge_usage_based (id character(26) PRIMARY KEY, namespace character varying NOT NULL, feature_key character varying, status_detailed character varying);
`,
		up: `-- modify "charge_flat_fees" table
ALTER TABLE "charge_flat_fees" DROP COLUMN "feature_key" CASCADE;
-- drop "charges_search" view
DROP VIEW IF EXISTS "charges_search";
`,
		down: `-- reverse: modify "charge_flat_fees" table
ALTER TABLE "charge_flat_fees" ADD COLUMN "feature_key" VARCHAR(128);
-- reverse: drop "charges_search" view
CREATE VIEW "charges_search" AS
` + chargesSearchHeld,
	},
}

// diffStatementStyleSteps runs `migrate diff` once per step into one
// golang-migrate directory, with the format OpenMeter's atlas.hcl sets, and
// returns the directory. A run after the last step finds it in sync.
func diffStatementStyleSteps(c *qt.C, steps []statementStyleStep) string {
	c.Helper()
	dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_style_dev"), "public")
	work := c.TempDir()
	dir := filepath.Join(work, "migrations")
	diff := func(name, schema string) (string, error) {
		path := filepath.Join(work, name+".sql")
		c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
		return runCompatVerb("migrate", "diff", name,
			"--dir", "file://"+filepath.ToSlash(dir)+"?format=golang-migrate",
			"--to", "file://"+filepath.ToSlash(path),
			"--dev-url", dev,
			"--format", `{{ sql . "  " }}`)
	}
	for _, step := range steps {
		output, err := diff(step.name, step.schema)
		c.Assert(err, qt.IsNil, qt.Commentf("step %s:\n%s", step.name, output))
	}
	again, err := diff("again", steps[len(steps)-1].schema)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", again))
	c.Assert(again, qt.Contains, "The migration directory is synced with the desired state")
	return dir
}

// migrationPair is the up and down file one step wrote, in version order.
type migrationPair struct {
	Up, Down string
}

func readMigrationPairs(c *qt.C, dir string) []migrationPair {
	c.Helper()
	ups, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	slices.Sort(ups)
	pairs := make([]migrationPair, 0, len(ups))
	for _, up := range ups {
		upSQL, err := os.ReadFile(up)
		c.Assert(err, qt.IsNil)
		downSQL, err := os.ReadFile(strings.TrimSuffix(up, ".up.sql") + ".down.sql")
		c.Assert(err, qt.IsNil)
		pairs = append(pairs, migrationPair{Up: string(upSQL), Down: string(downSQL)})
	}
	return pairs
}

// wantPairs is the files steps are expected to write, the first step's
// included when it has them.
func wantPairs(steps []statementStyleStep) []migrationPair {
	pairs := make([]migrationPair, 0, len(steps))
	for _, step := range steps {
		pairs = append(pairs, migrationPair{Up: step.up, Down: step.down})
	}
	return pairs
}

// statementStyleTarget is an empty database the written files are applied to.
func statementStyleTarget(c *qt.C) *sql.DB {
	c.Helper()
	db, err := sql.Open("pgx", postgresScratchDevURL(c, "ptah_style_target"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return db
}

// publicRelations names the tables and views of the target's public schema,
// sorted.
func publicRelations(c *qt.C, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public' ORDER BY table_name`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// TestCompatMigrateDiffStatementStyleE2E_WritesTheCommunityBinarysStatements
// diffs the three steps stokaro/ptah#4921 measured into an empty golang-migrate
// directory. Each file carries the community binary's change comment above each
// statement, `reverse: ` and the undone change in a rollback, no IF EXISTS or IF
// NOT EXISTS and no CASCADE. The files then run on PostgreSQL forward, back to
// an empty database, and forward again.
func TestCompatMigrateDiffStatementStyleE2E_WritesTheCommunityBinarysStatements(t *testing.T) {
	c := qt.New(t)

	dir := diffStatementStyleSteps(c, statementStyleFixture)

	pairs := readMigrationPairs(c, dir)
	c.Assert(pairs, qt.DeepEquals, wantPairs(statementStyleFixture))
	target := statementStyleTarget(c)
	for _, pair := range pairs {
		_, err := target.ExecContext(c.Context(), pair.Up)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Up))
	}
	c.Assert(publicRelations(c, target), qt.DeepEquals, []string{"items", "owners", "tags"})
	for _, pair := range slices.Backward(pairs) {
		_, err := target.ExecContext(c.Context(), pair.Down)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Down))
	}
	c.Assert(publicRelations(c, target), qt.HasLen, 0)
	for _, pair := range pairs {
		_, err := target.ExecContext(c.Context(), pair.Up)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Up))
	}
	c.Assert(publicRelations(c, target), qt.DeepEquals, []string{"items", "owners", "tags"})
}

// TestCompatMigrateDiffStatementStyleE2E_TheOpenMeterShapes diffs the OpenMeter
// shapes and runs the files on PostgreSQL. The view that a column type change
// runs under is dropped without CASCADE, and the column a rollback drops is
// dropped without it: the plan drops and recreates every object it knows reads
// them. The last step drops a column a view the declaration removes still
// reads, and only there does the plan rely on CASCADE, and on IF EXISTS for the
// view the CASCADE has already taken.
func TestCompatMigrateDiffStatementStyleE2E_TheOpenMeterShapes(t *testing.T) {
	c := qt.New(t)

	dir := diffStatementStyleSteps(c, chargesFixture)

	pairs := readMigrationPairs(c, dir)
	c.Assert(pairs[1:], qt.DeepEquals, wantPairs(chargesFixture[1:]))
	target := statementStyleTarget(c)
	for _, pair := range pairs {
		_, err := target.ExecContext(c.Context(), pair.Up)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Up))
	}
	c.Assert(publicRelations(c, target), qt.DeepEquals, []string{"charge_flat_fees", "charge_usage_based", "tax_codes"})
	for _, pair := range slices.Backward(pairs) {
		_, err := target.ExecContext(c.Context(), pair.Down)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Down))
	}
	c.Assert(publicRelations(c, target), qt.HasLen, 0)
}

// TestCompatMigrateDiffStatementStyleE2E_ARunAgainstAnotherStateStops runs the
// written files against databases they were not planned for, which is what a
// guard or CASCADE would have hidden. A rollback run twice stops at the
// column it already dropped, where IF EXISTS would make the second run do
// nothing. A view a schema the project does not declare holds stops the
// column type change that rebuilds charges_search, where CASCADE would drop it
// in silence; it is still there afterwards.
func TestCompatMigrateDiffStatementStyleE2E_ARunAgainstAnotherStateStops(t *testing.T) {
	c := qt.New(t)
	pairs := readMigrationPairs(c, diffStatementStyleSteps(c, chargesFixture[:4]))
	target := statementStyleTarget(c)
	for _, pair := range pairs[:3] {
		_, err := target.ExecContext(c.Context(), pair.Up)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Up))
	}
	_, err := target.ExecContext(c.Context(), pairs[1].Down)
	c.Assert(err, qt.IsNil)
	_, err = target.ExecContext(c.Context(), `CREATE SCHEMA reporting;
CREATE VIEW reporting.charges_copy AS SELECT id FROM public.charges_search;`)
	c.Assert(err, qt.IsNil)

	_, again := target.ExecContext(c.Context(), pairs[1].Down)
	_, rebuild := target.ExecContext(c.Context(), pairs[3].Up)

	var undefinedColumn *pgconn.PgError
	c.Assert(again, qt.ErrorAs, &undefinedColumn)
	c.Assert(undefinedColumn.Code, qt.Equals, "42703")
	var dependent *pgconn.PgError
	c.Assert(rebuild, qt.ErrorAs, &dependent)
	c.Assert(dependent.Code, qt.Equals, "2BP01")
	var copies int
	c.Assert(target.QueryRowContext(c.Context(),
		`SELECT count(*) FROM information_schema.views WHERE table_schema = 'reporting' AND table_name = 'charges_copy'`,
	).Scan(&copies), qt.IsNil)
	c.Assert(copies, qt.Equals, 1)
}

// materializedViewFixture is stokaro/ptah#4964 as filed: a materialized view
// whose body changes, and a view that reads it.
var materializedViewFixture = []statementStyleStep{
	{
		name: "base",
		schema: `CREATE TABLE orders (id integer PRIMARY KEY, amount numeric NOT NULL);
CREATE MATERIALIZED VIEW order_totals AS SELECT sum(amount) AS total FROM orders;
CREATE VIEW order_report AS SELECT total FROM order_totals;
`,
	},
	{
		name: "count_orders",
		schema: `CREATE TABLE orders (id integer PRIMARY KEY, amount numeric NOT NULL);
CREATE MATERIALIZED VIEW order_totals AS SELECT sum(amount) AS total, count(*) AS n FROM orders;
CREATE VIEW order_report AS SELECT total FROM order_totals;
`,
		up: `-- drop "order_report" view
DROP VIEW "order_report";
-- drop "order_totals" materialized view
DROP MATERIALIZED VIEW "order_totals";
-- create "order_totals" materialized view
CREATE MATERIALIZED VIEW "order_totals" AS
SELECT sum(amount) AS total, count(*) AS n FROM orders;
-- create "order_report" view
CREATE VIEW "order_report" AS
SELECT total FROM order_totals;
`,
		down: `-- reverse: create "order_report" view
DROP VIEW "order_report";
-- reverse: create "order_totals" materialized view
DROP MATERIALIZED VIEW "order_totals";
-- reverse: drop "order_totals" materialized view
CREATE MATERIALIZED VIEW "order_totals" AS
SELECT sum(amount) AS total
   FROM orders;
-- reverse: drop "order_report" view
CREATE VIEW "order_report" AS
SELECT total
   FROM order_totals;
`,
	},
}

// TestCompatMigrateDiffStatementStyleE2E_AMaterializedViewChangeKeepsItsReaders
// diffs stokaro/ptah#4964 as filed and runs the files on PostgreSQL. The plan
// drops the view that reads the materialized view before it and creates the
// view again after it, both directions without CASCADE, so the database holds
// the view after the change and after its rollback, and a run after the change
// finds the directory in sync.
func TestCompatMigrateDiffStatementStyleE2E_AMaterializedViewChangeKeepsItsReaders(t *testing.T) {
	c := qt.New(t)

	dir := diffStatementStyleSteps(c, materializedViewFixture)

	pairs := readMigrationPairs(c, dir)
	c.Assert(pairs[1:], qt.DeepEquals, wantPairs(materializedViewFixture[1:]))
	target := statementStyleTarget(c)
	for _, pair := range pairs {
		_, err := target.ExecContext(c.Context(), pair.Up)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", pair.Up))
	}
	c.Assert(publicRelations(c, target), qt.DeepEquals, []string{"order_report", "orders"})
	_, err := target.ExecContext(c.Context(), pairs[1].Down)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", pairs[1].Down))
	c.Assert(publicRelations(c, target), qt.DeepEquals, []string{"order_report", "orders"})
}

// TestCompatMigrateDiffStatementStyleE2E_RefusesARetypeThatDropsAPolicy diffs
// stokaro/ptah#4965 as filed. The column type change drops the view a policy
// reads, so `migrate diff` refuses it and names the policy, where the file it
// would write stops at the drop on PostgreSQL. The directory keeps the first
// step only.
func TestCompatMigrateDiffStatementStyleE2E_RefusesARetypeThatDropsAPolicy(t *testing.T) {
	c := qt.New(t)
	const schema = `CREATE TABLE tenants (id %s PRIMARY KEY, active boolean NOT NULL);
CREATE VIEW active_tenants AS SELECT id FROM tenants WHERE active;
CREATE TABLE orders (id integer PRIMARY KEY, tenant_id integer NOT NULL);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_visible ON orders USING (tenant_id IN (SELECT id FROM active_tenants));
`
	dir := diffStatementStyleSteps(c, []statementStyleStep{{name: "base", schema: strings.Replace(schema, "%s", "integer", 1)}})
	retyped := filepath.Join(c.TempDir(), "retyped.sql")
	c.Assert(os.WriteFile(retyped, []byte(strings.Replace(schema, "%s", "bigint", 1)), 0o600), qt.IsNil)

	output, err := runCompatVerb("migrate", "diff", "widen_tenant_id",
		"--dir", "file://"+filepath.ToSlash(dir)+"?format=golang-migrate",
		"--to", "file://"+filepath.ToSlash(retyped),
		"--dev-url", pinnedDevURL(c, postgresScratchDevURL(c, "ptah_style_dev"), "public"))

	c.Assert(err, qt.ErrorMatches, `(?s).*drops active_tenants with CASCADE, which also drops ptah\.run/pgpolicy/policy public\.orders\.tenant_visible, whose definition reads it.*`, qt.Commentf("%s", output))
	c.Assert(readMigrationPairs(c, dir), qt.HasLen, 1)
}
