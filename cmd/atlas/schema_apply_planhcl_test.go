package atlas_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasschema"
)

// The fixtures under testdata are the real measurement artifacts:
// atlas.plan.hcl was written by the licensed Atlas binary
// (v1.2.4-e282f76-canary) and atlas-plan-desired.sql is the desired state the
// plan was computed for. The from-state DDL below recreates the scenario's
// source database.
const (
	oracleAtlasPlanFile   = "testdata/atlas.plan.hcl"
	oracleDesiredFile     = "testdata/atlas-plan-desired.sql"
	oracleFromStateSchema = `CREATE TABLE users (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, name text NOT NULL);`
)

// oracleFixturePath returns the absolute path of a testdata fixture so plan
// and schema URLs stay valid regardless of the working directory.
func oracleFixturePath(c *qt.C, name string) string {
	c.Helper()
	path, err := filepath.Abs(name)
	c.Assert(err, qt.IsNil)
	return path
}

func sqliteColumnCount(c *qt.C, dbPath, table, column string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	row := conn.QueryRowContext(
		context.Background(),
		`SELECT count(*) FROM pragma_table_info(?) WHERE name = ?`,
		table, column,
	)
	var count int
	c.Assert(row.Scan(&count), qt.IsNil)
	return count
}

func sqliteIndexCount(c *qt.C, dbPath, index string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	row := conn.QueryRowContext(
		context.Background(),
		`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = ?`,
		index,
	)
	var count int
	c.Assert(row.Scan(&count), qt.IsNil)
	return count
}

func TestSchemaApplyAtlasOraclePlanFileAppliesAndVerifies(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "oracle.db")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath,
		oracleFixturePath(c, oracleAtlasPlanFile),
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--auto-approve",
	)

	// The Atlas-authored plan executes end to end: the foreign hashes are
	// re-verified by rehearsing the plan on an ephemeral SQLite dev database
	// against --to, the plan's exact SQL applies, and the post-apply
	// end-state verification passes.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Planned schema changes:")
	c.Assert(out, qt.Contains, "ALTER TABLE `users` ADD COLUMN `email` text NULL")
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "posts"), qt.Equals, 1)
	c.Assert(sqliteColumnCount(c, dbPath, "users", "email"), qt.Equals, 1)
	c.Assert(sqliteIndexCount(c, dbPath, "idx_posts_user_id"), qt.Equals, 1)
}

func TestSchemaApplyAtlasPlanFileWithExplicitDevURL(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "oracle-dev.db")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)

	// The exact invocation shape measured against the official binary:
	// apply --url --to --dev-url --plan --auto-approve.
	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath,
		oracleFixturePath(c, oracleAtlasPlanFile),
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "posts"), qt.Equals, 1)
}

func TestSchemaApplyAtlasPlanFileRequiresTo(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "oracle-noto.db")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)

	_, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath,
		oracleFixturePath(c, oracleAtlasPlanFile),
		"--auto-approve",
	)

	// The official binary's contract and error, verbatim: an Atlas plan file
	// carries nothing Ptah can verify without the desired state.
	c.Assert(err, qt.ErrorMatches, `the flag "to" is required to verify the provided plan`)
	c.Assert(sqliteTableCount(c, dbPath, "posts"), qt.Equals, 0)
}

func TestSchemaApplyAtlasPlanFileRefusesDriftedTarget(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "oracle-drift.db")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
	// The database drifts after the plan was computed.
	seedSQLiteSchema(c, dbPath, `CREATE TABLE drifted (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath,
		oracleFixturePath(c, oracleAtlasPlanFile),
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--auto-approve",
	)

	// Ptah cannot compare the plan's Atlas hashes, so the drift surfaces
	// semantically: replaying the plan from the drifted schema does not
	// reach --to, and the apply refuses with the target untouched.
	c.Assert(err, qt.ErrorMatches, `(?s)pre-planned migration does not converge to the desired state:.*the plan was not applied to the target.*re-run .schema plan. against the current database and review the fresh plan`)
	c.Assert(atlasschema.IsPlanDesiredStateFailure(err), qt.IsTrue, qt.Commentf("%s", out))
	c.Assert(sqliteTableCount(c, dbPath, "posts"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c, dbPath, "drifted"), qt.Equals, 1)
}

func TestSchemaApplyAtlasPlanFileRefusesTamperedDesiredState(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "oracle-tampered.db")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
	// The desired state no longer matches what the plan was computed for:
	// a second --to source adds a table on top of the oracle desired state.
	extraPath := filepath.Join(dir, "tampered-extra.sql")
	c.Assert(os.WriteFile(extraPath, []byte(
		"CREATE TABLE tampered_extra (id integer NOT NULL PRIMARY KEY AUTOINCREMENT);\n",
	), 0o600), qt.IsNil)

	_, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath,
		oracleFixturePath(c, oracleAtlasPlanFile),
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--to", "file://"+extraPath,
		"--auto-approve",
	)

	// The rehearsed end state misses the tampered-in table, so the apply
	// refuses before the target is touched.
	c.Assert(err, qt.ErrorMatches, `(?s)pre-planned migration does not converge to the desired state:.*tampered_extra.*`)
	c.Assert(sqliteTableCount(c, dbPath, "posts"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c, dbPath, "tampered_extra"), qt.Equals, 0)
}

// writeAtlasPlanFile writes a hand-built Atlas-format plan file carrying the
// given migration body, the way a third party would hand one over.
func writeAtlasPlanFile(c *qt.C, path, migration string) {
	c.Helper()
	var body strings.Builder
	for line := range strings.SplitSeq(strings.TrimRight(migration, "\n"), "\n") {
		body.WriteString("  " + line + "\n")
	}
	document := "plan \"20260801102801\" {\n" +
		"  from      = \"2Avyplv6jw8kAsH/g2YFPkfnp+UNBpomMXPUl/4R4+Q=\"\n" +
		"  to        = \"YEugbm2aJqmXFA8dDrzmqLPC4tiNUrXe6YCrvazKOiY=\"\n" +
		"  migration = <<-SQL\n" + body.String() + "  SQL\n}\n"
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
}

// TestSchemaApplyAtlasPlanFileRefusesDevDatabaseEscape is the regression test
// for the plan-verification sandbox escape: a plan whose migration attaches
// another database file writes outside the dev database during what claims to
// be a rehearsal, and could even pass verification because the effect lands
// where the comparison never looks. The plan must be refused before anything
// executes anywhere.
func TestSchemaApplyAtlasPlanFileRefusesDevDatabaseEscape(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "escape-target.db")
	victimPath := filepath.Join(dir, "victim.db")
	planPath := filepath.Join(dir, "escape.plan.hcl")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
	seedSQLiteSchema(c, victimPath, `CREATE TABLE untouched (id INTEGER PRIMARY KEY);`)
	writeAtlasPlanFile(c, planPath,
		"ATTACH DATABASE '"+victimPath+"' AS victim;\nCREATE TABLE victim.pwned (id integer);")

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath,
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--auto-approve",
	)

	// Refused by name, and nothing ran: not on the dev database, not on the
	// target, and not on the third database the plan tried to reach.
	c.Assert(err, qt.ErrorMatches,
		`pre-planned migration was refused before it reached the dev database: statement 1 uses ATTACH, which attaches another SQLite database file.*`)
	c.Assert(err, qt.ErrorMatches, `(?s).*A dev database executes plan SQL for real.*`)
	c.Assert(out, qt.Contains, "refused before it reached the dev database")
	c.Assert(sqliteTableCount(c, victimPath, "pwned"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c, victimPath, "untouched"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, dbPath, "posts"), qt.Equals, 0)
	c.Assert(sqliteColumnCount(c, dbPath, "users", "email"), qt.Equals, 0)
}

// TestSchemaApplyAtlasPlanFileRefusesEscapeHiddenBehindValidChanges covers the
// second probe from the review: the escape rides along with statements that do
// converge to the desired state, so the end-state comparison would have passed
// while the payload landed in an attached file.
func TestSchemaApplyAtlasPlanFileRefusesEscapeHiddenBehindValidChanges(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "escape-hidden.db")
	victimPath := filepath.Join(dir, "hidden-victim.db")
	planPath := filepath.Join(dir, "hidden.plan.hcl")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
	seedSQLiteSchema(c, victimPath, `CREATE TABLE untouched (id INTEGER PRIMARY KEY);`)
	oraclePlan, err := os.ReadFile(oracleFixturePath(c, oracleAtlasPlanFile))
	c.Assert(err, qt.IsNil)
	// The real oracle migration plus a trailing escape.
	migration := strings.SplitN(string(oraclePlan), "migration = <<-SQL\n", 2)[1]
	migration = strings.SplitN(migration, "\n  SQL\n", 2)[0]
	migration = strings.ReplaceAll(migration, "\n  ", "\n")
	migration = strings.TrimPrefix(migration, "  ")
	writeAtlasPlanFile(c, planPath, migration+
		"\nATTACH DATABASE '"+victimPath+"' AS victim;\nCREATE TABLE victim.pwned (id integer);")

	_, err = runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath,
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches, `pre-planned migration was refused before it reached the dev database: statement 4 uses ATTACH.*`)
	c.Assert(sqliteTableCount(c, victimPath, "pwned"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c, dbPath, "posts"), qt.Equals, 0)
}

func TestSchemaApplyAtlasPlanFileDryRunPrintsWithoutApplying(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "oracle-dry.db")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath,
		oracleFixturePath(c, oracleAtlasPlanFile),
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--dry-run",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "CREATE TABLE `posts`")
	c.Assert(sqliteTableCount(c, dbPath, "posts"), qt.Equals, 0)
}

// TestSchemaApplyAtlasPlanFileDryRunRefusesDevDatabaseEscape pins that
// --dry-run performs the verification rather than printing and returning.
// Without it, restoring an early `if opts.dryRun { return nil }` in front of
// the rehearsal would leave the whole suite green while a dry run silently
// stopped checking anything. This case exits through the escape guard.
func TestSchemaApplyAtlasPlanFileDryRunRefusesDevDatabaseEscape(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dryrun-escape.db")
	victimPath := filepath.Join(dir, "dryrun-victim.db")
	planPath := filepath.Join(dir, "dryrun-escape.plan.hcl")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
	seedSQLiteSchema(c, victimPath, `CREATE TABLE untouched (id INTEGER PRIMARY KEY);`)
	writeAtlasPlanFile(c, planPath,
		"ATTACH DATABASE '"+victimPath+"' AS victim;\nCREATE TABLE victim.pwned (id integer);")

	_, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath,
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--dry-run",
	)

	c.Assert(err, qt.ErrorMatches,
		`pre-planned migration was refused before it reached the dev database: statement 1 uses ATTACH.*`)
	c.Assert(sqliteTableCount(c, victimPath, "pwned"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c, victimPath, "untouched"), qt.Equals, 1)
}

// TestSchemaApplyAtlasPlanFileDryRunRefusesNonConvergingPlan is the second
// half of the same pin, exiting through the desired-state comparison instead
// of the escape guard: a dry run must report that the plan does not reach the
// --to state, which is the whole point of test-driving a foreign plan.
func TestSchemaApplyAtlasPlanFileDryRunRefusesNonConvergingPlan(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dryrun-nonconverging.db")
	planPath := filepath.Join(dir, "dryrun-nonconverging.plan.hcl")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
	writeAtlasPlanFile(c, planPath, "CREATE TABLE unrelated (id integer NOT NULL PRIMARY KEY);")

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath,
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--dry-run",
	)

	c.Assert(err, qt.ErrorMatches, `(?s)pre-planned migration does not converge to the desired state.*`)
	c.Assert(atlasschema.IsPlanDesiredStateFailure(err), qt.IsTrue, qt.Commentf("%s", out))
	// A dry run still applies nothing to the target.
	c.Assert(sqliteTableCount(c, dbPath, "unrelated"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c, dbPath, "posts"), qt.Equals, 0)
}

func TestSchemaApplyJSONPlanWithToVerifiesEndState(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "json-to.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	desiredSQL := "CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE json_orders (id INTEGER PRIMARY KEY);\n"
	planPath := planFileFixture(c, dir, dbPath, desiredSQL)

	// --plan now combines with --to on the JSON path too: the plan applies
	// and the end state is verified against the desired schema.
	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath,
		"--to", "file://"+filepath.Join(dir, "desired.sql"),
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "json_orders"), qt.Equals, 1)
}

func TestSchemaApplyJSONPlanWithMismatchedToFailsPostApply(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "json-mismatch.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	planPath := planFileFixture(c, dir, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE planned_only (id INTEGER PRIMARY KEY);\n")
	otherDesiredPath := filepath.Join(dir, "other-desired.sql")
	c.Assert(os.WriteFile(otherDesiredPath, []byte(
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE somewhere_else (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath,
		"--to", "file://"+otherDesiredPath,
		"--auto-approve",
	)

	// The fingerprint gate passes (the database matches the plan's source),
	// the plan applies, and the always-on end-state verification then fails
	// loudly: the reached state is not the --to desired state.
	c.Assert(err, qt.ErrorMatches, `(?s)schema apply --plan end-state verification failed: after applying the plan, the database does not match the --to desired state.*`)
	c.Assert(atlasschema.IsPlanDesiredStateFailure(err), qt.IsTrue, qt.Commentf("%s", out))
	c.Assert(sqliteTableCount(c, dbPath, "planned_only"), qt.Equals, 1)
}

func TestSchemaApplyPtahWrittenHCLPlanRoundTrip(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "roundtrip.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	planPath := filepath.Join(dir, "roundtrip.plan.hcl")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	c.Assert(os.WriteFile(schemaPath, []byte(
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE rt_orders (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)

	planOut, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--output", planPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", planOut))

	// The saved .plan.hcl carries Ptah's native sha256 fingerprints, so the
	// apply verifies them AND rehearses the plan, then applies it.
	plan, format, err := atlasschema.ReadPlanDocument(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(format, qt.Equals, atlasschema.PlanFormatHCL)
	c.Assert(atlasschema.IsNativeFingerprint(plan.FromFingerprint), qt.IsTrue)

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath,
		"--to", "file://"+schemaPath,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "rt_orders"), qt.Equals, 1)
}

func TestSchemaApplyPtahWrittenHCLPlanRefusesStaleTarget(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "roundtrip-stale.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	planPath := filepath.Join(dir, "stale.plan.hcl")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	c.Assert(os.WriteFile(schemaPath, []byte(
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE stale_rt_orders (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	planOut, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--output", planPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", planOut))
	// The database drifts after the plan was computed.
	seedSQLiteSchema(c, dbPath, `CREATE TABLE drifted (id INTEGER PRIMARY KEY);`)

	_, err = runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath,
		"--to", "file://"+schemaPath,
		"--auto-approve",
	)

	// A Ptah-written .plan.hcl keeps the native fingerprint contract: the
	// drifted target refuses as stale before any rehearsal or execution.
	c.Assert(err, qt.ErrorMatches, `pre-planned migration is stale: .*`)
	c.Assert(sqliteTableCount(c, dbPath, "stale_rt_orders"), qt.Equals, 0)
}
