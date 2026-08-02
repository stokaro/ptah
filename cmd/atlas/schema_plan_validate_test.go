package atlas_test

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// `atlas schema plan validate` has no oracle. Its flag set comes from the
// published Atlas CLI reference (see schema_plan_subverb_surface_test.go); the
// checks it runs, their order, its exit code and its output are the reading
// this tree implements.
//
// The load-bearing rule IS documented: a plan's `from` must match the schema
// state it is validated against (https://atlasgo.io/hcl/testing), and Atlas's
// own apply path refuses an Atlas-format plan without --to with
// `the flag "to" is required to verify the provided plan`.

// validateFixture is a live SQLite target, a desired-state file, and a plan
// file computed between them.
type validateFixture struct {
	planFixture
	planPath string
	dbPath   string
}

// newValidateFixture seeds the target, writes the desired state, and produces
// a plan file for the transition using the command under test's sibling.
func newValidateFixture(c *qt.C, name, seedSQL, desiredSQL string) validateFixture {
	c.Helper()
	fixture := newPlanFixture(c, name, seedSQL, desiredSQL)
	planPath := filepath.Join(fixture.dir, name+".plan.hcl")
	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
		fixture.args("--output", planPath)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return validateFixture{
		planFixture: fixture,
		planPath:    planPath,
		dbPath:      filepath.Join(fixture.dir, name+".db"),
	}
}

func (f validateFixture) validateArgs(extra ...string) []string {
	return f.args(append([]string{"--file", "file://" + f.planPath}, extra...)...)
}

// execOnTarget runs a statement against the target, so a test can put data
// where a destructive dev reset would be visible.
func execOnTarget(c *qt.C, dbURL, statement string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.Exec(statement)
	c.Assert(err, qt.IsNil)
}

// readTargetSchema introspects the target so a test can prove validate left it
// alone.
func readTargetSchema(c *qt.C, dbURL string) *dbschematypes.DBSchema {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	schema, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	c.Assert(err, qt.IsNil)
	return schema
}

// targetSchemaFingerprint captures the complete introspected target shape in
// the same deterministic form used by native plan stale-state checks.
func targetSchemaFingerprint(c *qt.C, dbURL string) string {
	c.Helper()
	fingerprint, err := atlasschema.SchemaFingerprint(readTargetSchema(c, dbURL))
	c.Assert(err, qt.IsNil)
	return fingerprint
}

// TestSchemaPlanValidateAcceptsAMatchingPlanSilently pins the success contract:
// exit 0 with empty stdout and stderr.
//
// Empty stdout is the deliberate choice: no measurement settles what a
// successful Atlas plan validation prints, and an empty stdout cannot be the
// wrong shape.
func TestSchemaPlanValidateAcceptsAMatchingPlanSilently(t *testing.T) {
	c := qt.New(t)
	fixture := newValidateFixture(c, "validate-ok",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")

	stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "validate",
		fixture.validateArgs()...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
}

// TestSchemaPlanValidateAcceptsAtlasAuthoredPlan keeps validation independent
// from Ptah's plan writer. The plan file was produced by the standard Atlas
// distribution during the plan-format measurement campaign.
func TestSchemaPlanValidateAcceptsAtlasAuthoredPlan(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "oracle-validate.db")
	dbURL := sqliteURLFromPath(dbPath)
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
	execOnTarget(c, dbURL, `INSERT INTO users (id, name) VALUES (1, 'kept');`)
	beforeFingerprint := targetSchemaFingerprint(c, dbURL)

	stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "validate",
		"--from", dbURL,
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--file", "file://"+oracleFixturePath(c, oracleAtlasPlanFile),
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
	c.Assert(targetSchemaFingerprint(c, dbURL), qt.Equals, beforeFingerprint)
	c.Assert(sqliteRowCount(c, dbPath, "users"), qt.Equals, 1)
}

// TestSchemaPlanValidateTreatsForeignFingerprintsAsUnauthenticatedMetadata
// pins the limit of validating Atlas-authored plans. Atlas's base64 hashes
// have no public local derivation, so the semantic replay below is the
// authority; neither foreign fingerprint can serve as an integrity check.
func TestSchemaPlanValidateTreatsForeignFingerprintsAsUnauthenticatedMetadata(t *testing.T) {
	c := qt.New(t)
	oraclePlan, _, err := atlasschema.ReadPlanDocument(oracleFixturePath(c, oracleAtlasPlanFile))
	c.Assert(err, qt.IsNil)
	tests := []struct {
		name   string
		mutate func(*atlasschema.PlanFile)
	}{
		{
			name: "from fingerprint",
			mutate: func(plan *atlasschema.PlanFile) {
				plan.FromFingerprint = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
			},
		},
		{
			name: "to fingerprint",
			mutate: func(plan *atlasschema.PlanFile) {
				plan.ToFingerprint = "//////////////////////////////////////////8="
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "target.db")
			dbURL := sqliteURLFromPath(dbPath)
			planPath := filepath.Join(dir, "foreign-fingerprint.plan.hcl")
			seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
			execOnTarget(c, dbURL, `INSERT INTO users (id, name) VALUES (1, 'kept');`)
			beforeFingerprint := targetSchemaFingerprint(c, dbURL)
			plan := oraclePlan
			test.mutate(&plan)
			document, err := atlasschema.MarshalPlanFileAs(plan, atlasschema.PlanFormatHCL)
			c.Assert(err, qt.IsNil)
			c.Assert(os.WriteFile(planPath, document, 0o600), qt.IsNil)

			stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "validate",
				"--from", dbURL,
				"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
				"--file", "file://"+planPath,
			)

			c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
			c.Assert(targetSchemaFingerprint(c, dbURL), qt.Equals, beforeFingerprint)
			c.Assert(sqliteRowCount(c, dbPath, "users"), qt.Equals, 1)
		})
	}
}

func TestSchemaPlanValidateRejectsAtlasAuthoredPlanMutations(t *testing.T) {
	c := qt.New(t)
	oraclePlan, _, err := atlasschema.ReadPlanDocument(oracleFixturePath(c, oracleAtlasPlanFile))
	c.Assert(err, qt.IsNil)
	tests := []struct {
		name      string
		migration string
	}{
		{
			name:      "changed migration",
			migration: "CREATE TABLE unrelated (id INTEGER PRIMARY KEY);",
		},
		{
			name: "extra statement",
			migration: atlasPlanMigrationSQL(oraclePlan.Statements) +
				"\nCREATE TABLE unexpected (id INTEGER PRIMARY KEY);",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "target.db")
			dbURL := sqliteURLFromPath(dbPath)
			planPath := filepath.Join(dir, "mutated.plan.hcl")
			seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
			execOnTarget(c, dbURL, `INSERT INTO users (id, name) VALUES (1, 'kept');`)
			beforeFingerprint := targetSchemaFingerprint(c, dbURL)
			writeAtlasPlanFile(c, planPath, test.migration)

			out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
				"--from", dbURL,
				"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
				"--file", "file://"+planPath,
			)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Contains, "does not converge to the desired state")
			c.Assert(targetSchemaFingerprint(c, dbURL), qt.Equals, beforeFingerprint)
			c.Assert(sqliteRowCount(c, dbPath, "users"), qt.Equals, 1)
		})
	}
}

func TestSchemaPlanValidateRejectsMalformedAtlasPlanWithoutMutation(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	dbURL := sqliteURLFromPath(dbPath)
	planPath := filepath.Join(dir, "malformed.plan.hcl")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
	execOnTarget(c, dbURL, `INSERT INTO users (id, name) VALUES (1, 'kept');`)
	beforeFingerprint := targetSchemaFingerprint(c, dbURL)
	c.Assert(os.WriteFile(planPath, []byte("plan {"), 0o600), qt.IsNil)

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
		"--from", dbURL,
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--file", "file://"+planPath,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "parse plan file")
	c.Assert(targetSchemaFingerprint(c, dbURL), qt.Equals, beforeFingerprint)
	c.Assert(sqliteRowCount(c, dbPath, "users"), qt.Equals, 1)
}

func TestSchemaPlanValidateRejectsDriftedSourceForAtlasAuthoredPlan(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "drifted-target.db")
	dbURL := sqliteURLFromPath(dbPath)
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
	seedSQLiteSchema(c, dbPath, "CREATE TABLE drifted (id INTEGER PRIMARY KEY);")
	execOnTarget(c, dbURL, `INSERT INTO users (id, name) VALUES (1, 'kept');`)
	beforeFingerprint := targetSchemaFingerprint(c, dbURL)

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
		"--from", dbURL,
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--file", "file://"+oracleFixturePath(c, oracleAtlasPlanFile),
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "does not converge to the desired state")
	c.Assert(targetSchemaFingerprint(c, dbURL), qt.Equals, beforeFingerprint)
	c.Assert(sqliteRowCount(c, dbPath, "users"), qt.Equals, 1)
}

func TestSchemaPlanValidateRejectsChangedDesiredStateForAtlasAuthoredPlan(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	dbURL := sqliteURLFromPath(dbPath)
	extraDesiredPath := filepath.Join(dir, "extra-desired.sql")
	seedSQLiteSchema(c, dbPath, oracleFromStateSchema)
	execOnTarget(c, dbURL, `INSERT INTO users (id, name) VALUES (1, 'kept');`)
	beforeFingerprint := targetSchemaFingerprint(c, dbURL)
	c.Assert(os.WriteFile(extraDesiredPath, []byte(
		"CREATE TABLE desired_extra (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
		"--from", dbURL,
		"--to", "file://"+oracleFixturePath(c, oracleDesiredFile),
		"--to", "file://"+extraDesiredPath,
		"--file", "file://"+oracleFixturePath(c, oracleAtlasPlanFile),
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "does not converge to the desired state")
	c.Assert(targetSchemaFingerprint(c, dbURL), qt.Equals, beforeFingerprint)
	c.Assert(sqliteRowCount(c, dbPath, "users"), qt.Equals, 1)
}

func atlasPlanMigrationSQL(statements []atlasschema.PlanStatement) string {
	sql := make([]string, len(statements))
	for i, statement := range statements {
		sql[i] = statement.SQL
	}
	return strings.Join(sql, ";\n") + ";"
}

// TestSchemaPlanValidateLeavesTheTargetDatabaseUnchanged asserts the protected
// state rather than the proxy. `validate` reuses the apply path's verification,
// which replays the plan's SQL for real — on a dev database. If that replay
// ever reached the target, exit 0 would still be reported and the schema would
// silently be migrated by a command whose whole contract is that it does not
// migrate anything.
//
// The two subtests are not variations on one theme. The first is a control: with
// no --dev-url there is an ephemeral dev database and nothing could have touched
// the target, so it can only ever pass. The second is the one that carries the
// weight — it points --dev-url AT the target, which is the single input where the
// rehearsal would destroy it, because a dev database is reset destructively
// before the plan is replayed on it. Only the second can fail.
func TestSchemaPlanValidateLeavesTheTargetDatabaseUnchanged(t *testing.T) {
	c := qt.New(t)

	// newSeededFixture builds the fixture and puts rows in the target, so the
	// assertions can speak about data and not only about table names. A
	// destructive dev reset takes the rows with it.
	newSeededFixture := func(c *qt.C, name string) validateFixture {
		c.Helper()
		fixture := newValidateFixture(c, name,
			`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
			"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")
		execOnTarget(c, fixture.dbURL, `INSERT INTO keep_me (id) VALUES (1), (2), (3);`)
		return fixture
	}

	c.Run("control_no_dev_url_cannot_touch_the_target", func(c *qt.C) {
		fixture := newSeededFixture(c, "validate-readonly")
		before := readTargetSchema(c, fixture.dbURL)

		stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "validate",
			fixture.validateArgs()...)

		c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
		after := readTargetSchema(c, fixture.dbURL)
		c.Assert(tableNames(after), qt.DeepEquals, tableNames(before))
		c.Assert(tableNames(after), qt.DeepEquals, []string{"keep_me"})
		c.Assert(countRows(c, fixture.dbPath, "keep_me"), qt.Equals, 3)
	})

	// A dev database is reset destructively before the plan is replayed on it,
	// so a --dev-url that resolves to the target turns `validate` into the one
	// thing it promises never to be: a command that migrates the target. The
	// refusal lives in atlasschema.connectSimulationDev and only fires because
	// runAtlasSchemaPlanValidate hands it the target URL; drop that one field
	// and the whole suite stays green while this input destroys the database.
	c.Run("dev_url_pointing_at_the_target_is_refused", func(c *qt.C) {
		fixture := newSeededFixture(c, "validate-devurl-is-target")
		before := readTargetSchema(c, fixture.dbURL)

		out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
			fixture.validateArgs("--dev-url", fixture.dbURL)...)

		c.Assert(err, qt.IsNotNil)
		c.Assert(out, qt.Contains, "--dev-url must not point at the target database")
		// The protected state, not the message: the refusal is worthless if the
		// reset already ran. `added` is the plan's new table — its presence would
		// prove the plan was applied to the target.
		after := readTargetSchema(c, fixture.dbURL)
		c.Assert(tableNames(after), qt.DeepEquals, tableNames(before))
		c.Assert(tableNames(after), qt.DeepEquals, []string{"keep_me"})
		c.Assert(countRows(c, fixture.dbPath, "keep_me"), qt.Equals, 3)
	})

	c.Run("dev_url_path_alias_of_the_target_is_refused", func(c *qt.C) {
		fixture := newSeededFixture(c, "validate-devurl-path-alias")
		beforeFingerprint := targetSchemaFingerprint(c, fixture.dbURL)
		aliasPath := filepath.Dir(fixture.dbPath) + string(os.PathSeparator) + "." +
			string(os.PathSeparator) + filepath.Base(fixture.dbPath)
		aliasURL := sqliteURLFromPath(aliasPath) + "?mode=rwc"

		out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
			fixture.validateArgs("--dev-url", aliasURL)...)

		c.Assert(err, qt.IsNotNil)
		c.Assert(out, qt.Contains, "--dev-url must not point at the target database")
		c.Assert(targetSchemaFingerprint(c, fixture.dbURL), qt.Equals, beforeFingerprint)
		c.Assert(countRows(c, fixture.dbPath, "keep_me"), qt.Equals, 3)
	})

	c.Run("percent_encoded_dev_url_alias_of_the_target_is_refused", func(c *qt.C) {
		fixture := newSeededFixture(c, "validate-devurl-percent-encoded-alias")
		beforeFingerprint := targetSchemaFingerprint(c, fixture.dbURL)
		aliasURL := "sqlite:file:" + url.PathEscape(filepath.ToSlash(fixture.dbPath)) + "?mode=rwc"

		out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
			fixture.validateArgs("--dev-url", aliasURL)...)

		c.Assert(err, qt.IsNotNil)
		c.Assert(out, qt.Contains, "--dev-url must not point at the target database")
		c.Assert(targetSchemaFingerprint(c, fixture.dbURL), qt.Equals, beforeFingerprint)
		c.Assert(countRows(c, fixture.dbPath, "keep_me"), qt.Equals, 3)
	})

	c.Run("dev_url_hard_link_of_the_target_is_refused", func(c *qt.C) {
		fixture := newSeededFixture(c, "validate-devurl-hard-link")
		beforeFingerprint := targetSchemaFingerprint(c, fixture.dbURL)
		aliasPath := filepath.Join(filepath.Dir(fixture.dbPath), "target-hard-link.db")
		c.Assert(os.Link(fixture.dbPath, aliasPath), qt.IsNil)

		out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
			fixture.validateArgs("--dev-url", sqliteURLFromPath(aliasPath))...)

		c.Assert(err, qt.IsNotNil)
		c.Assert(out, qt.Contains, "--dev-url must not point at the target database")
		c.Assert(targetSchemaFingerprint(c, fixture.dbURL), qt.Equals, beforeFingerprint)
		c.Assert(countRows(c, fixture.dbPath, "keep_me"), qt.Equals, 3)
	})
}

// TestSchemaPlanValidateHonorsTheExcludePatternsRecordedInThePlan pins the
// other half of the "validate must not be stricter than apply" invariant.
//
// A JSON plan records the --exclude patterns it was computed with, and the
// verification has to apply them or it compares against a state the plan never
// described: the target keeps an object the desired state does not mention, so
// an unfiltered comparison reports drift and refuses a plan that
// `schema apply --plan` accepts. That is the exact inverse of this PR's stated
// invariant, and it is invisible to every other test here, because every other
// fixture is computed without --exclude and so has nothing to drop.
func TestSchemaPlanValidateHonorsTheExcludePatternsRecordedInThePlan(t *testing.T) {
	c := qt.New(t)
	// audit_log lives on the target and is absent from --to. Only the plan's
	// recorded exclude patterns keep it out of the comparison.
	fixture := newPlanFixture(c, "validate-exclude",
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE audit_log (id INTEGER PRIMARY KEY);",
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")
	// The Atlas .plan.hcl shape has no field for exclude patterns and refuses to
	// record them, so this has to be the native JSON plan.
	planPath := filepath.Join(fixture.dir, "excluded.plan.json")
	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", planPath, "--exclude", "audit_log")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Exclude, qt.DeepEquals, []string{"audit_log"})

	stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "validate",
		fixture.args("--file", "file://"+planPath)...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
}

// TestSchemaPlanValidateRefusesAPlanThatDoesNotReachTheDesiredState is the
// discriminating pair for check (2). The plan is valid for ITS desired state
// and invalid for the one passed here; a validate that only checked the
// from-fingerprint, or only that the file parses, would accept both.
func TestSchemaPlanValidateRefusesAPlanThatDoesNotReachTheDesiredState(t *testing.T) {
	c := qt.New(t)
	fixture := newValidateFixture(c, "validate-drift",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")
	other := filepath.Join(fixture.dir, "other.sql")
	c.Assert(os.WriteFile(other, []byte(
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE different (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
		"--from", fixture.dbURL,
		"--to", "file://"+other,
		"--file", "file://"+fixture.planPath,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "does not converge to the desired state")
}

// TestSchemaPlanValidateRefusesAPlanWhoseSourceStateMovedOn covers check (1),
// the documented from-must-match rule, on an input where check (2) provably
// cannot catch the problem.
//
// Any ordinary drift on the target breaks BOTH checks at once — the replay is
// measured against the same --to — so an ordinary fixture would leave "check
// (1) exists" and "check (1) is dead code" indistinguishable. The separator is
// an IDEMPOTENT plan: `CREATE TABLE IF NOT EXISTS` replayed from the moved-on
// target still lands exactly on --to, so check (2) passes and only the
// fingerprint says the plan was computed against a database that no longer
// exists. The control below runs the same idempotent plan against an untouched
// target and requires it to validate.
func TestSchemaPlanValidateRefusesAPlanWhoseSourceStateMovedOn(t *testing.T) {
	c := qt.New(t)

	// idempotentPlan rewrites a fixture's plan file so its statements can run
	// against a target that already has the planned table.
	idempotentPlan := func(c *qt.C, fixture validateFixture) {
		c.Helper()
		plan, _, err := atlasschema.ReadPlanDocument(fixture.planPath)
		c.Assert(err, qt.IsNil)
		plan.Dialect = "sqlite"
		document, err := atlasschema.MarshalPlanFileAs(
			plan.WithStatementsFromSQL(`CREATE TABLE IF NOT EXISTS "added" ("id" INTEGER PRIMARY KEY);`),
			atlasschema.PlanFormatHCL)
		c.Assert(err, qt.IsNil)
		c.Assert(os.WriteFile(fixture.planPath, document, 0o600), qt.IsNil)
	}
	newFixture := func(c *qt.C, name string) validateFixture {
		c.Helper()
		return newValidateFixture(c, name,
			`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
			"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")
	}

	c.Run("control_untouched_target_validates", func(c *qt.C) {
		fixture := newFixture(c, "validate-stale-control")
		idempotentPlan(c, fixture)

		stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "validate",
			fixture.validateArgs()...)

		c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
		c.Assert(stdout, qt.Equals, "")
	})

	c.Run("moved_on_target_is_refused_as_stale", func(c *qt.C) {
		fixture := newFixture(c, "validate-stale")
		idempotentPlan(c, fixture)
		// The plan's own change, applied out of band. The target now equals the
		// desired state, so replaying the idempotent plan on it converges;
		// only the recorded from-fingerprint disagrees.
		seedSQLiteSchema(c, fixture.dbPath, `CREATE TABLE added (id INTEGER PRIMARY KEY);`)

		out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
			fixture.validateArgs()...)

		c.Assert(err, qt.IsNotNil)
		c.Assert(out, qt.Contains, "pre-planned migration is stale")
	})
}

// TestSchemaPlanValidateReplaysANativePlanWithoutADevURL pins the one place
// validate deliberately verifies MORE than `schema apply --plan`: a native JSON
// plan whose from-fingerprint matches is still replayed. Under the apply
// policy this input skips the replay entirely, so a validate that reused that
// policy would report this plan valid against a desired state it cannot reach.
func TestSchemaPlanValidateReplaysANativePlanWithoutADevURL(t *testing.T) {
	c := qt.New(t)
	fixture := newPlanFixture(c, "validate-json-replay",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")
	planPath := filepath.Join(fixture.dir, "native.plan.json")
	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
		fixture.args("--output", planPath)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	other := filepath.Join(fixture.dir, "other.sql")
	c.Assert(os.WriteFile(other, []byte(
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE different (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)

	// The from-fingerprint still matches the untouched target, so check (1)
	// passes; only the replay can see that the plan does not reach this --to.
	// No --dev-url is passed, which is exactly the input apply would skip.
	out, err = runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
		"--from", fixture.dbURL,
		"--to", "file://"+other,
		"--file", "file://"+planPath,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "does not converge to the desired state")
}

// TestSchemaPlanValidateAcceptsAnEditedPlanThatStillConverges is the other half
// of the discriminating pair: Atlas documents editing a saved plan's migration
// attribute, so validate must check WHERE the statements arrive, not whether
// they are spelled the way a fresh plan would spell them.
//
// A validate implemented as "recompute the plan and compare the SQL" passes
// every other test in this file and fails this one.
func TestSchemaPlanValidateAcceptsAnEditedPlanThatStillConverges(t *testing.T) {
	c := qt.New(t)
	fixture := newValidateFixture(c, "validate-edited",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY, note TEXT);")

	// Rewrite the plan's SQL to reach the same end state by a different route:
	// create the table, then add the column, instead of one CREATE TABLE.
	original, err := os.ReadFile(fixture.planPath)
	c.Assert(err, qt.IsNil)
	plan, planFormat, err := atlasschema.ReadPlanDocument(fixture.planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(planFormat, qt.Equals, atlasschema.PlanFormatHCL)
	plan.Dialect = "sqlite"
	edited := plan.WithStatementsFromSQL(
		"CREATE TABLE \"added\" (\n  \"id\" INTEGER PRIMARY KEY\n);\n" +
			"ALTER TABLE \"added\" ADD COLUMN \"note\" TEXT;\n")
	document, err := atlasschema.MarshalPlanFileAs(edited, atlasschema.PlanFormatHCL)
	c.Assert(err, qt.IsNil)
	c.Assert(string(document), qt.Not(qt.Equals), string(original))
	c.Assert(os.WriteFile(fixture.planPath, document, 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "validate",
		fixture.validateArgs()...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
}

// TestSchemaPlanValidateReadsTheNativeJSONPlanFormat proves the reader accepts
// both encodings, like `schema apply --plan`.
func TestSchemaPlanValidateReadsTheNativeJSONPlanFormat(t *testing.T) {
	c := qt.New(t)
	fixture := newPlanFixture(c, "validate-json",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")
	planPath := filepath.Join(fixture.dir, "native.plan.json")
	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
		fixture.args("--output", planPath)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "validate",
		fixture.args("--file", "file://"+planPath)...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
}

// TestSchemaPlanValidateChecksANativePlanWithAForeignLookingFingerprint pins
// that the JSON plan format, not the fingerprint SHAPE, decides whether check
// (1) runs. The `sha256:<hex>` form is public, so a plan file that simply does
// not use it must not be able to switch the from-state gate off.
func TestSchemaPlanValidateChecksANativePlanWithAForeignLookingFingerprint(t *testing.T) {
	c := qt.New(t)
	fixture := newPlanFixture(c, "validate-forged",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")
	planPath := filepath.Join(fixture.dir, "forged.plan.json")
	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
		fixture.args("--output", planPath)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	// Replace the recomputable fingerprint with an Atlas-looking Base64 one.
	// Nothing else changes, so an implementation keyed on IsNativeFingerprint
	// alone silently stops checking the source state.
	raw, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil)
	var document map[string]any
	c.Assert(json.Unmarshal(raw, &document), qt.IsNil)
	document["from_fingerprint"] = "kX0nWQb1sO0lC8mYq4dQeYPZ0m2m0m3m1m4m5m6m7m8="
	patched, err := json.Marshal(document)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(planPath, patched, 0o600), qt.IsNil)

	out, err = runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
		fixture.args("--file", "file://"+planPath)...)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "pre-planned migration is stale")
}

// TestSchemaPlanValidateRequiredInputs enumerates the missing-input branches.
// The --to case is separated from the --from case on purpose: substituting
// Atlas's `the flag "to" is required` wording for any error that happens to
// occur while --to is empty would swallow the more useful "--from is required".
func TestSchemaPlanValidateRequiredInputs(t *testing.T) {
	c := qt.New(t)
	fixture := newValidateFixture(c, "validate-required",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing_to",
			args: []string{"--from", fixture.dbURL, "--file", "file://" + fixture.planPath},
			want: `the flag "to" is required to verify the provided plan`,
		},
		{
			name: "missing_from",
			args: []string{"--to", fixture.schemaURL, "--file", "file://" + fixture.planPath},
			want: "--from is required",
		},
		{
			name: "missing_from_and_to",
			args: []string{"--file", "file://" + fixture.planPath},
			want: "--from is required",
		},
		{
			name: "missing_file",
			args: []string{"--from", fixture.dbURL, "--to", fixture.schemaURL},
			want: "--file is required: atlas schema plan validate validates an existing plan file",
		},
		{
			name: "from_is_a_schema_file",
			args: []string{"--from", fixture.schemaURL, "--to", fixture.schemaURL, "--file", "file://" + fixture.planPath},
			want: "atlas schema plan validate requires --from to be the target database URL",
		},
		{
			name: "registry_plan_url",
			args: []string{"--from", fixture.dbURL, "--to", fixture.schemaURL, "--file", "atlas://app/plans/x"},
			want: "but Ptah has no plan registry",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate", test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Contains, test.want)
		})
	}
}

// TestSchemaPlanValidateRefusesUnimplementedTransitionFlags mirrors the `new`
// refusal table and adds --exclude, which validate refuses for its own reason:
// the plan records the patterns it was computed with, and the Atlas plan format
// records none at all, so honoring flag-supplied patterns would verify a
// different transition than the plan describes.
func TestSchemaPlanValidateRefusesUnimplementedTransitionFlags(t *testing.T) {
	c := qt.New(t)
	fixture := newValidateFixture(c, "validate-reject",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "repo", args: []string{"--repo", "atlas://app"}, want: "accepts --repo, but schema repositories require a hosted registry"},
		{name: "format", args: []string{"--format", "{{ json . }}"}, want: "accepts --format, but Ptah does not implement"},
		{name: "lock_timeout", args: []string{"--lock-timeout", "10s"}, want: "accepts --lock-timeout, but Ptah does not implement"},
		{name: "include", args: []string{"--include", "public.*"}, want: "accepts --include, but Ptah only supports"},
		{name: "schema", args: []string{"--schema", "public"}, want: "accepts --schema, but Ptah only supports"},
		{name: "exclude", args: []string{"--exclude", "audit_*"}, want: "accepts --exclude, but a plan file records the exclude patterns"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
				fixture.validateArgs(test.args...)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Contains, "atlas schema plan validate "+test.want)
		})
	}
}

// TestSchemaPlanValidateRejectsPositionalArguments pins the captured usage line
// `atlas schema plan validate [flags]`.
func TestSchemaPlanValidateRejectsPositionalArguments(t *testing.T) {
	c := qt.New(t)
	fixture := newValidateFixture(c, "validate-positional",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "validate",
		fixture.validateArgs("stray")...)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "unexpected positional arguments")
}

// tableNames returns the introspected table names, sorted by the reader's own
// order, so two schemas can be compared without depending on column detail.
func tableNames(schema *dbschematypes.DBSchema) []string {
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, table.Name)
	}
	return names
}
