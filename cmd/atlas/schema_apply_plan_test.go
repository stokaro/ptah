package atlas_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// planFileFixture computes and saves a plan for dbPath -> desired schema SQL
// and returns the plan-file path. The desired schema adds the named table on
// top of the seeded users table.
func planFileFixture(c *qt.C, dir, dbPath, desiredSQL string) string {
	c.Helper()
	schemaPath := filepath.Join(dir, "desired.sql")
	planPath := filepath.Join(dir, "fixture.plan.json")
	c.Assert(os.WriteFile(schemaPath, []byte(desiredSQL), 0o600), qt.IsNil)
	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--output", planPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return planPath
}

// runSchemaApplyPlan executes `schema apply --plan` with the given input and
// extra args, returning combined output and the execution error.
func runSchemaApplyPlan(root *cobra.Command, input, dbPath, planPath string, extra ...string) (string, error) {
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	if input != "" {
		root.SetIn(bytes.NewBufferString(input))
	}
	root.SetArgs(append([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--plan", "file://" + planPath,
	}, extra...))
	err := root.Execute()
	return out.String(), err
}

func TestSchemaApplyPlanFileExecutesPlannedSQL(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-plan.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	planPath := planFileFixture(c, dir, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE planned_orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath, "--auto-approve")

	// The pre-approved plan executes exactly the planned SQL instead of
	// re-planning, and the target ends up with the planned schema.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Planned schema changes:")
	c.Assert(out, qt.Contains, `CREATE TABLE "planned_orders"`)
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "planned_orders"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 1)
}

func TestSchemaApplyPlanFileRefusesStaleTarget(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-stale.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	planPath := planFileFixture(c, dir, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE stale_orders (id INTEGER PRIMARY KEY);\n")
	// The database drifts after the plan was computed.
	seedSQLiteSchema(c, dbPath, `CREATE TABLE drifted (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath, "--auto-approve")

	// Refusing a drifted target is the entire value of a pre-approved plan:
	// nothing is executed and the failure names both fingerprints. The atlas
	// error-code policy in the binaries normalizes the reported failure to
	// exit 1 like every other compat-tree failure.
	c.Assert(err, qt.ErrorMatches, `pre-planned migration is stale: the target database schema does not match the plan's source fingerprint \(plan sha256:[0-9a-f]{64}, database sha256:[0-9a-f]{64}\); the database changed since the plan was computed.*`)
	c.Assert(exitcode.Code(err, 0), qt.Not(qt.Equals), 0)
	c.Assert(out, qt.Contains, "pre-planned migration is stale")
	c.Assert(sqliteTableCount(c, dbPath, "stale_orders"), qt.Equals, 0)
}

func TestSchemaApplyPlanFileDryRunDoesNotApply(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-plan-dry.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	planPath := planFileFixture(c, dir, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE dry_orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `CREATE TABLE "dry_orders"`)
	c.Assert(out, qt.Not(qt.Contains), "Apply these schema changes?")
	c.Assert(sqliteTableCount(c, dbPath, "dry_orders"), qt.Equals, 0)
}

func TestSchemaApplyPlanFileDeclinedConfirmationDoesNotApply(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-plan-declined.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	planPath := planFileFixture(c, dir, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE declined_orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "NO\n", dbPath, planPath)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Apply these schema changes? Type 'YES' to confirm:")
	c.Assert(out, qt.Contains, "Schema apply canceled.")
	c.Assert(sqliteTableCount(c, dbPath, "declined_orders"), qt.Equals, 0)
}

func TestSchemaApplyPlanFileConfirmedApplies(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-plan-confirmed.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	planPath := planFileFixture(c, dir, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE confirmed_orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "YES\n", dbPath, planPath)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "confirmed_orders"), qt.Equals, 1)
}

func TestSchemaApplyPlanFileSupportsFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-plan-format.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	planPath := planFileFixture(c, dir, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE format_orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath,
		"--auto-approve", "--format", `{{ len .Changes }}`)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(strings.TrimSpace(out), qt.Equals, "1")
	c.Assert(sqliteTableCount(c, dbPath, "format_orders"), qt.Equals, 1)
}

func TestSchemaApplyPlanFileRejectsCombinedPlanningFlags(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE u (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "file",
			args: []string{"-f", schemaPath},
			want: `atlas schema apply --plan cannot be combined with --file: the plan file already fixes the desired state; name the verification desired state with --to`,
		},
		{
			name: "dev_url_without_to",
			args: []string{"--dev-url", "sqlite://dev.db"},
			want: `atlas schema apply --plan with --dev-url requires --to: the rehearsal verifies the plan against the desired schema state`,
		},
		{
			name: "exclude",
			args: []string{"--exclude", "users"},
			want: `atlas schema apply --plan cannot be combined with --exclude: the plan file records the exclude patterns it was computed with`,
		},
		{
			name: "edit",
			args: []string{"--edit"},
			want: `atlas schema apply --plan cannot be combined with --edit: a pre-approved plan must execute exactly as reviewed; recompute the plan with .schema plan. instead`,
		},
		{
			name: "schema",
			args: []string{"--schema", "public"},
			want: `atlas schema apply --plan cannot be combined with --schema: the plan file already fixes the planned schema objects`,
		},
		{
			name: "include",
			args: []string{"--include", "users"},
			want: `atlas schema apply --plan cannot be combined with --include: the plan file already fixes the planned schema objects`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append([]string{
				"schema", "apply",
				"--url", "sqlite://" + filepath.Join(dir, "combined.db"),
				"--plan", "file://" + filepath.Join(dir, "missing.plan.json"),
			}, tt.args...))

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

func TestSchemaApplyPlanFileRejectsWrongDialect(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-plan-dialect.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	planPath := planFileFixture(c, dir, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE dialect_orders (id INTEGER PRIMARY KEY);\n")
	rewritePlanDocument(c, planPath, func(document map[string]any) {
		document["dialect"] = "postgres"
	})

	_, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath, "--auto-approve")

	c.Assert(err, qt.ErrorMatches, `plan file targets dialect "postgres", but the --url database dialect is "sqlite"`)
	c.Assert(sqliteTableCount(c, dbPath, "dialect_orders"), qt.Equals, 0)
}

func TestSchemaApplyPlanFileRejectsMalformedDocuments(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-plan-malformed.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)

	tests := []struct {
		name    string
		rewrite func(map[string]any)
		want    string
	}{
		{
			name:    "unknown_field",
			rewrite: func(document map[string]any) { document["surprise"] = true },
			want:    `parse plan file .*: json: unknown field "surprise"`,
		},
		{
			name:    "unsupported_version",
			rewrite: func(document map[string]any) { document["format_version"] = 99 },
			want:    `invalid plan file .*: unsupported plan format_version 99 \(this Ptah build supports 1\)`,
		},
		{
			name:    "no_statements",
			rewrite: func(document map[string]any) { document["statements"] = []any{} },
			want:    `invalid plan file .*: plan contains no statements`,
		},
		{
			name:    "missing_fingerprint",
			rewrite: func(document map[string]any) { document["from_fingerprint"] = "" },
			want:    `invalid plan file .*: plan from_fingerprint is required`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			planPath := planFileFixture(c, t.TempDir(), dbPath,
				"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE malformed_orders (id INTEGER PRIMARY KEY);\n")
			rewritePlanDocument(c, planPath, tt.rewrite)

			_, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath, "--auto-approve")

			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

func TestNewCompatCommand_SchemaApplyPlanFileResolves(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "compat-apply-plan.db")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	planPath := planFileFixture(c, dir, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE compat_orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchemaApplyPlan(atlas.NewCompatCommand("atlas"), "", dbPath, planPath, "--auto-approve")

	// The plan-execution path resolves and runs under ptah-compat too.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c, dbPath, "compat_orders"), qt.Equals, 1)
}

// rewritePlanDocument mutates a saved plan JSON document in place so tests can
// produce dialect mismatches and malformed contracts from a real plan.
func rewritePlanDocument(c *qt.C, path string, mutate func(map[string]any)) {
	c.Helper()
	contents, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	var document map[string]any
	c.Assert(json.Unmarshal(contents, &document), qt.IsNil)
	mutate(document)
	rewritten, err := json.Marshal(document)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(path, rewritten, 0o600), qt.IsNil)
}
