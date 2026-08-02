package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/safety"
)

// seedSQLiteSchema executes DDL against a SQLite database file so plan tests
// start from a known live schema state.
func seedSQLiteSchema(c *qt.C, dbPath, schemaSQL string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll, schemaSQL), qt.IsNil)
}

// runSchemaPlan executes `schema plan` with args on root and returns combined
// output plus the execution error. Callers may reuse root to verify cleanup
// between Execute calls.
func runSchemaPlan(root *cobra.Command, args ...string) (string, error) {
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(append([]string{"schema", "plan"}, args...))
	err := root.Execute()
	return out.String(), err
}

func TestSchemaPlanSavesFingerprintedPlanFile(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "plan.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	planPath := filepath.Join(dir, "add-orders.plan.json")
	seedSQLiteSchema(c, dbPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	c.Assert(os.WriteFile(schemaPath, []byte(
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--output", planPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Planned schema changes:")
	c.Assert(out, qt.Contains, `CREATE TABLE "orders"`)
	c.Assert(out, qt.Contains, "Plan saved to file://"+planPath)
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.FormatVersion, qt.Equals, atlasschema.PlanFormatVersion)
	c.Assert(plan.Dialect, qt.Equals, "sqlite")
	c.Assert(plan.Name, qt.Matches, `plan_[0-9a-f]{12}`)
	c.Assert(plan.FromFingerprint, qt.Matches, `sha256:[0-9a-f]{64}`)
	c.Assert(plan.ToFingerprint, qt.Matches, `sha256:[0-9a-f]{64}`)
	c.Assert(plan.Destructive, qt.IsFalse)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, `CREATE TABLE "orders"`)
	c.Assert(plan.Statements[0].Severity, qt.Equals, safety.Safe)
}

func TestSchemaPlanSaveUsesDerivedDefaultFileName(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "plan-default.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE plan_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://schema.sql",
		"--save",
	)

	// --save without --output writes <name>.plan.hcl in the working
	// directory: the compat tree defaults to the Atlas plan format with the
	// Atlas-style timestamp name.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	matches, globErr := filepath.Glob("*.plan.hcl")
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	c.Assert(matches[0], qt.Matches, `\d{14}\.plan\.hcl`)
	c.Assert(out, qt.Contains, "Plan saved to file://"+matches[0])
	plan, format, err := atlasschema.ReadPlanDocument(matches[0])
	c.Assert(err, qt.IsNil)
	c.Assert(format, qt.Equals, atlasschema.PlanFormatHCL)
	c.Assert(plan.Name+atlasschema.PlanFileSuffixHCL, qt.Equals, matches[0])
	c.Assert(atlasschema.IsNativeFingerprint(plan.FromFingerprint), qt.IsTrue)
	c.Assert(atlasschema.IsNativeFingerprint(plan.ToFingerprint), qt.IsTrue)
}

func TestSchemaPlanCustomNameIsRecordedAndUsedAsFileName(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "plan-named.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE named_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://schema.sql",
		"--name", "add_named_users",
		"--save",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Plan saved to file://add_named_users.plan.hcl")
	plan, format, err := atlasschema.ReadPlanDocument("add_named_users.plan.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(format, qt.Equals, atlasschema.PlanFormatHCL)
	c.Assert(plan.Name, qt.Equals, "add_named_users")
}

func TestSchemaPlanRecordsDestructiveStatements(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "plan-destructive.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	planPath := filepath.Join(dir, "drop.plan.json")
	seedSQLiteSchema(c, dbPath,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE drop_me (id INTEGER);")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--output", planPath,
	)

	// Dropping drop_me is destructive; the plan records the per-statement
	// severity and the plan-level destructive marker.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Destructive, qt.IsTrue)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, `DROP TABLE`)
	c.Assert(plan.Statements[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(plan.Statements[0].Reason, qt.Not(qt.Equals), "")
}

func TestSchemaPlanDryRunPrintsPlanDocumentWithoutSaving(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "plan-dry.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE dry_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://schema.sql",
		"--dry-run",
	)

	// --dry-run prints exactly the plan document that --save would write —
	// the Atlas .plan.hcl shape by default.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan := parsePlanDocumentOutput(c, out)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, `CREATE TABLE "dry_users"`)
	for _, pattern := range []string{"*.plan.json", "*.plan.hcl"} {
		matches, globErr := filepath.Glob(pattern)
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 0)
	}
}

// parsePlanDocumentOutput round-trips printed plan-document output through
// the plan reader, asserting it is a valid Atlas-format plan document.
func parsePlanDocumentOutput(c *qt.C, out string) atlasschema.PlanFile {
	c.Helper()
	path := filepath.Join(c.TB.TempDir(), "stdout.plan.hcl")
	c.Assert(os.WriteFile(path, []byte(out), 0o600), qt.IsNil)
	plan, format, err := atlasschema.ReadPlanDocument(path)
	c.Assert(err, qt.IsNil)
	c.Assert(format, qt.Equals, atlasschema.PlanFormatHCL)
	return plan
}

func TestSchemaPlanSyncedSchemaWritesNothing(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "plan-synced.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	planPath := filepath.Join(dir, "synced.plan.json")
	schemaSQL := `CREATE TABLE synced_plan_users (id INTEGER PRIMARY KEY);`
	seedSQLiteSchema(c, dbPath, schemaSQL)
	c.Assert(os.WriteFile(schemaPath, []byte(schemaSQL), 0o600), qt.IsNil)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--output", planPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema is synced, no changes to be made.")
	_, statErr := os.Stat(planPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestSchemaPlanWithoutSavePrintsPlanDocument(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "plan-stdout.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE stdout_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://schema.sql",
	)

	// Without --save/--output/--dry-run the plan document prints to stdout
	// like Atlas printing the computed plan, and no file is written.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan := parsePlanDocumentOutput(c, out)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, `CREATE TABLE "stdout_users"`)
	for _, pattern := range []string{"*.plan.json", "*.plan.hcl"} {
		matches, globErr := filepath.Glob(pattern)
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 0)
	}
}

func TestSchemaPlanAcceptsAutoApprove(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "plan-approve.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	planPath := filepath.Join(dir, "p.plan.json")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE approve_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	// The exact invocation shape measured against Atlas:
	// plan --from --to --save --output --auto-approve. --auto-approve is
	// accepted (there is no local approval prompt to skip) and the .json
	// output path keeps the native JSON plan format.
	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--save", "--output", planPath,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Plan saved to file://"+planPath)
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.FormatVersion, qt.Equals, atlasschema.PlanFormatVersion)
}

func TestSchemaPlanHCLOutputRejectsExclude(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "plan-exclude.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE exclude_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--exclude", "zzz_unrelated",
		"--save", "--output", filepath.Join(dir, "x.plan.hcl"),
	)

	// The Atlas plan shape has no exclude field, and silently dropping the
	// patterns would break apply-time fingerprint verification.
	c.Assert(err, qt.ErrorMatches, `the Atlas \.plan\.hcl format has no field for exclude patterns.*`)
}

func TestSchemaPlanRejectsNonDatabaseFrom(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE u (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "file://"+schemaPath,
		"--to", "file://"+schemaPath,
		"--save",
	)

	// The plan binds its statements to the fingerprint of the live target
	// database, so a local-file --from has nothing to fingerprint.
	c.Assert(err, qt.ErrorMatches, `atlas schema plan requires --from to be the target database URL.*local desired-state schema files belong in --to`)
}

func TestSchemaPlanRejectsMultipleFromURLs(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE u (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://a.db",
		"--from", "sqlite://b.db",
		"--to", "file://"+schemaPath,
		"--save",
	)

	c.Assert(err, qt.ErrorMatches, `atlas schema plan accepts multiple --from URLs, but Ptah plans against one target database URL`)
}

func TestSchemaPlanRejectsUnusableNames(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE u (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	// --name and --name-format share one validator, so a literal name gets the
	// same protection a templated one does: a plan name becomes a file name on
	// every platform Ptah is built for, Windows included.
	tests := []struct {
		name     string
		planName string
		want     string
	}{
		{
			name:     "path_separator",
			planName: "nested/plan",
			want:     `--name: the plan name "nested/plan" contains a path separator; use --output to choose the plan file location`,
		},
		{
			name:     "windows_colon",
			planName: "plan:1",
			want:     `--name: the plan name "plan:1" contains one of :\*\?"<>\|, which cannot appear in a file name on Windows`,
		},
		{
			name:     "parent_directory",
			planName: "..",
			want:     `--name: the plan name "\.\." is a directory reference, not a name`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			// --save without --output writes into the working directory, so a
			// case that stopped refusing would drop its artifact into the
			// package source tree. Pointing the working directory at a scratch
			// dir contains that, and the no-file assertion is what proves the
			// refusal happened before the write rather than after it.
			scratch := chdirToScratch(c.TB.(*testing.T))

			_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
				"--from", "sqlite://"+filepath.Join(dir, "plan.db"),
				"--to", "file://"+schemaPath,
				"--name", tt.planName,
				"--save",
			)

			c.Assert(err, qt.ErrorMatches, tt.want)
			assertNoPlanFileWritten(c, scratch)
		})
	}
}

func TestSchemaPlanSaveAndDryRunAreMutuallyExclusive(t *testing.T) {
	c := qt.New(t)

	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://plan.db",
		"--to", "file://schema.sql",
		"--save", "--dry-run",
	)

	c.Assert(err, qt.ErrorMatches, `if any flags in the group \[save dry-run\] are set none of the others can be.*`)
}

func TestSchemaPlanRejectsUnimplementedAtlasFlags(t *testing.T) {
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
			name: "push",
			args: []string{"--push"},
			want: `atlas schema plan accepts --push, but plan push requires a hosted registry; Ptah's local plan workflow saves plan files with --save or --output instead`,
		},
		{
			name: "pending",
			args: []string{"--pending"},
			want: `atlas schema plan accepts --pending, but pending plans require a hosted approval state; a locally saved plan file is approved by operator review`,
		},
		{
			name: "repo",
			args: []string{"--repo", "atlas://plans"},
			want: `atlas schema plan accepts --repo, but schema repositories require a hosted registry; Ptah plans are local files`,
		},
		{
			name: "format",
			args: []string{"--format", "{{ json . }}"},
			want: `atlas schema plan accepts --format, but Ptah does not implement --format for schema plan yet`,
		},
		{
			name: "directive",
			args: []string{"--directive", "atlas:txmode none"},
			want: `atlas schema plan accepts --directive, but Ptah does not implement Atlas plan directives yet; the plan file records only the migration SQL`,
		},
		{
			name: "lock_timeout",
			args: []string{"--lock-timeout", "10s"},
			want: `atlas schema plan accepts --lock-timeout, but Ptah does not implement database lock waiting yet`,
		},
		{
			name: "schema",
			args: []string{"--schema", "public"},
			want: `atlas schema plan accepts --schema, but Ptah only supports local schema files for this command yet`,
		},
		{
			name: "include",
			args: []string{"--include", "users"},
			want: `atlas schema plan accepts --include, but Ptah only supports local schema files for this command yet`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			// --save with no --output writes into the working directory, so
			// point it at a scratch directory: a refusal that regresses must
			// leave its artifact somewhere a test can see it, not in the
			// package source tree. (A mutation sweep found exactly that leak.)
			scratch := chdirToScratchC(c)
			args := append([]string{
				"--from", "sqlite://" + filepath.Join(dir, "plan.db"),
				"--to", "file://" + schemaPath,
				"--save",
			}, tt.args...)

			_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"), args...)

			c.Assert(err, qt.ErrorMatches, tt.want)
			// Assert the protected state, not the proxy: the thing a refusal
			// must prevent is a plan file, and an error return alone does not
			// prove one was not written first.
			assertNoPlanFileWritten(c, scratch)
		})
	}
}

func TestSchemaPlanUsesAtlasProjectEnv(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "plan-env.db")
	planPath := filepath.Join(dir, "env.plan.json")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE env_plan_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  src = "schema.sql"
  dev = "sqlite://dev.db"
}
`), 0o600), qt.IsNil)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--env", "local",
		"--output", planPath,
	)

	// env url supplies the plan target (--from) and schema.src the desired
	// state (--to), mirroring schema apply's project-config resolution.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, `CREATE TABLE "env_plan_users"`)
}

func TestNewCompatCommand_SchemaPlanResolves(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "compat-plan.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	planPath := filepath.Join(dir, "compat.plan.json")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE compat_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--output", planPath,
	)

	// The verb resolves and executes under the ptah-compat entry point too.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Plan saved to file://"+planPath)
	_, statErr := os.Stat(planPath)
	c.Assert(statErr, qt.IsNil)
}
