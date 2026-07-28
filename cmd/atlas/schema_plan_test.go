package atlas_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/safety"
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

// runSchemaPlan executes `schema plan` with args on a fresh command tree and
// returns combined output plus the execution error.
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

	out, err := runSchemaPlan(atlas.NewAtlasCommand(),
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

	out, err := runSchemaPlan(atlas.NewAtlasCommand(),
		"--from", "sqlite://"+dbPath,
		"--to", "file://schema.sql",
		"--save",
	)

	// --save without --output writes <name>.plan.json in the working
	// directory, with the deterministic fingerprint-derived default name.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	matches, globErr := filepath.Glob("plan_*.plan.json")
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	c.Assert(out, qt.Contains, "Plan saved to file://"+matches[0])
	plan, err := atlasschema.ReadPlanFile(matches[0])
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Name+atlasschema.PlanFileSuffix, qt.Equals, matches[0])
}

func TestSchemaPlanCustomNameIsRecordedAndUsedAsFileName(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "plan-named.db")
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE named_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	out, err := runSchemaPlan(atlas.NewAtlasCommand(),
		"--from", "sqlite://"+dbPath,
		"--to", "file://schema.sql",
		"--name", "add_named_users",
		"--save",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Plan saved to file://add_named_users.plan.json")
	plan, err := atlasschema.ReadPlanFile("add_named_users.plan.json")
	c.Assert(err, qt.IsNil)
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

	out, err := runSchemaPlan(atlas.NewAtlasCommand(),
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

	out, err := runSchemaPlan(atlas.NewAtlasCommand(),
		"--from", "sqlite://"+dbPath,
		"--to", "file://schema.sql",
		"--dry-run",
	)

	// --dry-run prints exactly the plan document that --save would write.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	var plan atlasschema.PlanFile
	c.Assert(json.Unmarshal([]byte(out), &plan), qt.IsNil)
	c.Assert(plan.FormatVersion, qt.Equals, atlasschema.PlanFormatVersion)
	c.Assert(plan.Statements, qt.HasLen, 1)
	matches, globErr := filepath.Glob("*.plan.json")
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 0)
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

	out, err := runSchemaPlan(atlas.NewAtlasCommand(),
		"--from", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--output", planPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema is synced, no changes to be made.")
	_, statErr := os.Stat(planPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestSchemaPlanRequiresSaveOutputOrDryRun(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE u (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	out, err := runSchemaPlan(atlas.NewAtlasCommand(),
		"--from", "sqlite://"+filepath.Join(dir, "plan.db"),
		"--to", "file://"+schemaPath,
	)

	// Atlas pushes an unsaved plan to its registry; Ptah has none, so the
	// output selection must be explicit instead of silently defaulting.
	c.Assert(err, qt.ErrorMatches, `atlas schema plan pushes unsaved plans to the Atlas Registry.*pass --save or --output <path>.*or --dry-run.*`)
	c.Assert(out, qt.Contains, "error: atlas schema plan pushes unsaved plans")
}

func TestSchemaPlanRejectsNonDatabaseFrom(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE u (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	_, err := runSchemaPlan(atlas.NewAtlasCommand(),
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

	_, err := runSchemaPlan(atlas.NewAtlasCommand(),
		"--from", "sqlite://a.db",
		"--from", "sqlite://b.db",
		"--to", "file://"+schemaPath,
		"--save",
	)

	c.Assert(err, qt.ErrorMatches, `atlas schema plan accepts multiple --from URLs, but Ptah plans against one target database URL`)
}

func TestSchemaPlanRejectsNameWithPathSeparators(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE u (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	_, err := runSchemaPlan(atlas.NewAtlasCommand(),
		"--from", "sqlite://"+filepath.Join(dir, "plan.db"),
		"--to", "file://"+schemaPath,
		"--name", "nested/plan",
		"--save",
	)

	c.Assert(err, qt.ErrorMatches, `--name must not contain path separators; use --output to choose the plan file location`)
}

func TestSchemaPlanSaveAndDryRunAreMutuallyExclusive(t *testing.T) {
	c := qt.New(t)

	_, err := runSchemaPlan(atlas.NewAtlasCommand(),
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
			want: `atlas schema plan accepts --push, but plan push targets the Atlas Registry \(Atlas Cloud\); Ptah's local plan workflow saves plan files with --save or --output instead`,
		},
		{
			name: "pending",
			args: []string{"--pending"},
			want: `atlas schema plan accepts --pending, but pending plans are an Atlas Registry approval state; a locally saved plan file is approved by operator review`,
		},
		{
			name: "repo",
			args: []string{"--repo", "atlas://plans"},
			want: `atlas schema plan accepts --repo, but schema repositories exist only in the Atlas Registry \(Atlas Cloud\); Ptah plans are local files`,
		},
		{
			name: "auto_approve",
			args: []string{"--auto-approve"},
			want: `atlas schema plan accepts --auto-approve, but plan approval is an Atlas Registry state; a locally saved plan file is approved by operator review, so there is no approval prompt to skip`,
		},
		{
			name: "edit",
			args: []string{"--edit"},
			want: `atlas schema plan accepts --edit, but Ptah does not implement editing the plan before saving yet; review the saved plan file instead`,
		},
		{
			name: "skip_lint",
			args: []string{"--skip-lint"},
			want: `atlas schema plan accepts --skip-lint, but Ptah does not lint declarative plans yet, so there is no lint step to skip`,
		},
		{
			name: "format",
			args: []string{"--format", "{{ json . }}"},
			want: `atlas schema plan accepts --format, but Ptah does not implement --format for schema plan yet`,
		},
		{
			name: "name_format",
			args: []string{"--name-format", "{{ .Hash }}"},
			want: `atlas schema plan accepts --name-format, but Ptah does not implement Go-template plan naming yet; use --name`,
		},
		{
			name: "directive",
			args: []string{"--directive", "atlas:txmode none"},
			want: `atlas schema plan accepts --directive, but Ptah does not implement Atlas plan directives yet`,
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
			args := append([]string{
				"--from", "sqlite://" + filepath.Join(dir, "plan.db"),
				"--to", "file://" + schemaPath,
				"--save",
			}, tt.args...)

			_, err := runSchemaPlan(atlas.NewAtlasCommand(), args...)

			c.Assert(err, qt.ErrorMatches, tt.want)
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

	out, err := runSchemaPlan(atlas.NewAtlasCommand(),
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
