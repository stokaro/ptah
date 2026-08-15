package atlasschema_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// connectPlanSQLite opens a SQLite database file for plan tests and closes it
// with the test.
func connectPlanSQLite(tb testing.TB, dbPath string) *dbschema.DatabaseConnection {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func writePlanDesiredSchema(tb testing.TB, dir, sql string) string {
	c := qt.New(tb)
	c.Helper()
	path := filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(path, []byte(sql), 0o600), qt.IsNil)
	return "file://" + path
}

func TestPreparePlanFileComputesFingerprintedPlan(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	conn := connectPlanSQLite(c.TB, filepath.Join(dir, "plan.db"))
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll,
		`CREATE TABLE users (id INTEGER PRIMARY KEY);`), qt.IsNil)
	desired := writePlanDesiredSchema(c.TB, dir,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		ToURLs: []string{desired},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.FormatVersion, qt.Equals, atlasschema.PlanFormatVersion)
	c.Assert(plan.Dialect, qt.Equals, "sqlite")
	c.Assert(plan.HasChanges(), qt.IsTrue)
	c.Assert(plan.Name, qt.Matches, `plan_[0-9a-f]{12}`)
	c.Assert(plan.FromFingerprint, qt.Matches, `sha256:[0-9a-f]{64}`)
	c.Assert(plan.ToFingerprint, qt.Matches, `sha256:[0-9a-f]{64}`)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.SQL(), qt.Contains, `CREATE TABLE "orders"`)
}

func TestPreparePlanFileSyncedSchemaHasNoChanges(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	conn := connectPlanSQLite(c.TB, filepath.Join(dir, "synced.db"))
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll,
		`CREATE TABLE users (id INTEGER PRIMARY KEY);`), qt.IsNil)
	desired := writePlanDesiredSchema(c.TB, dir, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		ToURLs: []string{desired},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasChanges(), qt.IsFalse)
}

func TestPreparePlanFileHonorsCustomNameAndDevURLDialect(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	conn := connectPlanSQLite(c.TB, filepath.Join(dir, "named.db"))
	desired := writePlanDesiredSchema(c.TB, dir, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Name:   "my_plan",
		DevURL: "sqlite://dev.db",
		ToURLs: []string{desired},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Name, qt.Equals, "my_plan")

	_, err = atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		DevURL: "postgres://localhost/dev",
		ToURLs: []string{desired},
	})

	// A dev database of another dialect cannot stand in for the plan target.
	c.Assert(err, qt.ErrorMatches, `--dev-url dialect "postgres" does not match --url dialect "sqlite"`)
}

func TestPreparePlanFileRequiresConnection(t *testing.T) {
	c := qt.New(t)

	_, err := atlasschema.PreparePlanFile(context.Background(), nil, atlasschema.PlanFileOptions{
		ToURLs: []string{"file://desired.sql"},
	})

	c.Assert(err, qt.ErrorMatches, `schema plan requires database connection`)
}

func TestVerifyPlanTargetDetectsDriftAndDialectMismatch(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	conn := connectPlanSQLite(c.TB, filepath.Join(dir, "verify.db"))
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll,
		`CREATE TABLE users (id INTEGER PRIMARY KEY);`), qt.IsNil)
	desired := writePlanDesiredSchema(c.TB, dir,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")
	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		ToURLs: []string{desired},
	})
	c.Assert(err, qt.IsNil)

	c.Assert(atlasschema.VerifyPlanTarget(conn, plan), qt.IsNil)

	// A schema change after planning must be detected as a stale plan.
	c.Assert(atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll,
		`CREATE TABLE drifted (id INTEGER PRIMARY KEY);`), qt.IsNil)
	err = atlasschema.VerifyPlanTarget(conn, plan)
	var stale *atlasschema.StalePlanError
	c.Assert(err, qt.ErrorAs, &stale)
	c.Assert(stale.PlanFingerprint, qt.Equals, plan.FromFingerprint)
	c.Assert(stale.DatabaseFingerprint, qt.Not(qt.Equals), plan.FromFingerprint)

	mismatched := plan
	mismatched.Dialect = "mysql"
	c.Assert(atlasschema.VerifyPlanTarget(conn, mismatched), qt.ErrorMatches,
		`plan file targets dialect "mysql", but the --url database dialect is "sqlite"`)
}

func TestPlanFileMarshalReadRoundTrip(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	conn := connectPlanSQLite(c.TB, filepath.Join(dir, "roundtrip.db"))
	desired := writePlanDesiredSchema(c.TB, dir, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		ToURLs: []string{desired},
	})
	c.Assert(err, qt.IsNil)

	document, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	path := filepath.Join(dir, "roundtrip.plan.json")
	c.Assert(os.WriteFile(path, document, 0o600), qt.IsNil)
	loaded, err := atlasschema.ReadPlanFile(path)

	c.Assert(err, qt.IsNil)
	c.Assert(loaded, qt.DeepEquals, plan)
}

func TestReadPlanFileValidatesContract(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name     string
		contents string
		want     string
	}{
		{
			// The native reader is JSON-only, but an Atlas plan document is a
			// likely mistake and must say so instead of leaking a JSON
			// decoder complaint about the letter 'p'.
			name:     "atlas_hcl_plan_document",
			contents: "plan \"x\" {\n  from = \"a\"\n  to = \"b\"\n  migration = \"c;\"\n}\n",
			want:     `plan file .* is in the Atlas \.plan\.hcl format, which the native .ptah schema apply --plan. does not read; apply it with .ptah-compat schema apply --plan file://.*`,
		},
		{
			name:     "not_json_and_not_hcl",
			contents: "%%%",
			want:     `plan file .* is in the Atlas \.plan\.hcl format.*`,
		},
		{
			name:     "unknown_field",
			contents: `{"format_version":1,"dialect":"sqlite","from_fingerprint":"sha256:ab","statements":[{"sql":"SELECT 1"}],"registry":"atlas"}`,
			want:     `parse plan file .*: json: unknown field "registry"`,
		},
		{
			name:     "wrong_version",
			contents: `{"format_version":2,"dialect":"sqlite","from_fingerprint":"sha256:ab","statements":[{"sql":"SELECT 1"}]}`,
			want:     `invalid plan file .*: unsupported plan format_version 2 \(this Ptah build supports 1\)`,
		},
		{
			name:     "missing_dialect",
			contents: `{"format_version":1,"from_fingerprint":"sha256:ab","statements":[{"sql":"SELECT 1"}]}`,
			want:     `invalid plan file .*: plan dialect is required`,
		},
		{
			name:     "missing_fingerprint",
			contents: `{"format_version":1,"dialect":"sqlite","statements":[{"sql":"SELECT 1"}]}`,
			want:     `invalid plan file .*: plan from_fingerprint is required`,
		},
		{
			name:     "no_statements",
			contents: `{"format_version":1,"dialect":"sqlite","from_fingerprint":"sha256:ab","statements":[]}`,
			want:     `invalid plan file .*: plan contains no statements`,
		},
		{
			name:     "empty_statement_sql",
			contents: `{"format_version":1,"dialect":"sqlite","from_fingerprint":"sha256:ab","statements":[{"sql":"  "}]}`,
			want:     `invalid plan file .*: plan statement 1 has empty sql`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(dir, tt.name+".plan.json")
			c.Assert(os.WriteFile(path, []byte(tt.contents), 0o600), qt.IsNil)

			_, err := atlasschema.ReadPlanFile(path)

			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

func TestReadPlanFileMissingFile(t *testing.T) {
	c := qt.New(t)

	_, err := atlasschema.ReadPlanFile(filepath.Join(t.TempDir(), "missing.plan.json"))

	c.Assert(err, qt.ErrorMatches, `read plan file: .*`)
}

func TestSchemaFingerprintIsDeterministicOverContent(t *testing.T) {
	c := qt.New(t)
	schema := &types.DBSchema{
		Tables: []types.DBTable{{Name: "users"}},
	}

	first, err := atlasschema.SchemaFingerprint(schema)
	c.Assert(err, qt.IsNil)
	second, err := atlasschema.SchemaFingerprint(&types.DBSchema{
		Tables: []types.DBTable{{Name: "users"}},
	})
	c.Assert(err, qt.IsNil)
	changed, err := atlasschema.SchemaFingerprint(&types.DBSchema{
		Tables: []types.DBTable{{Name: "orders"}},
	})
	c.Assert(err, qt.IsNil)

	// Equal content yields equal fingerprints; different content differs.
	c.Assert(first, qt.Equals, second)
	c.Assert(first, qt.Matches, `sha256:[0-9a-f]{64}`)
	c.Assert(changed, qt.Not(qt.Equals), first)

	_, err = atlasschema.SchemaFingerprint(nil)
	c.Assert(err, qt.ErrorMatches, `schema fingerprint requires schema`)
}
