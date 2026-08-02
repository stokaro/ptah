package dbtest_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/dbtest"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestRunMigrationTest_AssertionsHappyPath(t *testing.T) {
	tests := []struct {
		name  string
		cases []dbtest.Case
	}{
		{
			name: "row_count and scalar pass",
			cases: []dbtest.Case{{
				Name: "products",
				Steps: []dbtest.Step{
					{Name: "create", Exec: "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"},
					{Name: "seed", Exec: "INSERT INTO products (name) VALUES ('a'), ('b')"},
					{Name: "count", Assert: &dbtest.Assertion{Query: "SELECT id FROM products", RowCount: new(2)}},
					{Name: "first", Assert: &dbtest.Assertion{Query: "SELECT name FROM products ORDER BY id LIMIT 1", Scalar: new("a")}},
				},
			}},
		},
		{
			name: "error_contains passes on failing query",
			cases: []dbtest.Case{{
				Name: "missing table errors",
				Steps: []dbtest.Step{
					{Name: "query missing", Assert: &dbtest.Assertion{Query: "SELECT * FROM missing_table", ErrorContains: "missing_table"}},
				},
			}},
		},
	}

	c := qt.New(t)
	for _, tc := range tests {
		c.Run(tc.name, func(c *qt.C) {
			report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: tc.cases})
			c.Assert(err, qt.IsNil)
			c.Assert(report, qt.IsNotNil)
			c.Assert(report.Failed(), qt.IsFalse)
		})
	}
}

func TestRunMigrationTest_AssertionsFailurePath(t *testing.T) {
	tests := []struct {
		name  string
		cases []dbtest.Case
	}{
		{
			name: "row_count mismatch fails",
			cases: []dbtest.Case{{
				Name: "empty table",
				Steps: []dbtest.Step{
					{Name: "create", Exec: "CREATE TABLE t (id INTEGER PRIMARY KEY)"},
					{Name: "count", Assert: &dbtest.Assertion{Query: "SELECT id FROM t", RowCount: new(3)}},
				},
			}},
		},
		{
			name: "error_contains fails when query succeeds",
			cases: []dbtest.Case{{
				Name: "query unexpectedly succeeds",
				Steps: []dbtest.Step{
					{Name: "create", Exec: "CREATE TABLE ok (id INTEGER PRIMARY KEY)"},
					{Name: "expect error", Assert: &dbtest.Assertion{Query: "SELECT id FROM ok", ErrorContains: "boom"}},
				},
			}},
		},
		{
			name: "exec failure fails the case",
			cases: []dbtest.Case{{
				Name: "bad sql",
				Steps: []dbtest.Step{
					{Name: "broken", Exec: "THIS IS NOT SQL"},
				},
			}},
		},
	}

	c := qt.New(t)
	for _, tc := range tests {
		c.Run(tc.name, func(c *qt.C) {
			report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: tc.cases})
			c.Assert(err, qt.IsNil)
			c.Assert(report, qt.IsNotNil)
			c.Assert(report.Failed(), qt.IsTrue)
		})
	}
}

func TestRunMigrationTest_FailureDetailAndShortCircuit(t *testing.T) {
	c := qt.New(t)
	cases := []dbtest.Case{{
		Name: "stops after first failure",
		Steps: []dbtest.Step{
			{Name: "create", Exec: "CREATE TABLE t (id INTEGER PRIMARY KEY)"},
			{Name: "bad count", Assert: &dbtest.Assertion{Query: "SELECT id FROM t", RowCount: new(3)}},
			{Name: "never runs", Exec: "INSERT INTO t (id) VALUES (1)"},
		},
	}}

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: cases})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsTrue)
	c.Assert(report.Cases, qt.HasLen, 1)
	c.Assert(report.Cases[0].Passed, qt.IsFalse)
	// The third step must be skipped once the second fails.
	c.Assert(report.Cases[0].Steps, qt.HasLen, 2)
	c.Assert(report.Cases[0].Steps[0].Passed, qt.IsTrue)
	c.Assert(report.Cases[0].Steps[1].Passed, qt.IsFalse)
	c.Assert(report.Cases[0].Steps[1].Detail, qt.Contains, "expected row_count 3, got 0")

	text := report.Text()
	c.Assert(text, qt.Contains, "=== MIGRATION TEST ===")
	c.Assert(text, qt.Contains, "FAIL  case \"stops after first failure\"")
	c.Assert(text, qt.Contains, "1 cases, 0 passed, 1 failed")
}

func TestRunMigrationTest_InvalidCasesError(t *testing.T) {
	c := qt.New(t)
	cases := []dbtest.Case{{
		Name:  "no action",
		Steps: []dbtest.Step{{Name: "empty"}},
	}}
	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: cases})
	c.Assert(err, qt.IsNotNil)
	c.Assert(report, qt.IsNil)
	c.Assert(err.Error(), qt.Contains, "invalid test cases")
}

func TestRunMigrationTest_MigrateToLatest(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_create_widgets.up.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL);"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_create_widgets.down.sql"),
		[]byte("DROP TABLE widgets;"), 0o600), qt.IsNil)

	cases := []dbtest.Case{{
		Name: "widgets migrate and accept rows",
		Steps: []dbtest.Step{
			{Name: "migrate to latest", MigrateTo: "latest"},
			{Name: "widgets starts empty", Assert: &dbtest.Assertion{Query: "SELECT * FROM widgets", RowCount: new(0)}},
			{Name: "insert widget", Exec: "INSERT INTO widgets (name) VALUES ('gear')"},
			{Name: "one widget exists", Assert: &dbtest.Assertion{Query: "SELECT COUNT(*) FROM widgets", Scalar: new("1")}},
		},
	}}

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases:         cases,
		MigrationsDir: migrationsDir,
		DirFormat:     migrator.MigrationDirFormatPtah,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse)
	c.Assert(report.Cases, qt.HasLen, 1)
	c.Assert(report.Cases[0].Passed, qt.IsTrue)
	c.Assert(report.Cases[0].Steps, qt.HasLen, 4)
}

func TestRunMigrationTest_ApplySchema(t *testing.T) {
	c := qt.New(t)
	rootDir := writeUsersEntity(c)
	cases := []dbtest.Case{{
		Name: "desired schema applies",
		Steps: []dbtest.Step{
			{Name: "apply desired schema", ApplySchema: true},
			{Name: "users table is empty", Assert: &dbtest.Assertion{Query: "SELECT id FROM users", RowCount: new(0)}},
			{Name: "insert user", Exec: "INSERT INTO users (id, name) VALUES (1, 'ada')"},
			{Name: "user is retrievable", Assert: &dbtest.Assertion{Query: "SELECT name FROM users", Scalar: new("ada")}},
		},
	}}

	report, err := dbtest.RunMigrationTest(t.Context(), dbtest.Options{Cases: cases, RootDir: rootDir})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("%s", report.Text()))
	c.Assert(report.Cases[0].Steps[0].Detail, qt.Equals, "desired schema applied")
}

func TestRunMigrationTest_ApplySchemaPreservesMigrationObjects(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_create_widgets.up.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_create_widgets.down.sql"),
		[]byte("DROP TABLE widgets;"), 0o600), qt.IsNil)
	rootDir := writeUsersEntity(c)

	report, err := dbtest.RunMigrationTest(t.Context(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name: "desired schema is additive",
			Steps: []dbtest.Step{
				{Name: "migrate", MigrateTo: "latest"},
				{Name: "apply desired schema", ApplySchema: true},
				{Name: "migration table remains", Assert: &dbtest.Assertion{Query: "SELECT id FROM widgets", RowCount: new(0)}},
				{Name: "desired table exists", Assert: &dbtest.Assertion{Query: "SELECT id FROM users", RowCount: new(0)}},
			},
		}},
		MigrationsDir: migrationsDir,
		RootDir:       rootDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("%s", report.Text()))
	c.Assert(report.Cases[0].Steps[1].Detail, qt.Equals, "desired schema applied")
}

func TestRunMigrationTest_ApplySchemaConvergesOverlappingTable(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_create_users.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_create_users.down.sql"),
		[]byte("DROP TABLE users;"), 0o600), qt.IsNil)
	rootDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(rootDir, "user.go"), []byte(`package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT"
	Name string
}
`), 0o600), qt.IsNil)

	report, err := dbtest.RunMigrationTest(t.Context(), dbtest.Options{
		Cases: []dbtest.Case{{
			Name: "desired schema adds the missing column",
			Steps: []dbtest.Step{
				{Name: "migrate", MigrateTo: "latest"},
				{Name: "converge desired schema", ApplySchema: true},
				{
					Name: "name column exists",
					Assert: &dbtest.Assertion{
						Query:  "SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = 'name'",
						Scalar: new("1"),
					},
				},
			},
		}},
		MigrationsDir: migrationsDir,
		RootDir:       rootDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("%s", report.Text()))
}

func TestRunMigrationTest_ApplySchemaRequiresRootDir(t *testing.T) {
	c := qt.New(t)
	cases := []dbtest.Case{{
		Name:  "missing desired schema",
		Steps: []dbtest.Step{{Name: "apply desired schema", ApplySchema: true}},
	}}

	report, err := dbtest.RunMigrationTest(t.Context(), dbtest.Options{Cases: cases})
	c.Assert(err, qt.ErrorMatches, "apply_schema requires a desired schema root directory")
	c.Assert(report, qt.IsNil)
}

func TestRunMigrationTest_EphemeralCasesAreIsolated(t *testing.T) {
	c := qt.New(t)
	cases := []dbtest.Case{
		{
			Name: "creates a table",
			Steps: []dbtest.Step{
				{Name: "create", Exec: "CREATE TABLE leak (id INTEGER PRIMARY KEY)"},
				{Name: "insert", Exec: "INSERT INTO leak (id) VALUES (1)"},
			},
		},
		{
			Name: "does not see the other case's table",
			Steps: []dbtest.Step{
				// A fresh ephemeral database per case means this table is absent, so
				// the query errors and error_contains passes. A shared database would
				// make the query succeed and fail this assertion.
				{Name: "table is absent", Assert: &dbtest.Assertion{Query: "SELECT id FROM leak", ErrorContains: "leak"}},
			},
		},
	}

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: cases})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse)
	c.Assert(report.Cases, qt.HasLen, 2)
	c.Assert(report.Cases[0].Passed, qt.IsTrue)
	c.Assert(report.Cases[1].Passed, qt.IsTrue)
}

func TestRunMigrationTest_SeedStepDirectoryOverride(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_create_widgets.up.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL);"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_create_widgets.down.sql"),
		[]byte("DROP TABLE widgets;"), 0o600), qt.IsNil)

	seedsDir := t.TempDir()
	// .all.sql applies for any env; the .dev.sql file only applies when env=dev.
	c.Assert(os.WriteFile(filepath.Join(seedsDir, "010_widgets.all.sql"),
		[]byte("INSERT INTO widgets (name) VALUES ('gear');"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(seedsDir, "020_widgets.dev.sql"),
		[]byte("INSERT INTO widgets (name) VALUES ('cog');"), 0o600), qt.IsNil)

	cases := []dbtest.Case{{
		Name: "seed applies matching environment files",
		Steps: []dbtest.Step{
			{Name: "migrate", MigrateTo: "latest"},
			{Name: "seed dev", Seed: &dbtest.SeedStep{Dir: seedsDir, Env: "dev"}},
			{Name: "both seed rows present", Assert: &dbtest.Assertion{Query: "SELECT id FROM widgets", RowCount: new(2)}},
		},
	}}

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases:         cases,
		MigrationsDir: migrationsDir,
		SeedDir:       t.TempDir(),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("%s", report.Text()))
	c.Assert(report.Cases[0].Steps, qt.HasLen, 3)
	c.Assert(report.Cases[0].Steps[1].Detail, qt.Contains, "seeded 2 file(s)")
}

func TestRunMigrationTest_DefaultSeedDirectory(t *testing.T) {
	c := qt.New(t)
	seedsDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(seedsDir, "010_widgets.test.sql"),
		[]byte("INSERT INTO widgets (name) VALUES ('gear');"),
		0o600,
	), qt.IsNil)
	cases := []dbtest.Case{{
		Name: "run-level seed directory",
		Steps: []dbtest.Step{
			{Name: "create", Exec: "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"},
			{Name: "seed test", Seed: &dbtest.SeedStep{Env: "test"}},
			{Name: "seed row present", Assert: &dbtest.Assertion{
				Query:  "SELECT name FROM widgets",
				Scalar: new("gear"),
			}},
		},
	}}

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases:   cases,
		SeedDir: seedsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("%s", report.Text()))
	c.Assert(report.Cases[0].Steps[1].Detail, qt.Contains, "seeded 1 file(s)")
}

func TestRunMigrationTest_SeedRequiresEnv(t *testing.T) {
	c := qt.New(t)
	// A seed step without an env must be rejected at validation, not accepted and
	// then failed at run time by the seeder's "environment is required" guard.
	cases := []dbtest.Case{{
		Name:  "seed without env",
		Steps: []dbtest.Step{{Name: "seed", Seed: &dbtest.SeedStep{Dir: t.TempDir()}}},
	}}

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: cases})
	c.Assert(err, qt.IsNotNil)
	c.Assert(report, qt.IsNil)
	c.Assert(err.Error(), qt.Contains, "seed requires an env")
}

func TestRunMigrationTest_SeedRequiresDirectory(t *testing.T) {
	c := qt.New(t)
	cases := []dbtest.Case{{
		Name:  "seed without directory",
		Steps: []dbtest.Step{{Name: "seed", Seed: &dbtest.SeedStep{Env: "test"}}},
	}}

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: cases})
	c.Assert(err, qt.IsNotNil)
	c.Assert(report, qt.IsNil)
	c.Assert(err.Error(), qt.Contains, "seed requires a dir or a run-level seed directory")
}

func TestReport_RenderFormats(t *testing.T) {
	c := qt.New(t)
	cases := []dbtest.Case{{
		Name: "one exec and assert",
		Steps: []dbtest.Step{
			{Name: "create", Exec: "CREATE TABLE t (id INTEGER PRIMARY KEY)"},
			{Name: "empty", Assert: &dbtest.Assertion{Query: "SELECT id FROM t", RowCount: new(0)}},
		},
	}}
	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: cases})
	c.Assert(err, qt.IsNil)

	// JSON carries the kind, summary counts, and cases, and round-trips.
	jsonOut, err := report.JSON()
	c.Assert(err, qt.IsNil)
	var parsed map[string]any
	c.Assert(json.Unmarshal([]byte(jsonOut), &parsed), qt.IsNil)
	c.Assert(parsed["kind"], qt.Equals, "MIGRATION")
	c.Assert(parsed["total"], qt.Equals, float64(1))
	c.Assert(parsed["passed"], qt.Equals, float64(1))
	c.Assert(parsed["failed"], qt.Equals, float64(0))

	// HTML is a self-contained document naming the case.
	htmlOut, err := report.HTML()
	c.Assert(err, qt.IsNil)
	c.Assert(htmlOut, qt.Contains, "<!doctype html>")
	c.Assert(htmlOut, qt.Contains, "one exec and assert")

	// A case name with HTML metacharacters is escaped, never emitted raw, so the
	// report cannot inject markup regardless of the test-case contents.
	xssReport, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: []dbtest.Case{{
		Name:  "<script>alert(1)</script>",
		Steps: []dbtest.Step{{Name: "noop", Exec: "SELECT 1"}},
	}}})
	c.Assert(err, qt.IsNil)
	xssHTML, err := xssReport.HTML()
	c.Assert(err, qt.IsNil)
	c.Assert(xssHTML, qt.Not(qt.Contains), "<script>alert(1)</script>")
	c.Assert(xssHTML, qt.Contains, "&lt;script&gt;")

	// Render dispatches by format name.
	text, err := report.Render("text")
	c.Assert(err, qt.IsNil)
	c.Assert(text, qt.Contains, "=== MIGRATION TEST ===")
	rjson, err := report.Render("json")
	c.Assert(err, qt.IsNil)
	c.Assert(rjson, qt.Equals, jsonOut)
}

func TestReport_RenderFailurePath(t *testing.T) {
	c := qt.New(t)

	rendered, err := (&dbtest.Report{}).Render("xml")

	c.Assert(err, qt.ErrorMatches, `unsupported report format "xml":.*`)
	c.Assert(rendered, qt.Equals, "")
}

func TestRunMigrationTest_MigrateToWithoutDir(t *testing.T) {
	c := qt.New(t)
	cases := []dbtest.Case{{
		Name:  "migrate without dir",
		Steps: []dbtest.Step{{Name: "migrate", MigrateTo: "latest"}},
	}}
	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: cases})
	c.Assert(err, qt.ErrorMatches, "migrate_to requires a migrations directory")
	c.Assert(report, qt.IsNil)
}

func TestRunMigrationTest_CanceledContext(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	report, err := dbtest.RunMigrationTest(ctx, dbtest.Options{Cases: []dbtest.Case{{
		Name:  "never runs",
		Steps: []dbtest.Step{{Name: "query", Exec: "SELECT 1"}},
	}}})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(report, qt.IsNil)
}
