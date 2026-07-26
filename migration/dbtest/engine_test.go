package dbtest_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/dbtest"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestRunMigrationTest_Assertions(t *testing.T) {
	tests := []struct {
		name       string
		cases      []dbtest.Case
		wantFailed bool
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
			wantFailed: false,
		},
		{
			name: "row_count mismatch fails",
			cases: []dbtest.Case{{
				Name: "empty table",
				Steps: []dbtest.Step{
					{Name: "create", Exec: "CREATE TABLE t (id INTEGER PRIMARY KEY)"},
					{Name: "count", Assert: &dbtest.Assertion{Query: "SELECT id FROM t", RowCount: new(3)}},
				},
			}},
			wantFailed: true,
		},
		{
			name: "error_contains passes on failing query",
			cases: []dbtest.Case{{
				Name: "missing table errors",
				Steps: []dbtest.Step{
					{Name: "query missing", Assert: &dbtest.Assertion{Query: "SELECT * FROM missing_table", ErrorContains: "missing_table"}},
				},
			}},
			wantFailed: false,
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
			wantFailed: true,
		},
		{
			name: "exec failure fails the case",
			cases: []dbtest.Case{{
				Name: "bad sql",
				Steps: []dbtest.Step{
					{Name: "broken", Exec: "THIS IS NOT SQL"},
				},
			}},
			wantFailed: true,
		},
	}

	c := qt.New(t)
	for _, tc := range tests {
		c.Run(tc.name, func(c *qt.C) {
			report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: tc.cases})
			c.Assert(err, qt.IsNil)
			c.Assert(report, qt.IsNotNil)
			c.Assert(report.Failed(), qt.Equals, tc.wantFailed)
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

func TestRunMigrationTest_SeedStep(t *testing.T) {
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

	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: cases, MigrationsDir: migrationsDir})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("%s", report.Text()))
	c.Assert(report.Cases[0].Steps, qt.HasLen, 3)
	c.Assert(report.Cases[0].Steps[1].Detail, qt.Contains, "seeded 2 file(s)")
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

func TestRunMigrationTest_MigrateToWithoutDir(t *testing.T) {
	c := qt.New(t)
	cases := []dbtest.Case{{
		Name:  "migrate without dir",
		Steps: []dbtest.Step{{Name: "migrate", MigrateTo: "latest"}},
	}}
	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{Cases: cases})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsTrue)
	c.Assert(report.Cases[0].Steps[0].Detail, qt.Contains, "migrate_to requires a migrations directory")
}
