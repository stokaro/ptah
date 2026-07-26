package dbtest_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/dbtest"
)

// writeUsersEntity writes a minimal Go entity annotation file describing a
// "users" table into a fresh temporary directory and returns that directory so
// it can be used as a [dbtest.SchemaOptions] RootDir.
func writeUsersEntity(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	content := `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "user.go"), []byte(content), 0o600), qt.IsNil)
	return dir
}

func TestRunSchemaTest_AppliesDesiredSchema(t *testing.T) {
	c := qt.New(t)
	rootDir := writeUsersEntity(c)

	cases := []dbtest.Case{{
		Name: "users schema applies and accepts rows",
		Steps: []dbtest.Step{
			{Name: "table exists and is empty", Assert: &dbtest.Assertion{Query: "SELECT * FROM users", RowCount: new(0)}},
			{Name: "insert a user", Exec: "INSERT INTO users (id, name) VALUES (1, 'ada')"},
			{Name: "user is retrievable", Assert: &dbtest.Assertion{Query: "SELECT name FROM users WHERE id = 1", Scalar: new("ada")}},
		},
	}}

	report, err := dbtest.RunSchemaTest(context.Background(), dbtest.SchemaOptions{Cases: cases, RootDir: rootDir})
	c.Assert(err, qt.IsNil)
	c.Assert(report, qt.IsNotNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("%s", report.Text()))
	c.Assert(report.Cases, qt.HasLen, 1)
	c.Assert(report.Cases[0].Steps, qt.HasLen, 3)
	// The report header identifies the schema-test surface, not the migration one.
	c.Assert(report.Text(), qt.Contains, "=== SCHEMA TEST ===")
}

func TestRunSchemaTest_RejectsMigrateToStep(t *testing.T) {
	c := qt.New(t)
	rootDir := writeUsersEntity(c)

	cases := []dbtest.Case{{
		Name:  "migrate_to is not allowed",
		Steps: []dbtest.Step{{Name: "attempt migrate", MigrateTo: "latest"}},
	}}

	report, err := dbtest.RunSchemaTest(context.Background(), dbtest.SchemaOptions{Cases: cases, RootDir: rootDir})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsTrue)
	c.Assert(report.Cases, qt.HasLen, 1)
	c.Assert(report.Cases[0].Passed, qt.IsFalse)
	c.Assert(report.Cases[0].Steps, qt.HasLen, 1)
	c.Assert(report.Cases[0].Steps[0].Passed, qt.IsFalse)
	c.Assert(report.Cases[0].Steps[0].Detail, qt.Contains, `migrate_to is not valid in a schema test; use "ptah migrations test"`)
}

func TestRunSchemaTest_EphemeralCasesAreIsolated(t *testing.T) {
	c := qt.New(t)
	rootDir := writeUsersEntity(c)

	cases := []dbtest.Case{
		{
			Name: "first case inserts a row",
			Steps: []dbtest.Step{
				{Name: "insert", Exec: "INSERT INTO users (id, name) VALUES (1, 'ada')"},
				{Name: "one row present", Assert: &dbtest.Assertion{Query: "SELECT * FROM users", RowCount: new(1)}},
			},
		},
		{
			Name: "second case sees a fresh schema",
			Steps: []dbtest.Step{
				// A fresh ephemeral database per case reapplies the desired schema, so
				// the users table exists but is empty. A shared database would leak the
				// first case's row and fail this assertion.
				{Name: "no rows leaked", Assert: &dbtest.Assertion{Query: "SELECT * FROM users", RowCount: new(0)}},
			},
		},
	}

	report, err := dbtest.RunSchemaTest(context.Background(), dbtest.SchemaOptions{Cases: cases, RootDir: rootDir})
	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsFalse, qt.Commentf("%s", report.Text()))
	c.Assert(report.Cases, qt.HasLen, 2)
	c.Assert(report.Cases[0].Passed, qt.IsTrue)
	c.Assert(report.Cases[1].Passed, qt.IsTrue)
}

func TestRunSchemaTest_InvalidCasesError(t *testing.T) {
	c := qt.New(t)
	rootDir := writeUsersEntity(c)

	cases := []dbtest.Case{{
		Name:  "no action",
		Steps: []dbtest.Step{{Name: "empty"}},
	}}

	report, err := dbtest.RunSchemaTest(context.Background(), dbtest.SchemaOptions{Cases: cases, RootDir: rootDir})
	c.Assert(err, qt.IsNotNil)
	c.Assert(report, qt.IsNil)
	c.Assert(err.Error(), qt.Contains, "invalid test cases")
}
