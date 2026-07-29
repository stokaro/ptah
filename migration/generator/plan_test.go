package generator_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/generator"
)

func TestPlanMigration_DoesNotWriteArtifacts(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c)

	matches, err := filepath.Glob(filepath.Join(outputDir, "*.sql"))

	c.Assert(err, qt.IsNil)
	c.Assert(plan, qt.IsNotNil)
	c.Assert(matches, qt.HasLen, 0)
}

func TestMigrationPlanWriteFiles_HappyPath(t *testing.T) {
	c := qt.New(t)
	plan, outputDir := newSQLiteMigrationPlan(c)

	files, err := plan.WriteFiles()

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 1)
	matches, err := filepath.Glob(filepath.Join(outputDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 2)
}

func TestMigrationPlanWriteFiles_FailurePath(t *testing.T) {
	c := qt.New(t)
	plan, _ := newSQLiteMigrationPlan(c)
	files, err := plan.WriteFiles()
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)

	files, err = plan.WriteFiles()

	c.Assert(err, qt.ErrorMatches, `migration plan has already been written`)
	c.Assert(files, qt.IsNil)
}

func newSQLiteMigrationPlan(c *qt.C) (*generator.MigrationPlan, string) {
	c.Helper()
	outputDir := c.TempDir()
	devURL := "sqlite://" + filepath.Join(c.TempDir(), "dev.db")
	conn, err := dbschema.ConnectToDatabase(c.Context(), devURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	plan, err := generator.PlanMigration(c.Context(), generator.GenerateMigrationOptions{
		Generated: &goschema.Database{
			Tables: []goschema.Table{
				{StructName: "User", Name: "users"},
			},
			Fields: []goschema.Field{
				{
					StructName: "User",
					FieldName:  "ID",
					Name:       "id",
					Type:       "INTEGER",
					Primary:    true,
				},
			},
		},
		DBConn:        conn,
		MigrationName: "create_users",
		OutputDir:     outputDir,
	})
	c.Assert(err, qt.IsNil)
	return plan, outputDir
}
