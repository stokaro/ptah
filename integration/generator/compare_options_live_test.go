//go:build integration

package generator_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/migration/generator"
)

func TestGenerateMigration_CompareOptionsWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_compare_options")
	defer dropGeneratorTestPostgres(c, admin, targetDatabase)
	target, err := dbschema.ConnectToDatabase(ctx, targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)

	t.Run("default options ignore plpgsql", func(t *testing.T) {
		c := qt.New(t)
		files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
			Generated:     &schemamodel.Database{},
			DBConn:        target,
			MigrationName: "default_ignore",
			OutputDir:     c.TempDir(),
		})
		c.Assert(err, qt.IsNil)
		c.Assert(files, qt.IsNil)
	})

	t.Run("custom options ignore plpgsql", func(t *testing.T) {
		c := qt.New(t)
		files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
			Generated:      &schemamodel.Database{},
			DBConn:         target,
			MigrationName:  "custom_ignore",
			OutputDir:      c.TempDir(),
			CompareOptions: config.WithIgnoredExtensions("plpgsql", "adminpack"),
		})
		c.Assert(err, qt.IsNil)
		c.Assert(files, qt.IsNil)
	})

	t.Run("empty ignore list manages plpgsql", func(t *testing.T) {
		c := qt.New(t)
		files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
			Generated:      &schemamodel.Database{},
			DBConn:         target,
			MigrationName:  "manage_plpgsql",
			OutputDir:      c.TempDir(),
			CompareOptions: config.WithIgnoredExtensions(),
		})
		c.Assert(err, qt.IsNil)
		c.Assert(files, qt.IsNotNil)
		c.Assert(files.Files, qt.HasLen, 1)
		upSQL, err := os.ReadFile(files.Files[0].UpFile)
		c.Assert(err, qt.IsNil)
		c.Assert(string(upSQL), qt.Contains, "DROP EXTENSION")
		c.Assert(string(upSQL), qt.Contains, "plpgsql")
	})

	t.Run("desired extension is added while plpgsql is ignored", func(t *testing.T) {
		c := qt.New(t)
		files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
			Generated: &schemamodel.Database{
				Extensions: []schemamodel.Extension{{Name: "pg_trgm", IfNotExists: true}},
			},
			DBConn:         target,
			MigrationName:  "add_pg_trgm",
			OutputDir:      c.TempDir(),
			CompareOptions: config.WithIgnoredExtensions("plpgsql"),
		})
		c.Assert(err, qt.IsNil)
		c.Assert(files, qt.IsNotNil)
		c.Assert(files.Files, qt.HasLen, 1)
		upSQL, err := os.ReadFile(files.Files[0].UpFile)
		c.Assert(err, qt.IsNil)
		c.Assert(string(upSQL), qt.Contains, "CREATE EXTENSION IF NOT EXISTS")
		c.Assert(string(upSQL), qt.Contains, "pg_trgm")
		c.Assert(string(upSQL), qt.Not(qt.Contains), "DROP EXTENSION")
	})
}
