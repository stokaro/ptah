package migrate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemaartifacttest"
)

func ociUsersSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT", Nullable: false},
		},
	}
}

// TestMigrateCommandsAcceptOCISchemaArtifact is the command-level evidence for
// the OCI rows in docs/source-support.json. The helper serves a real schema
// artifact through the distribution API, so this proves more than flag
// registration or an unreachable-registry diagnostic.
func TestMigrateCommandsAcceptOCISchemaArtifact(t *testing.T) {
	c := qt.New(t)
	host := schemaartifacttest.StartSchemaArtifactRegistry(c, "acme/app", "v1", ociUsersSchema())
	reference := "oci://" + host + "/acme/app:v1"
	root := t.TempDir()

	var planOutput bytes.Buffer
	plan := migrate.NewMigrateCommand()
	plan.SetOut(&planOutput)
	plan.SetErr(&planOutput)
	plan.SetArgs([]string{
		"--schema-file", reference,
		"--plain-http",
		"--db-url", "sqlite:///" + filepath.Join(root, "plan.db"),
	})
	c.Assert(plan.Execute(), qt.IsNil, qt.Commentf("output:\n%s", planOutput.String()))
	c.Check(planOutput.String(), qt.Contains, `CREATE TABLE "users"`)

	var generateOutput bytes.Buffer
	migrationsDir := filepath.Join(root, "migrations")
	generate := migrate.NewMigrateGenerateCommand()
	generate.SetOut(&generateOutput)
	generate.SetErr(&generateOutput)
	generate.SetArgs([]string{
		"--schema-file", reference,
		"--plain-http",
		"--db-url", "sqlite:///" + filepath.Join(root, "generate.db"),
		"--migrations-dir", migrationsDir,
		"--name", "init",
	})
	c.Assert(generate.Execute(), qt.IsNil, qt.Commentf("output:\n%s", generateOutput.String()))

	upFiles, err := filepath.Glob(filepath.Join(migrationsDir, "*_init.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(upFiles, qt.HasLen, 1)
	upSQL, err := os.ReadFile(upFiles[0])
	c.Assert(err, qt.IsNil)
	c.Check(string(upSQL), qt.Contains, `CREATE TABLE "users"`)
}
