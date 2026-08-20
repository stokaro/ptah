package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlasregistry"
	"go.5x5.cz/ptah/internal/schemaartifacttest"
)

// remoteSchemaProject writes an atlas.hcl whose desired state is a registry
// artifact, and returns the project directory.
func remoteSchemaProject(c *qt.C, namespace string) string {
	c.Helper()
	dir := c.TB.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`data "remote_schema" "app" {
  name = "app"
  tag  = "prod"
}

env "local" {
  url = "`+sqliteURLFromPath(filepath.Join(dir, "app.db"))+`"
  src = data.remote_schema.app.url
}
`), 0o600), qt.IsNil)
	c.Setenv(atlasregistry.NamespaceEnvVar, namespace)
	return dir
}

func remoteSchemaArtifact() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
	}
}

// TestSchemaApply_AppliesADesiredStateFromARegistry is the workflow the matrix
// row is about: a desired state distributed through an ordinary OCI registry is
// applied to a database, with no hosted service in the path
// (stokaro/ptah#1210).
func TestSchemaApply_AppliesADesiredStateFromARegistry(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.PlainHTTP.Name(), "1")
	host := schemaartifacttest.StartSchemaArtifactRegistry(c, "acme/app", "prod", remoteSchemaArtifact())
	dir := remoteSchemaProject(c, host+"/acme")

	out, err := runAtlasArgs(
		"schema", "apply",
		"--config", "file://"+filepath.Join(dir, "atlas.hcl"),
		"--env", "local",
		"--to", "env://src",
		"--dev-url", "sqlite://dev?mode=memory",
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	// Read the result back rather than trusting the exit code: an apply that
	// planned nothing also succeeds.
	inspected, inspectErr := runAtlasArgs(
		"schema", "inspect",
		"--url", sqliteURLFromPath(filepath.Join(dir, "app.db")),
	)
	c.Assert(inspectErr, qt.IsNil, qt.Commentf("%s", inspected))
	c.Assert(inspected, qt.Contains, `table "users"`)
	c.Assert(inspected, qt.Contains, `column "email"`)
}

// TestSchemaApply_RegistryDesiredStateStillRefusesTheOCISpellingOnAFlag is the
// compatibility boundary, asserted through the command rather than the
// classifier.
//
// The capability above is reachable through a project file. The same artifact
// named directly on `--to` must go on being refused, because the pinned
// community binary answers `oci://` with `unknown driver "oci"` at exit 1 and
// AGENTS.md rule (a) forbids ptah-compat exiting 0 where it exits 1.
func TestSchemaApply_RegistryDesiredStateStillRefusesTheOCISpellingOnAFlag(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.PlainHTTP.Name(), "1")
	host := schemaartifacttest.StartSchemaArtifactRegistry(c, "acme/app", "prod", remoteSchemaArtifact())
	target := sqliteURLFromPath(filepath.Join(c.TB.TempDir(), "app.db"))

	out, err := runAtlasArgs(
		"schema", "apply",
		"--url", target,
		"--to", "oci://"+host+"/acme/app:prod",
		"--dev-url", "sqlite://dev?mode=memory",
		"--auto-approve",
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, `unsupported desired-state URL scheme "oci"`)
}

// TestSchemaInspect_ReadsADesiredStateFromARegistry covers the inspection arms,
// which are a different pair of switches from the apply path.
//
// A resolver that returns the right schema into a switch with no arm for its
// kind reports "unresolved inspection source", and one that resolves but does
// not render reports an empty schema. Both arms had to learn the kind, so both
// are asserted here.
func TestSchemaInspect_ReadsADesiredStateFromARegistry(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.PlainHTTP.Name(), "1")
	host := schemaartifacttest.StartSchemaArtifactRegistry(c, "acme/app", "prod", remoteSchemaArtifact())
	dir := remoteSchemaProject(c, host+"/acme")

	out, err := runAtlasArgs(
		"schema", "inspect",
		"--config", "file://"+filepath.Join(dir, "atlas.hcl"),
		"--env", "local",
		"--url", "env://src",
		"--dev-url", "sqlite://dev?mode=memory",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `table "users"`)
	c.Assert(out, qt.Contains, `column "email"`)
}

// TestSchemaInspect_RendersARegistryDesiredStateAsSQL covers the second
// inspection arm, which renders through the dev database rather than printing
// the IR.
func TestSchemaInspect_RendersARegistryDesiredStateAsSQL(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.PlainHTTP.Name(), "1")
	host := schemaartifacttest.StartSchemaArtifactRegistry(c, "acme/app", "prod", remoteSchemaArtifact())
	dir := remoteSchemaProject(c, host+"/acme")

	out, err := runAtlasArgs(
		"schema", "inspect",
		"--config", "file://"+filepath.Join(dir, "atlas.hcl"),
		"--env", "local",
		"--url", "env://src",
		"--dev-url", "sqlite://dev?mode=memory",
		"--format", "{{ sql . }}",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "CREATE TABLE")
	c.Assert(out, qt.Contains, "users")
}
