package schematests_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas/internal/atlastest"
	"go.5x5.cz/ptah/core/schemamodel"
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

func remoteSchemaArtifact() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
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

	out, err := atlastest.RunCompatOutput(
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
	inspected, inspectErr := atlastest.RunCompatOutput(
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

	out, err := atlastest.RunCompatOutput(
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

	out, err := atlastest.RunCompatOutput(
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

	out, err := atlastest.RunCompatOutput(
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

// TestSchemaApply_AppliesADesiredStateNamedByTheVendorSpelling is the direct
// flag half of the same capability: the artifact the test above reaches through
// a project file, named on `--to` the way an Atlas project names it.
//
// It applies rather than classifies. A classifier that returns the right kind
// into a resolver with no arm for it, or one that pulls the wrong reference,
// both look correct until a database is read back -- so the assertion is the
// table and the column, out of the target database, after the run
// (stokaro/ptah#1210).
func TestSchemaApply_AppliesADesiredStateNamedByTheVendorSpelling(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.PlainHTTP.Name(), "1")
	host := schemaartifacttest.StartSchemaArtifactRegistry(c, "acme/app", "prod", remoteSchemaArtifact())
	c.Setenv(atlasregistry.NamespaceEnvVar, host+"/acme")
	target := filepath.Join(c.TB.TempDir(), "app.db")

	out, err := atlastest.RunCompatOutput(
		"schema", "apply",
		"--url", sqliteURLFromPath(target),
		"--to", "atlas://app?tag=prod",
		"--dev-url", "sqlite://dev?mode=memory",
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	inspected, inspectErr := atlastest.RunCompatOutput("schema", "inspect", "--url", sqliteURLFromPath(target))
	c.Assert(inspectErr, qt.IsNil, qt.Commentf("%s", inspected))
	c.Assert(inspected, qt.Contains, `table "users"`)
	c.Assert(inspected, qt.Contains, `column "email"`)
}

// TestSchemaDiff_ReachesTheSameArtifactByBothSpellings is the equivalence this
// issue asks for: one artifact, two ways of naming it, one answer.
//
// The vendor spelling carries no registry host, so it can only agree with an
// explicit reference if the namespace it resolves against is the one that
// reference names. The project file states the OCI reference in full; the flag
// states the logical name; the diff each produces against the same empty
// database has to be the same statements.
func TestSchemaDiff_ReachesTheSameArtifactByBothSpellings(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.PlainHTTP.Name(), "1")
	host := schemaartifacttest.StartSchemaArtifactRegistry(c, "acme/app", "prod", remoteSchemaArtifact())
	dir := remoteSchemaProject(c, host+"/acme")
	empty := sqliteURLFromPath(filepath.Join(c.TB.TempDir(), "empty.db"))

	viaProject, projectErr := atlastest.RunCompatOutput(
		"schema", "diff",
		"--config", "file://"+filepath.Join(dir, "atlas.hcl"),
		"--env", "local",
		"--from", empty,
		"--to", "env://src",
		"--dev-url", "sqlite://dev?mode=memory",
	)
	viaFlag, flagErr := atlastest.RunCompatOutput(
		"schema", "diff",
		"--from", empty,
		"--to", "atlas://app?tag=prod",
		"--dev-url", "sqlite://dev?mode=memory",
	)

	c.Assert(projectErr, qt.IsNil, qt.Commentf("%s", viaProject))
	c.Assert(flagErr, qt.IsNil, qt.Commentf("%s", viaFlag))
	c.Assert(viaFlag, qt.Contains, `CREATE TABLE "users"`)
	c.Assert(viaFlag, qt.Equals, viaProject)
}
