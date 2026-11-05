//go:build integration

package providercli_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// readExportProvider is the hcl-claims fixture with a read that gives every
// table a retention of 30 seconds. Read with --schemas, its knowledge of
// retention is complete in the main schema of the database it read and
// uninspected outside it. Its encoder refuses a request that does not carry
// the knowledge the host owes such a read: complete, or, with
// FIXTURE_READ_HIDDEN=1, which adds a nested namespace the read did not read,
// both regions as read, with the tables bound in their database. environment
// adds to the fixture's settings. The database holds one table, records.
func readExportProvider(c *qt.C, environment ...string) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "hcl-claims", testutils.FixtureRelease{})
	spec.Environment = append([]string{"FIXTURE_READ_RETENTION=1"}, environment...)
	f := newProviderCLI(c, spec)
	host, err := providerhost.Start(c.Context(), spec)
	c.Assert(err, qt.IsNil)
	c.Cleanup(host.Close)
	ctx, err := dbschema.WithNativeConnections(c.Context(), host.Connections())
	c.Assert(err, qt.IsNil)
	connection, err := dbschema.ConnectToDatabase(ctx, f.address)
	c.Assert(err, qt.IsNil)
	c.Assert(connection.Writer().ExecuteSQL(ctx, `CREATE TABLE "records" ("id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
	c.Assert(connection.Close(), qt.IsNil)
	host.Close()
	return f
}

// TestProviderIntrospect_ReadNarrowedBySchemas_HappyPath writes the Go source
// of a provider read that --schemas narrowed: the read knows retention in every
// namespace it listed, so the host sends it as complete, as an embedded
// engine's writer writes such a read, and binds the table in the database its
// retention was read in. The provider writes the retention as its directive,
// and reading the source back gives the retention again.
func TestProviderIntrospect_ReadNarrowedBySchemas_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := readExportProvider(c)
	out := c.TempDir()

	_, stderr, code := executeProviderCLI(c.Context(), []string{"introspect", "--provider-config", f.manifestPath, "--provider-engine", "fixture",
		"--db-url", f.address, "--schemas", "main", "--out", out, "--single-file"})

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	written, err := os.ReadFile(filepath.Join(out, "schema.go"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Contains, "//ptah:fixture:retention seconds=\"30\"\n")
	c.Assert(string(written), qt.Contains, "\tId int64\n")
	runtime := hclClaimsProvider(c)
	db, err := goschema.ParseSource(runtime.Annotations(), "schema.go", string(written))
	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, db), qt.DeepEquals, []string{`table records {"seconds":30}`})
}

// TestProviderInspectHCL_ReadNarrowedBySchemas_HappyPath writes the HCL of a
// provider read that --schemas narrowed, the retention as the provider's block
// in the table, and reads it back. The read knows retention in every namespace
// it listed, so the document's captured feature coverage header claims it
// completely, with no namespace region: the document is complete for the
// selection a later plan applies it to, as the other formats write such a
// read.
func TestProviderInspectHCL_ReadNarrowedBySchemas_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := readExportProvider(c)

	stdout, stderr, code := executeProviderCLI(c.Context(), f.arguments("inspect", "--schemas", "main", "--format", "hcl"))

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Contains, "  retention {\n    seconds = \"30\"\n  }\n")
	runtime := hclClaimsProvider(c)
	db, err := atlashcl.ParseWithOptions(c.Context(), []byte(stdout), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, *db), qt.DeepEquals, []string{`table records {"seconds":30}`})
	kinds := db.FeatureCoverage.KindRecords()
	c.Assert(kinds, qt.HasLen, 1, qt.Commentf("%s", stdout))
	c.Assert(kinds[0].Knowledge.HasNamespaceScopes(), qt.IsFalse, qt.Commentf("%s", stdout))
	c.Assert(kinds[0].Knowledge.Unscoped(), qt.DeepEquals, schemaext.Knowledge{State: schemaext.Complete})
}

// TestProviderIntrospect_ReadOfEverySchema_HappyPath is the control for the
// narrowed read: a read of every schema knows retention everywhere, has no
// region to send, and the provider's check stays out of the way.
func TestProviderIntrospect_ReadOfEverySchema_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := readExportProvider(c)
	out := c.TempDir()

	_, stderr, code := executeProviderCLI(c.Context(), []string{"introspect", "--provider-config", f.manifestPath, "--provider-engine", "fixture",
		"--db-url", f.address, "--out", out, "--single-file"})

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	written, err := os.ReadFile(filepath.Join(out, "schema.go"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Contains, "//ptah:fixture:retention seconds=\"30\"\n")
}

// TestProviderIntrospect_ReadWithAnUnreadNamespace_HappyPath writes the Go
// source of a narrowed read that also reports a nested namespace it did not
// read: the host sends both regions as the read reported them, which the
// provider checks, and the provider writes the retention it read.
func TestProviderIntrospect_ReadWithAnUnreadNamespace_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := readExportProvider(c, "FIXTURE_READ_HIDDEN=1")
	out := c.TempDir()

	_, stderr, code := executeProviderCLI(c.Context(), []string{"introspect", "--provider-config", f.manifestPath, "--provider-engine", "fixture",
		"--db-url", f.address, "--schemas", "main", "--out", out, "--single-file"})

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	written, err := os.ReadFile(filepath.Join(out, "schema.go"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Contains, "//ptah:fixture:retention seconds=\"30\"\n")
}

// TestProviderIntrospect_ReadWithAnUnreadNamespace_FailurePath has a provider
// that writes only whole-model ptah:not-described entries receive the regions
// of a read that did not read a nested namespace. It cannot write that
// knowledge, so it refuses, and the export fails and writes nothing rather
// than a source whose silence claims more than the read knew.
func TestProviderIntrospect_ReadWithAnUnreadNamespace_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := readExportProvider(c, "FIXTURE_READ_HIDDEN=1", "FIXTURE_SOURCE_LIMITS=1")
	out := filepath.Join(c.TempDir(), "models")

	stdout, stderr, code := executeProviderCLI(c.Context(), []string{"introspect", "--provider-config", f.manifestPath, "--provider-engine", "fixture",
		"--db-url", f.address, "--schemas", "main", "--out", out, "--single-file"})

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(stderr, qt.Matches, `(?s).*provider encode source failed \(Unknown\).*`)
	c.Assert(stdout, qt.Equals, "")
	_, err := os.Stat(filepath.Join(out, "schema.go"))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}
