//go:build integration

package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// requestedSchemaVerbs are the verbs that take --schema and an HCL desired state.
// Each answers on a document declaring a schema --schema public leaves out.
var requestedSchemaVerbs = []struct {
	name string
	args func(target, dev, document, dir string) []string
}{
	{name: "schema apply", args: func(target, _, document, _ string) []string {
		return []string{"schema", "apply", "-u", target, "--to", document, "--schema", "public", "--dry-run"}
	}},
	{name: "schema diff", args: func(target, dev, document, _ string) []string {
		return []string{"schema", "diff", "--from", target, "--to", document, "--dev-url", dev, "--schema", "public"}
	}},
	{name: "schema inspect", args: func(_, dev, document, _ string) []string {
		return []string{"schema", "inspect", "-u", document, "--dev-url", dev, "--schema", "public"}
	}},
	{name: "migrate diff", args: func(_, dev, document, dir string) []string {
		return []string{"migrate", "diff", "m1", "--dev-url", dev, "--to", document, "--dir", dir, "--schema", "public"}
	}},
}

// TestStrictCompatRefusesASchemaTheSelectionLeavesOutE2E holds strict mode to the
// pinned community binary, verb by verb, on an HCL document that declares a schema
// --schema leaves out. The default surface keeps the selection a narrowing, which
// is the capability strict mode refuses, so it exits 0 and plans nothing for that
// schema.
func TestStrictCompatRefusesASchemaTheSelectionLeavesOutE2E(t *testing.T) {
	const want = `schema "es_app" from paths`
	const wantWhy = `is not requested (all schemas in HCL must be requested)`
	reference := atlasReferenceBinary(t)
	for _, verb := range requestedSchemaVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			target := newEmptyReadScopeDatabase(c, "requested_target")
			dev := newEmptyReadScopeDatabase(c, "requested_dev")
			document := filepath.Join(c.TempDir(), "schema.hcl")
			c.Assert(os.WriteFile(document, []byte("schema \"public\" {\n}\n\nschema \"es_app\" {\n}\n"), 0o600), qt.IsNil)
			to := "file://" + filepath.ToSlash(document)
			dirs := []string{c.TempDir(), c.TempDir(), c.TempDir()}
			for _, dir := range dirs {
				c.Assert(os.WriteFile(filepath.Join(dir, "atlas.sum"), []byte("h1:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\n"), 0o600), qt.IsNil)
			}

			community, err := exec.CommandContext(c.Context(), reference, verb.args(target, dev, to, "file://"+filepath.ToSlash(dirs[0]))...).CombinedOutput() // #nosec G204 G702 -- operator-provided pinned reference binary
			strict := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")},
				verb.args(target, dev, to, "file://"+filepath.ToSlash(dirs[1]))...)
			full := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, verb.args(target, dev, to, "file://"+filepath.ToSlash(dirs[2]))...)

			c.Assert(exitCodeOf(err), qt.Equals, 1, qt.Commentf("%s", community))
			c.Assert(string(community), qt.Contains, want)
			c.Assert(string(community), qt.Contains, wantWhy)
			c.Assert(strict.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", strict.Stdout, strict.Stderr))
			c.Assert(strict.Stderr, qt.Contains, want)
			c.Assert(strict.Stderr, qt.Contains, wantWhy)
			c.Assert(full.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", full.Stdout, full.Stderr))
			c.Assert(full.Stdout+full.Stderr, qt.Not(qt.Contains), wantWhy)
		})
	}
}

// TestStrictCompatTakesASelectionThatNamesEverySchemaE2E is the control. A document
// the selection covers is not refused, in strict mode or on the community binary.
func TestStrictCompatTakesASelectionThatNamesEverySchemaE2E(t *testing.T) {
	c := qt.New(t)
	target := newEmptyReadScopeDatabase(c, "requested_target")
	document := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(document, []byte("schema \"public\" {\n}\n"), 0o600), qt.IsNil)
	args := []string{"schema", "apply", "-u", target, "--to", "file://" + filepath.ToSlash(document), "--schema", "public", "--dry-run"}
	reference := atlasReferenceBinary(t)

	community, err := exec.CommandContext(c.Context(), reference, args...).CombinedOutput() // #nosec G204 G702 -- operator-provided pinned reference binary
	strict := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")}, args...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", community))
	c.Assert(string(community), qt.Contains, "synced")
	c.Assert(strict.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", strict.Stdout, strict.Stderr))
	c.Assert(strict.Stdout, qt.Contains, "synced")
}
