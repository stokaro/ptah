package project_test

import (
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasregistry"
)

// adoptableProject is a file a real person wrote: a comment, blank lines, and
// an attribute that is not the one being rewritten.
const adoptableProject = `# Acme's project.

env "local" {
  url      =    "sqlite://app.db"

  # The migrations live in the registry.
  migration {
    dir = "atlas://acme-migrations"
  }
}
`

// TestProjectAdopt_RewritesOnlyTheCompatOnlySpelling is #1215's normalization
// half: "compatibility-only references with safe native equivalents can be
// normalized".
//
// The whole-file comparison is the assertion. A normalizer that re-printed the
// parsed file would produce a correct reference and an unreviewable diff, and
// the person whose comments and spacing it discarded has no way to tell the two
// apart from the command's output.
func TestProjectAdopt_RewritesOnlyTheCompatOnlySpelling(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")
	path := projectFile(c, adoptableProject)

	stdout, _, err := runInspect(c, "adopt", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "atlas://acme-migrations -> oci://ghcr.io/acme/acme-migrations:latest")

	rewritten, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	// The odd spacing on `url` is deliberate and is part of the assertion: a
	// normalizer that re-printed the parsed file would tidy it, and the person
	// whose formatting it silently changed has nothing in the output to tell
	// them so.
	c.Assert(string(rewritten), qt.Equals, `# Acme's project.

env "local" {
  url      =    "sqlite://app.db"

  # The migrations live in the registry.
  migration {
    dir = "oci://ghcr.io/acme/acme-migrations:latest"
  }
}
`)
}

// TestProjectAdopt_TheRewrittenProjectIsNativeReady closes the round trip.
//
// Without it the rewrite could produce any well-formed file and still pass the
// comparison above; this requires the result to be a project the analysis then
// calls native-ready, which is the state adoption exists to reach.
func TestProjectAdopt_TheRewrittenProjectIsNativeReady(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")
	path := projectFile(c, adoptableProject)

	_, _, err := runInspect(c, "adopt", "--atlas-config", path, "--env", "local")
	c.Assert(err, qt.IsNil)

	report, checkErr := adoptionReport(c, "--atlas-config", path, "--env", "local")

	c.Assert(checkErr, qt.IsNil)
	c.Assert(report.NativeReady, qt.IsTrue)
	c.Assert(classesOf(report)["migration dir"], qt.Equals, "exact")
}

// TestProjectAdopt_ASecondRunChangesNothing pins that the verb is idempotent,
// which is what lets it run in a pipeline that does not know whether the
// project was adopted already.
func TestProjectAdopt_ASecondRunChangesNothing(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")
	path := projectFile(c, adoptableProject)

	_, _, first := runInspect(c, "adopt", "--atlas-config", path, "--env", "local")
	c.Assert(first, qt.IsNil)
	after, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)

	stdout, _, second := runInspect(c, "adopt", "--atlas-config", path, "--env", "local")

	c.Assert(second, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Nothing to rewrite")
	again, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(again), qt.Equals, string(after))
}

// TestProjectAdopt_RefusesAProjectWithAnUnsupportedConstruct is the refusal
// that matters.
//
// An unsupported construct is a name Ptah read and acts on nothing. Rewriting
// the rest would hand back a file that LOOKS adopted while the behaviour that
// name asked for is still missing -- which is how such a construct disappears
// into a conversion nobody wrote.
func TestProjectAdopt_RefusesAProjectWithAnUnsupportedConstruct(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")
	document := `env "local" {
  project = "acme"
  url     = "sqlite://app.db"
  migration {
    dir = "atlas://acme-migrations"
  }
}
`
	path := projectFile(c, document)

	_, _, err := runInspect(c, "adopt", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "adoption cannot carry it")
	// The file is what proves the refusal happened before any writing.
	untouched, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(untouched), qt.Equals, document)
}

// TestProjectAdopt_RefusesWhenNoNativeSpellingCanBeNamed keeps the normalizer
// from inventing the half the project never wrote.
//
// #1215 asks for normalization "where mapping is unambiguous". The mapping is
// the configured OCI namespace, and with none set there is no repository to
// name -- so the answer is a refusal rather than a guess.
func TestProjectAdopt_RefusesWhenNoNativeSpellingCanBeNamed(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.NamespaceEnvVar, "")
	path := projectFile(c, adoptableProject)

	_, _, err := runInspect(c, "adopt", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no compat-only construct has an unambiguous native spelling")
	untouched, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(untouched), qt.Equals, adoptableProject)
}

// TestProjectAdopt_RewritesEveryEnvironment is why the walk does not stop at
// the selected env.
//
// Adoption is about the FILE, not about one run of one command. Rewriting only
// `--env local` would leave `staging` naming the reference native Ptah is being
// told to stop using, and the next person to select it would find a project
// half converted with nothing saying so.
func TestProjectAdopt_RewritesEveryEnvironment(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")
	path := projectFile(c, `env "local" {
  url = "sqlite://app.db"
  migration {
    dir = "atlas://acme-migrations"
  }
}

env "staging" {
  url = "sqlite://staging.db"
  migration {
    dir = "atlas://acme-migrations"
  }
}
`)

	_, _, err := runInspect(c, "adopt", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNil)
	rewritten, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(strings.Count(string(rewritten), "oci://ghcr.io/acme/acme-migrations:latest"), qt.Equals, 2)
	c.Assert(string(rewritten), qt.Not(qt.Contains), "atlas://")
}

// TestProjectAdopt_LeavesAnExpressionAlone keeps the normalizer off values it
// cannot read.
//
// A variable, a function call or an interpolation has no value until
// evaluation. Comparing one against the analysis, or replacing it with whatever
// it happened to evaluate to on this run, would write a constant where the
// author wrote a choice.
func TestProjectAdopt_LeavesAnExpressionAlone(t *testing.T) {
	c := qt.New(t)
	c.Setenv(atlasregistry.NamespaceEnvVar, "ghcr.io/acme")
	c.Setenv("ACME_MIGRATIONS", "atlas://acme-migrations")
	document := `env "local" {
  url = "sqlite://app.db"
  migration {
    dir = getenv("ACME_MIGRATIONS")
  }
}
`
	path := projectFile(c, document)

	_, _, err := runInspect(c, "adopt", "--atlas-config", path, "--env", "local")

	// The analysis still classifies the RESOLVED value as compat-only, so the
	// verb has something to do and cannot find a literal to do it to. It
	// refuses rather than writing a constant over the expression.
	c.Assert(err, qt.IsNotNil)
	untouched, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(untouched), qt.Equals, document)
}
