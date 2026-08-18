package ociartifact

// White-box testing required: provenanceAnnotations and withProvenance read the
// environment and decide what a published manifest records, and both are
// unexported because neither is a registry operation. Reaching them from
// outside means pushing to a registry and reading the manifest back, which
// tests the registry rather than the decision.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func clearProvenanceEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		SourceEnv, RevisionEnv, BuildRunEnv,
		"GITHUB_SERVER_URL", "GITHUB_REPOSITORY", "GITHUB_SHA", "GITHUB_RUN_ID",
	} {
		t.Setenv(name, "")
	}
}

func TestProvenanceAnnotations_ReadsTheExplicitNames(t *testing.T) {
	c := qt.New(t)
	clearProvenanceEnv(t)
	t.Setenv(SourceEnv, "https://git.internal/acme/db")
	t.Setenv(RevisionEnv, "0123456789abcdef")
	t.Setenv(BuildRunEnv, "4711")

	got := provenanceAnnotations()

	c.Assert(got[ocispec.AnnotationSource], qt.Equals, "https://git.internal/acme/db")
	c.Assert(got[ocispec.AnnotationRevision], qt.Equals, "0123456789abcdef")
	c.Assert(got[annotationBuildRun], qt.Equals, "4711")
}

func TestProvenanceAnnotations_FallsBackToTheRunner(t *testing.T) {
	c := qt.New(t)
	clearProvenanceEnv(t)
	t.Setenv("GITHUB_SERVER_URL", "https://github.com/")
	t.Setenv("GITHUB_REPOSITORY", "acme/db")
	t.Setenv("GITHUB_SHA", "fedcba9876543210")
	t.Setenv("GITHUB_RUN_ID", "99")

	got := provenanceAnnotations()

	c.Assert(got[ocispec.AnnotationSource], qt.Equals, "https://github.com/acme/db",
		qt.Commentf("the trailing slash on the server must not double up"))
	c.Assert(got[ocispec.AnnotationRevision], qt.Equals, "fedcba9876543210")
	c.Assert(got[annotationBuildRun], qt.Equals, "99")
}

// TestProvenanceAnnotations_HalfARunnerNamesNothing keeps a partial CI
// environment from producing a URL that is not one.
func TestProvenanceAnnotations_HalfARunnerNamesNothing(t *testing.T) {
	for _, tc := range []struct {
		name       string
		server     string
		repository string
	}{
		{name: "server without repository", server: "https://github.com", repository: ""},
		{name: "repository without server", server: "", repository: "acme/db"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			clearProvenanceEnv(t)
			t.Setenv("GITHUB_SERVER_URL", tc.server)
			t.Setenv("GITHUB_REPOSITORY", tc.repository)

			got := provenanceAnnotations()

			_, present := got[ocispec.AnnotationSource]
			c.Assert(present, qt.IsFalse)
		})
	}
}

func TestProvenanceAnnotations_ExplicitNamesOutrankTheRunner(t *testing.T) {
	c := qt.New(t)
	clearProvenanceEnv(t)
	t.Setenv(SourceEnv, "https://git.internal/acme/db")
	t.Setenv("GITHUB_SERVER_URL", "https://github.com")
	t.Setenv("GITHUB_REPOSITORY", "acme/db")

	got := provenanceAnnotations()

	c.Assert(got[ocispec.AnnotationSource], qt.Equals, "https://git.internal/acme/db")
}

// TestWithProvenance_DeclaredValuesWin is the rule that keeps an inferred value
// from replacing a stated one. The caller knows what it is publishing; the
// environment only knows what ran.
func TestWithProvenance_DeclaredValuesWin(t *testing.T) {
	c := qt.New(t)
	clearProvenanceEnv(t)
	t.Setenv(RevisionEnv, "from-the-environment")

	got := withProvenance(map[string]string{
		ocispec.AnnotationRevision: "from-the-caller",
		"io.stokaro.ptah.kind":     "migration",
	})

	c.Assert(got[ocispec.AnnotationRevision], qt.Equals, "from-the-caller")
	c.Assert(got["io.stokaro.ptah.kind"], qt.Equals, "migration",
		qt.Commentf("the caller's other annotations must survive the merge"))
}

func TestWithProvenance_LeavesTheCallerMapAlone(t *testing.T) {
	c := qt.New(t)
	clearProvenanceEnv(t)
	t.Setenv(SourceEnv, "https://git.internal/acme/db")
	declared := map[string]string{"io.stokaro.ptah.kind": "schema"}

	got := withProvenance(declared)

	c.Assert(got[ocispec.AnnotationSource], qt.Equals, "https://git.internal/acme/db")
	_, leaked := declared[ocispec.AnnotationSource]
	c.Assert(leaked, qt.IsFalse, qt.Commentf("the caller's map must not gain keys it never set"))
}

// TestProvenanceAnnotations_FallsBackToWhatTheLinkerStamped is why this reads
// buildinfo rather than the module version: a release binary knows its own
// commit, so an artifact published outside CI still records where it came from.
func TestProvenanceAnnotations_FallsBackToWhatTheLinkerStamped(t *testing.T) {
	c := qt.New(t)
	clearProvenanceEnv(t)

	got := provenanceAnnotations()

	// An unstamped test binary reports the sentinel, which must not be
	// recorded as if it named a commit.
	revision, present := got[ocispec.AnnotationRevision]
	c.Assert(present, qt.Equals, revision != "")
	c.Assert(revision, qt.Not(qt.Equals), "unknown")
}

func TestStampedValue_DropsTheSentinel(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
		want  string
	}{
		{name: "the sentinel becomes empty", input: "unknown", want: ""},
		{name: "padding does not hide it", input: "  unknown  ", want: ""},
		{name: "a real commit survives", input: "0123456789abcdef", want: "0123456789abcdef"},
		{name: "an empty value stays empty", input: "", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(stampedValue(tc.input), qt.Equals, tc.want)
		})
	}
}
