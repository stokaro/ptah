package ociartifact

import (
	"maps"
	"os"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"go.5x5.cz/ptah/internal/buildinfo"
)

const (
	// SourceEnv names the repository an artifact was built from.
	SourceEnv = "PTAH_OCI_SOURCE"
	// RevisionEnv names the commit an artifact was built from.
	RevisionEnv = "PTAH_OCI_REVISION"
	// BuildRunEnv names the build that produced an artifact.
	BuildRunEnv = "PTAH_OCI_BUILD_RUN"

	// annotationBuildRun records the build a published artifact came out of.
	// The OCI predefined set has no key for it, so this one is Ptah's.
	annotationBuildRun = "io.stokaro.ptah.build-run"
	// annotationProducerVersion records which Ptah published the artifact.
	annotationProducerVersion = "io.stokaro.ptah.version"
)

// provenanceAnnotations describes where a published artifact came from.
//
// A digest says two artifacts are the same bytes. It does not say which commit
// they were built from, which pipeline ran, or which Ptah wrote them, and those
// are the questions asked when a deployment is being explained rather than
// performed. The values are read from the environment because that is where a
// pipeline already keeps them; a flag would have to be threaded onto every verb
// that publishes and would then be forgotten on one of them.
//
// The GitHub fallbacks exist because that is the one CI system whose variables
// can be relied on without configuration. Anywhere else, the PTAH_ names are
// explicit and portable. Behind both sits what the linker stamped into this
// binary, so a release build records its own commit even when nothing in the
// environment names one.
func provenanceAnnotations() map[string]string {
	annotations := map[string]string{}
	if source := firstNonEmpty(os.Getenv(SourceEnv), githubRepositoryURL()); source != "" {
		annotations[ocispec.AnnotationSource] = source
	}
	stamped := buildinfo.Resolve()
	if revision := firstNonEmpty(
		os.Getenv(RevisionEnv), os.Getenv("GITHUB_SHA"), stampedValue(stamped.Commit),
	); revision != "" {
		annotations[ocispec.AnnotationRevision] = revision
	}
	if run := firstNonEmpty(os.Getenv(BuildRunEnv), os.Getenv("GITHUB_RUN_ID")); run != "" {
		annotations[annotationBuildRun] = run
	}
	if version := strings.TrimSpace(stamped.Version); version != "" {
		annotations[annotationProducerVersion] = version
	}
	return annotations
}

// githubRepositoryURL rebuilds the repository URL from the two variables the
// runner sets. Both are required: a server with no repository names a service
// rather than a source, and a repository with no server is not a URL.
func githubRepositoryURL() string {
	server := strings.TrimSpace(os.Getenv("GITHUB_SERVER_URL"))
	repository := strings.TrimSpace(os.Getenv("GITHUB_REPOSITORY"))
	if server == "" || repository == "" {
		return ""
	}
	return strings.TrimSuffix(server, "/") + "/" + repository
}

// stampedValue drops buildinfo's "unknown" sentinel.
//
// The sentinel is the right thing to print in `ptah version`, where the reader
// asked what this binary is and "unknown" answers them. It is the wrong thing
// to record on an artifact, where a reader finding a revision assumes it names
// a commit and would go looking for one.
func stampedValue(value string) string {
	if strings.TrimSpace(value) == "unknown" {
		return ""
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// withProvenance adds the run's provenance to an artifact's annotations.
//
// A value the caller set always wins. The caller knows what it is publishing
// and the environment only knows what ran, so an inferred value overwriting a
// declared one would replace a fact with a guess.
func withProvenance(declared map[string]string) map[string]string {
	provenance := provenanceAnnotations()
	if len(provenance) == 0 {
		return declared
	}
	merged := make(map[string]string, len(provenance)+len(declared))
	maps.Copy(merged, provenance)
	maps.Copy(merged, declared)
	return merged
}
