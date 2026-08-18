package ociartifact

import (
	"maps"
	"os"
	"runtime/debug"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
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
// explicit and portable.
func provenanceAnnotations() map[string]string {
	annotations := map[string]string{}
	if source := firstNonEmpty(os.Getenv(SourceEnv), githubRepositoryURL()); source != "" {
		annotations[ocispec.AnnotationSource] = source
	}
	if revision := firstNonEmpty(os.Getenv(RevisionEnv), os.Getenv("GITHUB_SHA")); revision != "" {
		annotations[ocispec.AnnotationRevision] = revision
	}
	if run := firstNonEmpty(os.Getenv(BuildRunEnv), os.Getenv("GITHUB_RUN_ID")); run != "" {
		annotations[annotationBuildRun] = run
	}
	if version := producerVersion(); version != "" {
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

// producerVersion reports the Ptah that is publishing, or "" when the binary
// carries no module version. A development build reports "(devel)", which is
// recorded as it stands: it is a true statement about an artifact that should
// not be in a registry anyone depends on.
func producerVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info == nil {
		return ""
	}
	return strings.TrimSpace(info.Main.Version)
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
