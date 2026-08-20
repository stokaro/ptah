// Package atlasregistry maps Atlas's logical `atlas://` references onto
// ordinary OCI references.
//
// `atlas://app?tag=prod` names a repository and a movable pointer, with no
// registry host in it, because the vendor spelling assumes one hosted account.
// Ptah has no hosted account and no private protocol to speak; what it has is
// an OCI artifact model with tags, write-once tags and digests. So the
// reference is resolved against a namespace the operator configures, and a run
// with no namespace configured is refused rather than sent anywhere
// (stokaro/ptah#1210).
package atlasregistry

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"go.5x5.cz/ptah/internal/envbool"
)

// Scheme is the vendor spelling this package resolves.
const Scheme = "atlas://"

// NamespaceEnvVar names the OCI namespace an `atlas://` reference resolves
// against, for example `ghcr.io/acme`.
//
// It is an environment variable rather than a flag because ptah-compat
// registers exactly the flags of the surface it stands in for, and this is
// Ptah configuration rather than an Atlas one.
const NamespaceEnvVar = "PTAH_ATLAS_REGISTRY"

// PlainHTTP permits an unencrypted connection to the namespace, for a registry
// an operator has explicitly decided to trust -- a local one, in practice.
//
// It is a separate variable from the --plain-http flag every push and pull verb
// carries because this fetch happens while a project file is being evaluated,
// where there is no flag to read. The default is off: a reference resolved from
// a project file must not quietly downgrade its transport.
var PlainHTTP = envbool.New("PTAH_ATLAS_REGISTRY_PLAIN_HTTP", false, envbool.Gated)

// DefaultTag is what a reference naming neither a tag nor a version resolves
// to, matching what `oci://repo` means everywhere else in Ptah.
const DefaultTag = "latest"

// OCIScheme is the scheme every Ptah artifact API expects on a reference.
const OCIScheme = "oci://"

// Reference is one resolved reference.
type Reference struct {
	// OCI is the ordinary reference the artifact is read through, carrying the
	// oci:// scheme every Ptah artifact API expects.
	OCI string
	// Repository is the logical name the atlas:// reference carried.
	Repository string
	// Tag is the resolved tag.
	Tag string
	// Immutable reports whether the tag came from `version=`, which names a
	// write-once tag rather than a movable one.
	Immutable bool
}

// IsReference reports whether raw uses the vendor scheme.
func IsReference(raw string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(raw)), Scheme)
}

// Resolve maps one `atlas://` reference onto an OCI reference.
//
// The three documented forms are supported and no others: a bare repository,
// `?tag=`, and `?version=`. An undocumented query parameter is refused rather
// than ignored, because ignoring one would resolve to a different artifact than
// the reference asked for and say nothing.
func Resolve(raw string) (Reference, error) {
	trimmed := strings.TrimSpace(raw)
	if !IsReference(trimmed) {
		return Reference{}, fmt.Errorf("atlasregistry: %q is not an %s reference", raw, Scheme)
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return Reference{}, fmt.Errorf("atlasregistry: parse %q: %w", raw, err)
	}
	if err := refuseUnusedReferenceParts(parsed, raw); err != nil {
		return Reference{}, err
	}
	repository := strings.Trim(parsed.Host+parsed.Path, "/")
	if repository == "" {
		return Reference{}, fmt.Errorf("atlasregistry: %q names no repository", raw)
	}
	tag, immutable, err := referenceTag(parsed.Query(), raw)
	if err != nil {
		return Reference{}, err
	}
	namespace, err := Namespace()
	if err != nil {
		return Reference{}, err
	}
	return Reference{
		OCI:        OCIScheme + namespace + "/" + repository + ":" + tag,
		Repository: repository,
		Tag:        tag,
		Immutable:  immutable,
	}, nil
}

// refuseUnusedReferenceParts rejects the URL components this scheme has no
// meaning for, for the same reason an undocumented query parameter is rejected:
// a reference carrying one resolves to a DIFFERENT artifact than it names.
//
// `atlas://app#staging` parses as repository `app` with the fragment discarded,
// so without this it would quietly pull `app:latest` and run migrations from
// somewhere the author did not write.
func refuseUnusedReferenceParts(parsed *url.URL, raw string) error {
	switch {
	case parsed.Fragment != "":
		return unusedReferencePart(raw, "a fragment (#"+parsed.Fragment+")")
	case parsed.User != nil:
		// The value is deliberately not echoed: it can carry a password, and
		// this error reaches logs.
		return unusedReferencePart(raw, "user information")
	case parsed.Opaque != "":
		return unusedReferencePart(raw, "an opaque body ("+parsed.Opaque+")")
	}
	return nil
}

func unusedReferencePart(raw, part string) error {
	return fmt.Errorf(
		"atlasregistry: %q carries %s, which has no meaning here and would resolve to a different "+
			"artifact than the reference names; the supported forms are %s<repository>, ?tag=<tag> and ?version=<version>",
		redactReference(raw), part, Scheme)
}

// redactReference strips user information so a password in an authored
// reference does not reach a log through the refusal that rejected it.
func redactReference(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.User == nil {
		return raw
	}
	parsed.User = url.User("redacted")
	return parsed.String()
}

// referenceTag reads the tag out of the query, refusing the combination that
// names two different things and any parameter this package cannot map.
func referenceTag(query url.Values, raw string) (tag string, immutable bool, err error) {
	for name := range query {
		if name != "tag" && name != "version" {
			return "", false, fmt.Errorf(
				"atlasregistry: %q carries the query parameter %q, which has no documented meaning here; "+
					"the supported forms are %s<repository>, ?tag=<tag> and ?version=<version>",
				raw, name, Scheme)
		}
	}
	tagValue := strings.TrimSpace(query.Get("tag"))
	versionValue := strings.TrimSpace(query.Get("version"))
	switch {
	case tagValue != "" && versionValue != "":
		return "", false, fmt.Errorf(
			"atlasregistry: %q names both tag %q and version %q; a tag moves and a version does not, "+
				"so a reference carrying both names two different artifacts",
			raw, tagValue, versionValue)
	case versionValue != "":
		// A version is Ptah's write-once tag: the same immutability the vendor
		// spelling promises, expressed in the OCI model rather than in a
		// hosted service.
		return versionValue, true, nil
	case tagValue != "":
		return tagValue, false, nil
	default:
		return DefaultTag, false, nil
	}
}

// Namespace returns the configured OCI namespace.
//
// A run with none configured is refused. Ptah must never send an `atlas://`
// reference to a hosted service as an implicit fallback, and a reference that
// silently resolved to nothing would be worse than a refusal.
func Namespace() (string, error) {
	namespace := strings.Trim(strings.TrimSpace(os.Getenv(NamespaceEnvVar)), "/")
	if namespace == "" {
		return "", fmt.Errorf(
			"%s references require an OCI backing registry in Ptah: set %s to the namespace holding them, "+
				"for example %s=ghcr.io/acme, or write the oci:// reference itself",
			Scheme, NamespaceEnvVar, NamespaceEnvVar)
	}
	return namespace, nil
}
