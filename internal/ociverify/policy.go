// Package ociverify checks a published OCI artifact against a policy before
// anything consumes it. It answers the question a digest cannot: not "are these
// the bytes I named" but "should these bytes be applied to my database".
package ociverify

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

// PolicyVersion is the only format version this package reads. A policy file
// carrying another one is refused rather than interpreted, because a gate that
// silently ignores rules it does not understand is worse than no gate.
const PolicyVersion = 1

// Policy is what an artifact must satisfy to be consumed.
//
// Every field is a refusal an operator opted into. The zero policy therefore
// permits everything, which is the honest default: a policy nobody wrote should
// not start rejecting artifacts that were fine yesterday.
type Policy struct {
	Version int `yaml:"version"`
	// RequireDigestPin refuses a reference that names a tag rather than a
	// digest. A tag is a pointer somebody else can move between the moment a
	// pipeline decided to trust an artifact and the moment it applies it.
	RequireDigestPin bool `yaml:"require_digest_pin"`
	// ArtifactTypes is the permitted set. Empty permits any. It stops a
	// consumer that expected a migration directory from applying whatever else
	// happened to be published at that reference.
	ArtifactTypes []string `yaml:"artifact_types"`
	// RequireAnnotations names annotations that must be present and non-empty,
	// which is how a deployment refuses an artifact whose provenance nobody
	// recorded.
	RequireAnnotations []string `yaml:"require_annotations"`
	// RequireSignature demands that a signature be attached.
	//
	// Read what this does and does not do before relying on it. Ptah checks
	// that a referrer of a signature artifact type EXISTS. It does not verify
	// the signature: no key is loaded, no identity is checked, no cryptography
	// runs. Signing and cryptographic verification stay with cosign or
	// Notation, which own the trust material.
	//
	// A presence check is still worth having, because the failure it catches is
	// the common one: a pipeline that was supposed to sign and did not. It is
	// not worth mistaking for authenticity, which is why it is named for what
	// it measures.
	RequireSignature bool `yaml:"require_signature"`
	// SignatureArtifactTypes is what counts as a signature. Empty uses
	// DefaultSignatureArtifactTypes.
	SignatureArtifactTypes []string `yaml:"signature_artifact_types"`
}

// DefaultSignatureArtifactTypes are the artifact types the common signers
// attach. They are defaults rather than a closed set: a policy naming its own
// replaces them, because which tool signs is the operator's decision.
var DefaultSignatureArtifactTypes = []string{
	"application/vnd.dev.cosign.artifact.sig.v1+json",
	"application/vnd.cncf.notary.signature",
}

// LoadPolicy reads a policy file.
func LoadPolicy(path string) (Policy, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Policy{}, fmt.Errorf("read the verification policy: %w", err)
	}
	var policy Policy
	decoder := yaml.NewDecoder(strings.NewReader(string(contents)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&policy); err != nil {
		return Policy{}, fmt.Errorf("parse the verification policy %s: %w", path, err)
	}
	if err := policy.Validate(); err != nil {
		return Policy{}, fmt.Errorf("verification policy %s: %w", path, err)
	}
	return policy, nil
}

// Validate refuses a policy that cannot gate anything.
func (p Policy) Validate() error {
	if p.Version != PolicyVersion {
		return fmt.Errorf("unsupported version %d: expected %d", p.Version, PolicyVersion)
	}
	if p.blank() {
		return fmt.Errorf(
			"declares no requirement, so every artifact would pass; " +
				"remove the policy rather than keeping one that gates nothing")
	}
	for _, name := range p.RequireAnnotations {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("require_annotations contains an empty name")
		}
	}
	for _, artifactType := range p.ArtifactTypes {
		if strings.TrimSpace(artifactType) == "" {
			return fmt.Errorf("artifact_types contains an empty type")
		}
	}
	return nil
}

// blank reports a policy with nothing to enforce.
//
// It is refused rather than accepted because the two ways to have no policy are
// not the same: not passing one means the operator did not ask for a gate, and
// passing an empty one means they think they have one.
func (p Policy) blank() bool {
	return !p.RequireDigestPin &&
		!p.RequireSignature &&
		len(p.ArtifactTypes) == 0 &&
		len(p.RequireAnnotations) == 0
}

// signatureTypes returns the artifact types this policy counts as a signature.
func (p Policy) signatureTypes() []string {
	if len(p.SignatureArtifactTypes) > 0 {
		return slices.Clone(p.SignatureArtifactTypes)
	}
	return slices.Clone(DefaultSignatureArtifactTypes)
}
