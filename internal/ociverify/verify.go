package ociverify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/ociartifact"
)

// ErrPolicyViolation reports an artifact a policy refuses.
var ErrPolicyViolation = errors.New("the artifact does not satisfy the verification policy")

// Finding is one requirement an artifact failed.
//
// The JSON names are snake_case because every other machine-readable document
// Ptah writes is -- `drift`, `highest_severity`, `capability_preset`. Without
// tags this one serialized as Go field names, so a consumer reading two Ptah
// documents needed two naming conventions (stokaro/ptah#852).
type Finding struct {
	Requirement string `json:"requirement"`
	Detail      string `json:"detail"`
}

// Report is the outcome of checking one artifact.
type Report struct {
	Reference string `json:"reference"`
	Digest    string `json:"digest"`
	// Satisfied names the requirements the artifact met, so a passing run says
	// what it actually checked rather than only that it passed.
	Satisfied []string  `json:"satisfied"`
	Findings  []Finding `json:"findings"`
}

// MarshalJSON writes the two lists as arrays rather than null.
//
// A refusal satisfied nothing and a pass found nothing, so one of the two is
// empty in every real report. `null` and `[]` are different shapes to a
// consumer that iterates, and the one it gets should not depend on the outcome.
func (r Report) MarshalJSON() ([]byte, error) {
	// The alias drops this method, so marshalling the copy does not recurse.
	// The copy is what gets the empty lists: the receiver is by value and
	// assigning to it would be a change nobody can observe.
	type report Report
	encoded := report(r)
	if encoded.Satisfied == nil {
		encoded.Satisfied = make([]string, 0)
	}
	if encoded.Findings == nil {
		encoded.Findings = make([]Finding, 0)
	}
	return json.Marshal(encoded)
}

// Err returns the refusal, or nil when the artifact satisfied the policy.
func (r Report) Err() error {
	if len(r.Findings) == 0 {
		return nil
	}
	var details strings.Builder
	for _, finding := range r.Findings {
		fmt.Fprintf(&details, "\n  %s: %s", finding.Requirement, finding.Detail)
	}
	return fmt.Errorf("%w (%s)%s", ErrPolicyViolation, r.Reference, details.String())
}

// Verify checks one artifact against a policy.
//
// Every requirement is evaluated, not just the first that fails. An operator
// fixing a pipeline wants the whole list, because finding one violation per run
// turns a five-minute fix into five deployments.
func Verify(
	ctx context.Context,
	client *ociartifact.Client,
	reference string,
	policy Policy,
) (Report, error) {
	if client == nil {
		return Report{}, fmt.Errorf("OCI client is required")
	}
	if err := policy.Validate(); err != nil {
		return Report{}, err
	}
	ref, err := ociartifact.ParseRef(reference)
	if err != nil {
		return Report{}, err
	}
	report := Report{Reference: ref.String()}

	if policy.RequireDigestPin {
		if !ref.IsDigest() {
			report.Findings = append(report.Findings, Finding{
				Requirement: "require_digest_pin",
				Detail: "the reference names a tag, which is a pointer somebody else can move " +
					"between the decision to trust this artifact and its application",
			})
		} else {
			report.Satisfied = append(report.Satisfied, "require_digest_pin")
		}
	}

	info, err := client.Inspect(ctx, reference)
	if err != nil {
		return Report{}, err
	}
	report.Digest = info.Descriptor.Digest.String()

	if len(policy.ArtifactTypes) > 0 {
		if slices.Contains(policy.ArtifactTypes, info.ArtifactType) {
			report.Satisfied = append(report.Satisfied, "artifact_types")
		} else {
			report.Findings = append(report.Findings, Finding{
				Requirement: "artifact_types",
				Detail: fmt.Sprintf("the artifact is %q, and the policy permits %s",
					info.ArtifactType, strings.Join(policy.ArtifactTypes, ", ")),
			})
		}
	}

	var missing []string
	for _, name := range policy.RequireAnnotations {
		if strings.TrimSpace(info.Annotations[name]) == "" {
			missing = append(missing, name)
		}
	}
	if len(policy.RequireAnnotations) > 0 {
		if len(missing) == 0 {
			report.Satisfied = append(report.Satisfied, "require_annotations")
		} else {
			report.Findings = append(report.Findings, Finding{
				Requirement: "require_annotations",
				Detail:      "absent or empty: " + strings.Join(missing, ", "),
			})
		}
	}

	if policy.RequireSignature {
		signed, err := hasSignature(ctx, client, reference, policy.signatureTypes())
		if err != nil {
			return Report{}, err
		}
		if signed {
			report.Satisfied = append(report.Satisfied, "require_signature")
		} else {
			report.Findings = append(report.Findings, Finding{
				Requirement: "require_signature",
				Detail: "no signature is attached; note that this checks a signature EXISTS " +
					"and never that it is valid, which stays with the signing tool",
			})
		}
	}
	return report, nil
}

// hasSignature reports whether a signature of one of the named types is
// attached to the artifact.
func hasSignature(
	ctx context.Context,
	client *ociartifact.Client,
	reference string,
	signatureTypes []string,
) (bool, error) {
	_, discovered, err := client.DiscoverReferrers(ctx, reference, "")
	if err != nil {
		return false, err
	}
	for _, referrer := range discovered {
		if slices.Contains(signatureTypes, referrer.Descriptor.ArtifactType) {
			return true, nil
		}
	}
	return false, nil
}
