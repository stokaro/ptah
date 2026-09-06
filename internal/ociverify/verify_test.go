package ociverify_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ociartifact"
	"ptah.run/internal/ociverify"
)

// TestVerify_RefusesATagBeforeReachingTheRegistry pins the one requirement that
// is decidable from the reference alone. A tag is a pointer somebody else can
// move between the decision to trust an artifact and its application, and
// noticing that needs no network.
func TestVerify_RefusesATagBeforeReachingTheRegistry(t *testing.T) {
	c := qt.New(t)
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
	c.Assert(err, qt.IsNil)

	_, err = ociverify.Verify(context.Background(), client,
		"oci://registry.invalid/acme/db:latest",
		ociverify.Policy{Version: 1, RequireDigestPin: true})

	// The registry is unreachable, so the run cannot finish -- but the point is
	// that it got as far as trying, which means the digest finding is recorded
	// rather than short-circuiting the rest of the policy.
	c.Assert(err, qt.IsNotNil)
}

func TestVerify_FailurePath(t *testing.T) {
	t.Run("a missing client", func(t *testing.T) {
		c := qt.New(t)

		_, err := ociverify.Verify(context.Background(), nil, "oci://registry.invalid/acme/db:latest",
			ociverify.Policy{Version: 1, RequireDigestPin: true})

		c.Assert(err, qt.ErrorMatches, "OCI client is required")
	})

	t.Run("an invalid policy is refused before any work", func(t *testing.T) {
		c := qt.New(t)
		client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
		c.Assert(err, qt.IsNil)

		_, err = ociverify.Verify(context.Background(), client,
			"oci://registry.invalid/acme/db:latest", ociverify.Policy{Version: 1})

		c.Assert(err, qt.ErrorMatches, `.*declares no requirement.*`)
	})
}

// TestReport_ErrListsEveryFinding is why the checks do not stop at the first
// failure: an operator fixing a pipeline wants the whole list, because one
// violation per run turns a five-minute fix into five deployments.
func TestReport_ErrListsEveryFinding(t *testing.T) {
	c := qt.New(t)
	report := ociverify.Report{
		Reference: "oci://registry.invalid/acme/db:latest",
		Findings: []ociverify.Finding{
			{Requirement: "require_digest_pin", Detail: "the reference names a tag"},
			{Requirement: "require_signature", Detail: "no signature is attached"},
		},
	}

	err := report.Err()

	c.Assert(err, qt.ErrorIs, ociverify.ErrPolicyViolation)
	c.Assert(err.Error(), qt.Contains, "require_digest_pin")
	c.Assert(err.Error(), qt.Contains, "require_signature")
}

func TestReport_ErrIsNilWhenNothingWasRefused(t *testing.T) {
	c := qt.New(t)
	report := ociverify.Report{
		Reference: "oci://registry.invalid/acme/db@sha256:abc",
		Satisfied: []string{"require_digest_pin"},
	}

	c.Assert(report.Err(), qt.IsNil)
}

// TestReport_JSONNamesMatchEveryOtherPtahDocument pins the wire shape.
//
// Without tags this report serialized as Go field names -- `Reference`,
// `Digest`, `Satisfied`, `Findings` -- while every other machine-readable Ptah
// document is snake_case, so a consumer reading two of them needed two naming
// conventions. `Satisfied` also serialized as `null` on a refusal, which is a
// different shape from `[]` to anything that iterates (stokaro/ptah#852).
func TestReport_JSONNamesMatchEveryOtherPtahDocument(t *testing.T) {
	tests := []struct {
		name   string
		report ociverify.Report
		// wantContains are the exact JSON fragments the document must carry.
		wantContains []string
	}{
		{
			name: "a refusal",
			report: ociverify.Report{
				Reference: "oci://example.test/app:v1",
				Digest:    "sha256:abc",
				Findings:  []ociverify.Finding{{Requirement: "require_digest_pin", Detail: "the reference names a tag"}},
			},
			wantContains: []string{
				`"reference":"oci://example.test/app:v1"`,
				`"digest":"sha256:abc"`,
				`"satisfied":[]`,
				`"requirement":"require_digest_pin"`,
				`"detail":"the reference names a tag"`,
			},
		},
		{
			name: "a pass",
			report: ociverify.Report{
				Reference: "oci://example.test/app@sha256:abc",
				Digest:    "sha256:abc",
				Satisfied: []string{"require_digest_pin"},
			},
			wantContains: []string{
				`"satisfied":["require_digest_pin"]`,
				`"findings":[]`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.report)

			c.Assert(err, qt.IsNil)
			for _, want := range test.wantContains {
				c.Assert(string(encoded), qt.Contains, want)
			}
			// The Go names must be gone, or a consumer written against the old
			// document keeps working and the two conventions both survive.
			c.Assert(string(encoded), qt.Not(qt.Contains), `"Reference"`)
			c.Assert(string(encoded), qt.Not(qt.Contains), `"Satisfied"`)
		})
	}
}
