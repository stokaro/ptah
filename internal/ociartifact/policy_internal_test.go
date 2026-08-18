package ociartifact

// White-box testing required: resolveAttachmentPolicy is the decision that
// turns an operator's policy into the write that happens, and it is unexported
// because it is not a registry operation. Reaching it from outside would mean
// standing up two registries that differ in whether they serve the referrers
// index, which is the one property a test registry is least able to vary.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
)

// countingProbe answers a fixed verdict and records that it was asked.
func countingProbe(supported bool, detail string, err error) (indexProbe, *int) {
	calls := 0
	return func(context.Context) (bool, string, error) {
		calls++
		return supported, detail, err
	}, &calls
}

func TestResolveAttachmentPolicy_AutoUsesTheStrongestTheRegistryHas(t *testing.T) {
	t.Run("index present becomes api", func(t *testing.T) {
		c := qt.New(t)
		probe, calls := countingProbe(true, "", nil)

		got, err := resolveAttachmentPolicy(context.Background(), ReferrerPolicyAuto, probe)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, ReferrerPolicyAPI)
		c.Assert(got.WritesDurableTag(), qt.IsFalse,
			qt.Commentf("a registry that indexes the attachment does not also need Ptah's tag"))
		c.Assert(*calls, qt.Equals, 1)
	})

	t.Run("index absent becomes tag", func(t *testing.T) {
		c := qt.New(t)
		probe, calls := countingProbe(false, "referrers API unsupported", nil)

		got, err := resolveAttachmentPolicy(context.Background(), ReferrerPolicyAuto, probe)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, ReferrerPolicyTag)
		c.Assert(got.WritesDurableTag(), qt.IsTrue)
		c.Assert(*calls, qt.Equals, 1)
	})

	t.Run("the zero value is auto", func(t *testing.T) {
		c := qt.New(t)
		probe, calls := countingProbe(true, "", nil)

		got, err := resolveAttachmentPolicy(context.Background(), "", probe)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, ReferrerPolicyAPI)
		c.Assert(*calls, qt.Equals, 1)
	})
}

// TestResolveAttachmentPolicy_RequiredAPIRefusesBeforeWriting is the reason
// required-api exists as a separate policy from api. Both end up demanding the
// index; only this one answers before an artifact has been created, so the
// pipeline that must not publish an undiscoverable attachment never publishes
// one.
func TestResolveAttachmentPolicy_RequiredAPIRefusesBeforeWriting(t *testing.T) {
	c := qt.New(t)
	probe, calls := countingProbe(false, "referrers API unsupported", nil)

	_, err := resolveAttachmentPolicy(context.Background(), ReferrerPolicyRequiredAPI, probe)

	c.Assert(err, qt.ErrorIs, ErrReferrerIndexRequired)
	c.Assert(err.Error(), qt.Contains, "referrers API unsupported",
		qt.Commentf("the registry's own words say why, so the refusal is actionable"))
	c.Assert(*calls, qt.Equals, 1)
}

func TestResolveAttachmentPolicy_RequiredAPIPassesWhenTheIndexIsThere(t *testing.T) {
	c := qt.New(t)
	probe, _ := countingProbe(true, "", nil)

	got, err := resolveAttachmentPolicy(context.Background(), ReferrerPolicyRequiredAPI, probe)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, ReferrerPolicyAPI)
}

// TestResolveAttachmentPolicy_ExplicitPoliciesDoNotAsk pins that naming a
// policy spends no round trip. An operator who already knows what their
// registry does should not pay for the question on every attachment.
func TestResolveAttachmentPolicy_ExplicitPoliciesDoNotAsk(t *testing.T) {
	for _, tc := range []struct {
		policy ReferrerPolicy
		want   ReferrerPolicy
	}{
		{policy: ReferrerPolicyAPI, want: ReferrerPolicyAPI},
		{policy: ReferrerPolicyTag, want: ReferrerPolicyTag},
	} {
		t.Run(string(tc.policy), func(t *testing.T) {
			c := qt.New(t)
			probe, calls := countingProbe(false, "", errors.New("the probe must not run"))

			got, err := resolveAttachmentPolicy(context.Background(), tc.policy, probe)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tc.want)
			c.Assert(*calls, qt.Equals, 0)
		})
	}
}

func TestResolveAttachmentPolicy_FailurePath(t *testing.T) {
	t.Run("a probe failure is not a no", func(t *testing.T) {
		c := qt.New(t)
		probe, _ := countingProbe(false, "", errors.New("connection refused"))

		_, err := resolveAttachmentPolicy(context.Background(), ReferrerPolicyAuto, probe)

		c.Assert(err, qt.ErrorMatches, "ask the registry for the referrers index: connection refused")
		c.Assert(err, qt.Not(qt.ErrorIs), ErrReferrerIndexRequired,
			qt.Commentf("a registry that could not be asked has not answered no"))
	})
}
