package ociartifact_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/ociartifact"
)

func TestParseReferrerPolicy(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  ociartifact.ReferrerPolicy
	}{
		{input: "", want: ociartifact.ReferrerPolicyAuto},
		{input: "auto", want: ociartifact.ReferrerPolicyAuto},
		{input: "  AUTO  ", want: ociartifact.ReferrerPolicyAuto},
		{input: "api", want: ociartifact.ReferrerPolicyAPI},
		{input: "required-api", want: ociartifact.ReferrerPolicyRequiredAPI},
		{input: "tag", want: ociartifact.ReferrerPolicyTag},
	} {
		t.Run("accepts "+tc.input, func(t *testing.T) {
			c := qt.New(t)

			got, err := ociartifact.ParseReferrerPolicy(tc.input)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tc.want)
		})
	}
}

func TestParseReferrerPolicy_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := ociartifact.ParseReferrerPolicy("best-effort")

	c.Assert(err, qt.ErrorMatches,
		`unsupported referrer policy "best-effort": expected auto, api, required-api, tag`)
}

// TestReferrerPolicy_DurableTagFollowsTheIndexDemand pins the rule the policy
// exists to enforce: a tag written beside an index the operator demanded would
// make a failed guarantee read as a satisfied one on the next listing.
func TestReferrerPolicy_DurableTagFollowsTheIndexDemand(t *testing.T) {
	for _, tc := range []struct {
		policy  ociartifact.ReferrerPolicy
		index   bool
		durable bool
	}{
		{policy: ociartifact.ReferrerPolicyAuto, index: false, durable: true},
		{policy: ociartifact.ReferrerPolicyAPI, index: true, durable: false},
		{policy: ociartifact.ReferrerPolicyRequiredAPI, index: true, durable: false},
		{policy: ociartifact.ReferrerPolicyTag, index: false, durable: true},
	} {
		t.Run(string(tc.policy), func(t *testing.T) {
			c := qt.New(t)

			c.Assert(tc.policy.RequiresIndex(), qt.Equals, tc.index)
			c.Assert(tc.policy.WritesDurableTag(), qt.Equals, tc.durable)
		})
	}
}

func TestReferrerPolicies_IsTheClosedSet(t *testing.T) {
	c := qt.New(t)

	got := ociartifact.ReferrerPolicies()

	c.Assert(got, qt.DeepEquals, []ociartifact.ReferrerPolicy{
		ociartifact.ReferrerPolicyAuto,
		ociartifact.ReferrerPolicyAPI,
		ociartifact.ReferrerPolicyRequiredAPI,
		ociartifact.ReferrerPolicyTag,
	})
}

// TestNewClient_ReadsThePolicyFromTheEnvironment pins the lever an audit
// pipeline actually uses. Every verb that attaches something has to make the
// same choice, so the setting is exported once for the run rather than passed
// to whichever command happened to grow a flag.
func TestNewClient_ReadsThePolicyFromTheEnvironment(t *testing.T) {
	t.Run("an explicit option wins over the environment", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(ociartifact.ReferrerPolicyEnv, "tag")

		client, err := ociartifact.NewClient(ociartifact.ClientOptions{
			ReferrerPolicy: ociartifact.ReferrerPolicyRequiredAPI,
		})

		c.Assert(err, qt.IsNil)
		c.Assert(client, qt.IsNotNil)
	})

	t.Run("an unreadable value fails the client rather than the publish", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(ociartifact.ReferrerPolicyEnv, "best-effort")

		_, err := ociartifact.NewClient(ociartifact.ClientOptions{})

		c.Assert(err, qt.ErrorMatches,
			`read PTAH_OCI_REFERRER_POLICY: unsupported referrer policy "best-effort": expected auto, api, required-api, tag`)
	})

	t.Run("an empty environment is the default", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(ociartifact.ReferrerPolicyEnv, "")

		client, err := ociartifact.NewClient(ociartifact.ClientOptions{})

		c.Assert(err, qt.IsNil)
		c.Assert(client, qt.IsNotNil)
	})
}
