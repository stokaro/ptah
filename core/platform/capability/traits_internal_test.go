package capability

// White-box testing required: the census below has to enumerate the policy list
// this package validates against, and foreignKeyReferencePolicies is unexported
// with no exported API that returns it. An external test can only retype the
// keys, and a retyped list cannot see a policy that was added to the real one —
// which is precisely the drift the census exists to catch. Measured on a copy
// of this repository: a fourth policy added to the registry, to mutexGroups, to
// foreignKeyReferencePolicies and to every preset, with referencePolicyNames
// left at three entries, kept every test in this package PASSING while the
// census was a hand-typed literal of three. The same mutant fails the census
// below.

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestForeignKeyReference_NamesEveryValidatedPolicy is the census that keeps
// [Capabilities.ForeignKeyReference] and the list [Capabilities.Validate]
// polices in step.
//
// A policy on that list with no entry in referencePolicyNames is not a rejected
// set: Validate ACCEPTS foreign keys plus exactly that one policy, and the
// derivation then answers ReferenceUnspecified — the zero value reserved for
// sets Validate rejects. That renders foreign_key_reference as an empty string
// in `ptah db capabilities` output, so the failure mode is a wrong answer that
// nothing else in the package notices. Asserting Validate accepts each row is
// what makes the rest of the row mean something: without it the census could be
// satisfied by a set no caller can legally build.
func TestForeignKeyReference_NamesEveryValidatedPolicy(t *testing.T) {
	for _, policy := range foreignKeyReferencePolicies {
		t.Run(string(policy), func(t *testing.T) {
			c := qt.New(t)

			caps := Capabilities{ForeignKeys: true, policy: true}

			c.Assert(caps.Validate(), qt.IsNil)
			c.Assert(caps.ForeignKeyReference(), qt.Not(qt.Equals), ReferenceUnspecified)
			c.Assert(caps.ForeignKeyReference(), qt.Not(qt.Equals), ReferenceUnsupported)
		})
	}
}

// TestReferencePolicyNames_CarriesOneDistinctModePerPolicy covers the two drifts
// the census above cannot see, because both leave every policy resolving to
// something: a name for a key that is no longer a validated policy (the map
// outgrows the list), and two policies sharing one mode value, which would make
// [Capabilities.ForeignKeyReference] report a policy the set did not name.
func TestReferencePolicyNames_CarriesOneDistinctModePerPolicy(t *testing.T) {
	c := qt.New(t)

	modes := make([]ReferencePolicy, 0, len(foreignKeyReferencePolicies))
	for _, policy := range foreignKeyReferencePolicies {
		modes = append(modes, referencePolicyNames[policy])
	}
	slices.Sort(modes)

	c.Assert(referencePolicyNames, qt.HasLen, len(foreignKeyReferencePolicies))
	c.Assert(slices.Compact(modes), qt.HasLen, len(foreignKeyReferencePolicies))
}
