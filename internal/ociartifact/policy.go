package ociartifact

import (
	"fmt"
	"slices"
	"strings"
)

// ReferrerPolicyEnv names the referrer policy for a whole run.
//
// It is an environment variable rather than a flag because the decision belongs
// to the pipeline rather than to one command: every verb that attaches
// something -- lint reports, plans, deployment reports -- has to make the same
// choice, and a flag that only some of them carry is a guarantee with holes in
// it. An audit pipeline exports this once and every attachment it makes obeys.
const ReferrerPolicyEnv = "PTAH_OCI_REFERRER_POLICY"

// ReferrerPolicy decides how an attachment is made discoverable.
//
// There are two mechanisms and they do not have the same reach. The referrers
// index defined by the distribution specification is what every conformant
// client reads. Ptah's content-derived durable tag is readable by Ptah and by
// anyone who knows the naming rule, which in practice means Ptah.
//
// Writing both is the safest thing to do when nothing is known about the
// registry and the worst thing to report, because a run that wrote both cannot
// tell an operator whether the guarantee they need actually holds. The policy
// exists so that the question is answered before the artifact is published
// rather than discovered afterwards by a client that could not find it.
type ReferrerPolicy string

const (
	// ReferrerPolicyAuto asks the registry and uses the referrers index where
	// it exists, falling back to the durable tag where it does not. This is
	// the default: it produces the strongest discovery each registry can
	// actually provide, and reports which one that was.
	ReferrerPolicyAuto ReferrerPolicy = "auto"
	// ReferrerPolicyAPI uses the referrers index and does not write the
	// durable tag. A registry without the index fails the publish rather than
	// receiving an attachment only Ptah can find.
	ReferrerPolicyAPI ReferrerPolicy = "api"
	// ReferrerPolicyRequiredAPI is ReferrerPolicyAPI with the question asked
	// first. The registry is checked before anything is written, so a pipeline
	// that must not publish an undiscoverable attachment fails without having
	// created one. This is the setting an audit trail needs.
	ReferrerPolicyRequiredAPI ReferrerPolicy = "required-api"
	// ReferrerPolicyTag writes the durable tag alone. It exists for a registry
	// whose index is present but wrong, where trusting it would be worse than
	// not using it.
	ReferrerPolicyTag ReferrerPolicy = "tag"
)

// referrerPolicies is the closed set, in the order documentation presents it:
// weakest promise first, then the two that demand the index, then the escape.
var referrerPolicies = []ReferrerPolicy{
	ReferrerPolicyAuto,
	ReferrerPolicyAPI,
	ReferrerPolicyRequiredAPI,
	ReferrerPolicyTag,
}

// ReferrerPolicies returns every policy, in presentation order.
func ReferrerPolicies() []ReferrerPolicy {
	return slices.Clone(referrerPolicies)
}

// ParseReferrerPolicy converts operator input into a policy. The empty string
// is the default rather than an error, so a caller that never set one behaves
// the way an unconfigured run should.
func ParseReferrerPolicy(value string) (ReferrerPolicy, error) {
	normalized := ReferrerPolicy(strings.ToLower(strings.TrimSpace(value)))
	if normalized == "" {
		return ReferrerPolicyAuto, nil
	}
	if slices.Contains(referrerPolicies, normalized) {
		return normalized, nil
	}
	return "", fmt.Errorf("unsupported referrer policy %q: expected %s",
		value, strings.Join(referrerPolicyNames(), ", "))
}

func referrerPolicyNames() []string {
	names := make([]string, 0, len(referrerPolicies))
	for _, policy := range referrerPolicies {
		names = append(names, string(policy))
	}
	return names
}

// normalized maps the zero value onto the default.
func (p ReferrerPolicy) normalized() ReferrerPolicy {
	if p == "" {
		return ReferrerPolicyAuto
	}
	return p
}

// String makes the policy printable without a conversion at every call site.
func (p ReferrerPolicy) String() string {
	return string(p.normalized())
}

// RequiresIndex reports whether the policy refuses to rely on the durable tag.
func (p ReferrerPolicy) RequiresIndex() bool {
	switch p.normalized() {
	case ReferrerPolicyAPI, ReferrerPolicyRequiredAPI:
		return true
	default:
		return false
	}
}

// WritesDurableTag reports whether an attachment under this policy also gets
// Ptah's content-derived tag. It is false for the index policies on purpose: a
// tag written beside an index the operator demanded would make a failed
// guarantee look like a satisfied one on the next read.
func (p ReferrerPolicy) WritesDurableTag() bool {
	return !p.RequiresIndex()
}
