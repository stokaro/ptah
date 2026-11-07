package conformance

import (
	"errors"
	"fmt"
	"strings"
)

// Status is the outcome of one check.
type Status string

const (
	// Passed reports a check the provider satisfied.
	Passed Status = "passed"
	// Failed reports a check the provider did not satisfy.
	Failed Status = "failed"
	// NotApplicable reports a check that does not apply: the provider does
	// not offer what it checks, or the configuration gives the suite nothing
	// to check it with.
	NotApplicable Status = "not-applicable"
)

// Result is the outcome of one check.
type Result struct {
	// ID names the check: an entry of Checks, followed for a call or a
	// change by a dot and its name.
	ID string `json:"id"`
	// Status is the outcome.
	Status Status `json:"status"`
	// Detail says what failed, or why the check does not apply. It never
	// quotes the live address or the canary.
	Detail string `json:"detail,omitempty"`
}

// Identity is who the provider says it is, from its description under a
// host that lists every two-way feature.
type Identity struct {
	ID               string   `json:"id"`
	Version          string   `json:"version"`
	SemanticIdentity string   `json:"semantic_identity"`
	MinContract      string   `json:"min_contract"`
	Engines          []string `json:"engines"`
	Features         []string `json:"features"`
}

// Report is the machine-readable result of a run. Results are in the order
// the checks ran, and that order is the same on every run of one
// configuration.
type Report struct {
	// Suite names the suite: always "ptah-provider-conformance".
	Suite string `json:"suite"`
	// Contract is the contract version the suite checks against, the one
	// this SDK speaks.
	Contract string `json:"contract"`
	// Target is how the suite reached the provider: "executable",
	// "in-process" or "wire".
	Target string `json:"target"`
	// Provider is nil when the provider never described itself.
	Provider *Identity `json:"provider,omitempty"`
	Results  []Result  `json:"results"`
}

// ErrNonconforming reports a run in which at least one check failed.
var ErrNonconforming = errors.New("the provider does not conform")

// Failures returns the results that failed, in report order.
func (r *Report) Failures() []Result {
	var failed []Result
	for _, result := range r.Results {
		if result.Status == Failed {
			failed = append(failed, result)
		}
	}
	return failed
}

// Err returns nil when no check failed, and otherwise an error wrapping
// ErrNonconforming that lists each failed check and its detail.
func (r *Report) Err() error {
	failed := r.Failures()
	if len(failed) == 0 {
		return nil
	}
	lines := make([]string, 0, len(failed))
	for _, result := range failed {
		lines = append(lines, result.ID+": "+result.Detail)
	}
	return fmt.Errorf("%w: %d of %d checks failed:\n%s", ErrNonconforming, len(failed), len(r.Results), strings.Join(lines, "\n"))
}
