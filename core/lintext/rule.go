// Package lintext is the contract between migration lint and the engine
// owners that contribute to it: the rules an owner publishes, as data, the
// statement splitting of the owner's dialect, and the batched check that
// reports an owner's findings. A host reads every published rule without
// running one, so a catalog and its generated documentation enumerate them,
// and calls an owner once per lint run, never once per statement, so an owner
// may run in another process.
//
// The contract carries no Go closure. An in-process owner may keep its checks
// as Go functions behind [Checks]; a remote one answers the same requests
// over its transport.
package lintext

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// ErrInvalid marks a malformed rule, request or reply: a rule that does not
// validate, a split that does not cover its source, a finding naming a rule
// or a statement the request did not hold. A host refuses such a reply rather
// than read part of it.
var ErrInvalid = errors.New("invalid lint contract value")

// Severity is how urgent a rule's findings are. A host may override it per
// rule from its configuration.
type Severity string

const (
	// Info findings are reported and never gate.
	Info Severity = "info"
	// Warning findings deserve review before a production rollout.
	Warning Severity = "warning"
	// Error findings block by default.
	Error Severity = "error"
)

// Input names what a rule reads besides the statement text. The empty value
// is the text alone.
type Input string

const (
	// InputStatementText reads the migration SQL and nothing else.
	InputStatementText Input = ""
	// InputRoutineBody also reads the parsed body of a routine a statement
	// defines, which [Statement.Routine] carries.
	InputRoutineBody Input = "routine-body"
	// InputBaselineSchema also reads the schema state a version starts from.
	InputBaselineSchema Input = "baseline-schema"
	// InputBaselineRefinement reports from the text alone and refines the
	// report with the schema state where a run supplies it.
	InputBaselineRefinement Input = "baseline-refinement"
)

// Rule describes one rule an owner checks, as data a host reads without
// running it.
type Rule struct {
	// Code is the stable identifier a finding carries, such as MY101: an
	// uppercase ASCII letter followed by uppercase ASCII letters and digits.
	// A code belongs to one rule across every owner a runtime holds.
	Code string `json:"code"`
	// Title is the rule's short name in reports.
	Title string `json:"title"`
	// Severity is the default urgency of the rule's findings.
	Severity Severity `json:"severity"`
	// Targets are the canonical names of the targets the rule judges, at
	// least one. A run that names a target sends the rule only when it is
	// one of them; a run that names none sends every rule.
	Targets []string `json:"targets"`
	// AppliesToDown extends a statement rule to the rollback half of a
	// migration. A rule describing a forward change leaves it false.
	AppliesToDown bool `json:"applies_to_down,omitempty"`
	// Input is what the rule reads besides the text.
	Input Input `json:"input,omitempty"`
	// Subsumes names rules whose finding on the same statement this rule's
	// finding replaces.
	Subsumes []string `json:"subsumes,omitempty"`
}

// Validate reports a rule a host cannot publish: a code outside the
// convention, no title, an unknown severity or input, no target, a blank or
// repeated target, or a rule that subsumes itself. Each failure wraps
// [ErrInvalid].
func (r Rule) Validate() error {
	if !CanonicalCode(r.Code) {
		return fmt.Errorf("%w: rule code %q must start with an uppercase ASCII letter and contain only uppercase ASCII letters and digits",
			ErrInvalid, r.Code)
	}
	if strings.TrimSpace(r.Title) == "" {
		return fmt.Errorf("%w: rule %s has no title", ErrInvalid, r.Code)
	}
	switch r.Severity {
	case Info, Warning, Error:
	default:
		return fmt.Errorf("%w: rule %s has unknown severity %q", ErrInvalid, r.Code, r.Severity)
	}
	switch r.Input {
	case InputStatementText, InputRoutineBody, InputBaselineSchema, InputBaselineRefinement:
	default:
		return fmt.Errorf("%w: rule %s has unknown input %q", ErrInvalid, r.Code, r.Input)
	}
	if len(r.Targets) == 0 {
		return fmt.Errorf("%w: rule %s names no target", ErrInvalid, r.Code)
	}
	for i, target := range r.Targets {
		if strings.TrimSpace(target) == "" || slices.Contains(r.Targets[:i], target) {
			return fmt.Errorf("%w: rule %s names a blank or repeated target", ErrInvalid, r.Code)
		}
	}
	if slices.Contains(r.Subsumes, r.Code) {
		return fmt.Errorf("%w: rule %s subsumes itself", ErrInvalid, r.Code)
	}
	return nil
}

// Clone returns a rule that shares no slice with r.
func (r Rule) Clone() Rule {
	r.Targets = slices.Clone(r.Targets)
	r.Subsumes = slices.Clone(r.Subsumes)
	return r
}

// Judges reports whether the rule judges target. Every rule judges a run
// that names no target.
func (r Rule) Judges(target string) bool {
	return target == "" || slices.Contains(r.Targets, target)
}

// CanonicalCode reports whether code follows the rule-code convention: an
// uppercase ASCII letter followed by uppercase ASCII letters and digits.
func CanonicalCode(code string) bool {
	for index, value := range code {
		if value >= 'A' && value <= 'Z' {
			continue
		}
		if index > 0 && value >= '0' && value <= '9' {
			continue
		}
		return false
	}
	return code != ""
}
