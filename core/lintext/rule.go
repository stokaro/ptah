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
//
// # Baseline rules
//
// Some rules cannot judge a statement from its text. Renaming a column says
// nothing about the type the retired name carried, and an index build says
// nothing about whether its table is partitioned. A rule that needs the
// schema state a migration version starts from declares
// [InputBaselineSchema], or [InputBaselineRefinement] when the text alone
// supports a finding that the state refines. A host serves such a rule in two
// calls of [Service.CheckMigrations] over the same files and codes.
//
// The first call carries no [CheckRequest.Baseline]. The owner reports what
// the text supports, and lists in [CheckResult.BaselineSubjects], per rule and
// file, the statements it would judge further with the state that file's
// version starts from. It lists them on every call, whatever the request
// carries, because the host reads them each time to decide what the run still
// lacks. A rule that lists nothing costs the host nothing.
//
// The host keeps the subjects it can act on: those on an up file the run
// lints, that carries a version, where the run enables the rule, and on a
// statement in a schema the run reviews. Each file it keeps names a version
// the host reads. With a dev database, the host replays the migration
// directory on it in version order and, just before it applies each of those
// versions, reads the schemas the run reviews: every column, index and
// TimescaleDB hypertable, and the views and routines that read each column.
// Those are the rows of a [Baseline], each naming the version whose starting
// state it describes.
//
// The host then calls again, with Baseline holding the rows of every version
// it read. The second answer replaces the first, so the owner reports every
// finding again rather than what the state added. A version the host read no
// rows for resolves nothing, and its files are judged as on the first call. A
// host may call more than twice for one run, for instance again with a server
// version the dev database reported, and keeps the last answer. Without a dev
// database it makes no second call: it keeps the first answer and reports
// each kept subject's rule and file as an input the run did not supply, so a
// thinner report never reads as a clean one.
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
	// InputBaselineSchema also reads the schema state a version starts from,
	// which a host supplies in [CheckRequest.Baseline] for the statements the
	// rule names in [CheckResult.BaselineSubjects].
	InputBaselineSchema Input = "baseline-schema"
	// InputBaselineRefinement reports from the text alone and refines the
	// report with the schema state where a run supplies it, asked for the
	// way [InputBaselineSchema] asks.
	InputBaselineRefinement Input = "baseline-refinement"
)

// ReadsBaseline reports whether a rule with this input reads the schema state
// a version starts from, as its subject or as a refinement of the text.
func (i Input) ReadsBaseline() bool {
	return i == InputBaselineSchema || i == InputBaselineRefinement
}

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
