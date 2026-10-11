package lintkit

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/lintext"
)

// File is one file of a check request, in the forms a bundled owner's rules
// read.
type File struct {
	// Name, Version, IsUp, IsDown, HasPair and NoTransaction are the
	// request's file: its name, its migration version, which half of a
	// migration it is, whether the other half exists, and whether it runs
	// outside a transaction.
	Name          string
	Version       int64
	IsUp          bool
	IsDown        bool
	HasPair       bool
	NoTransaction bool
	// Statements are the file's statements as the request carries them.
	Statements []lintext.Statement
	// Target is the server the run plans against, and Compatibility the
	// surface it reports on.
	Target        lintext.Target
	Compatibility string
	// Baseline is the schema state the file's version starts from, as the
	// request carries it. Its zero value resolves nothing, which is what a
	// rule meets on the first of the two calls a baseline rule is served in.
	Baseline State
}

// Rule is one rule a bundled owner checks in process. Exactly one of
// Statement and File is set, and BaselineSubjects is set exactly when the
// rule's input reads a baseline.
type Rule struct {
	// Rule is what the owner publishes.
	lintext.Rule
	// Statement reports whether a statement violates the rule, and the
	// message. It judges every statement of an up file, and of a down file
	// where the rule applies to down.
	Statement func(file *File, statement *lintext.Statement) (bool, string)
	// File reports the violations of one file. It sees every file and
	// decides for itself which it judges.
	File func(file *File) []lintext.Violation
	// BaselineSubjects names the statements of an up file the rule would
	// judge further with the schema state the file's version starts from.
	BaselineSubjects func(file *File) []int
}

// Rules is a [lintext.Service] over Go rules: what a bundled owner registers
// for rules a [lintext.Checks] cannot express, such as those that read a
// baseline.
type Rules []Rule

// Validate reports a rule whose published form does not validate, one that
// sets neither or both of Statement and File, one whose BaselineSubjects
// disagrees with its input, and two rules of one code. Each failure wraps
// lintext.ErrInvalid.
func (r Rules) Validate() error {
	codes := make([]string, 0, len(r))
	for _, rule := range r {
		if err := rule.Rule.Validate(); err != nil {
			return err
		}
		if (rule.Statement == nil) == (rule.File == nil) {
			return fmt.Errorf("%w: rule %s must set exactly one of Statement and File", lintext.ErrInvalid, rule.Code)
		}
		if (rule.BaselineSubjects != nil) != rule.Input.ReadsBaseline() {
			return fmt.Errorf("%w: rule %s sets BaselineSubjects exactly when its input reads a baseline", lintext.ErrInvalid, rule.Code)
		}
		if slices.Contains(codes, rule.Code) {
			return fmt.Errorf("%w: two rules of code %s", lintext.ErrInvalid, rule.Code)
		}
		codes = append(codes, rule.Code)
	}
	return nil
}

// Published returns the published form of every rule, in order.
func (r Rules) Published() []lintext.Rule {
	published := make([]lintext.Rule, 0, len(r))
	for _, rule := range r {
		published = append(published, rule.Rule.Clone())
	}
	return published
}

// CheckMigrations runs the rules the request enables over its files. It
// reports findings, statement findings in file and statement order, and the
// baseline subjects of every up file with a version. A code no rule carries
// wraps lintext.ErrInvalid, and cancellation returns the context's error;
// either returns no result.
func (r Rules) CheckMigrations(ctx context.Context, request lintext.CheckRequest) (lintext.CheckResult, error) {
	if err := r.Validate(); err != nil {
		return lintext.CheckResult{}, err
	}
	enabled := make([]Rule, 0, len(request.Codes))
	for _, code := range request.Codes {
		position := slices.IndexFunc(r, func(rule Rule) bool { return rule.Code == code })
		if position < 0 {
			return lintext.CheckResult{}, fmt.Errorf("%w: no rule of code %s", lintext.ErrInvalid, code)
		}
		enabled = append(enabled, r[position])
	}
	var states map[int64]State
	if request.Baseline != nil {
		states = NewStates(*request.Baseline)
	}
	result := lintext.CheckResult{Complete: true}
	for index := range request.Files {
		if err := ctx.Err(); err != nil {
			return lintext.CheckResult{}, err
		}
		requested := &request.Files[index]
		file := File{
			Name:          requested.Name,
			Version:       requested.Version,
			IsUp:          requested.Up,
			IsDown:        requested.Down,
			HasPair:       requested.HasPair,
			NoTransaction: requested.NoTransaction,
			Statements:    requested.Statements,
			Target:        request.Target,
			Compatibility: request.Compatibility,
			Baseline:      states[requested.Version],
		}
		for _, rule := range enabled {
			result.Findings = append(result.Findings, rule.findings(&file, index)...)
			if rule.BaselineSubjects != nil && file.IsUp && file.Version != 0 {
				if statements := rule.BaselineSubjects(&file); len(statements) > 0 {
					result.BaselineSubjects = append(result.BaselineSubjects,
						lintext.BaselineSubject{Code: rule.Code, File: index, Statements: slices.Clone(statements)})
				}
			}
		}
	}
	return result, ctx.Err()
}

// findings is one rule over one file, the file at index of its request.
func (r Rule) findings(file *File, index int) []lintext.Finding {
	var findings []lintext.Finding
	if r.File != nil {
		for _, violation := range r.File(file) {
			findings = append(findings, lintext.Finding{Code: r.Code, File: index, Statement: violation.Statement,
				Message: violation.Message, Subjects: slices.Clone(violation.Subjects)})
		}
		return findings
	}
	if !file.IsUp && (!file.IsDown || !r.AppliesToDown) {
		return nil
	}
	for position := range file.Statements {
		if found, message := r.Statement(file, &file.Statements[position]); found {
			findings = append(findings, lintext.Finding{Code: r.Code, File: index, Statement: position, Message: message})
		}
	}
	return findings
}
