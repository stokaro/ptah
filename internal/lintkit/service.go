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
	// Target is the server the run plans against, Compatibility the surface
	// it reports on, and TransactionOwner who opens the transaction each file
	// runs in.
	Target           lintext.Target
	Compatibility    string
	TransactionOwner lintext.TransactionOwner
	// Baseline is the schema state the file's version starts from, as the
	// request carries it. Its zero value resolves nothing, which is what a
	// rule meets on the first of the two calls a baseline rule is served in.
	Baseline State
}

// Rule is one rule a bundled owner checks in process. Exactly one of
// Statement, File and Directory is set, and BaselineSubjects is set exactly
// when the rule's input reads a baseline.
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
	// Directory reports the violations of every file of the request at once,
	// those of files[i] at index i, for a rule whose verdict on a file
	// depends on the files the directory applies before it. It sees every
	// file, in request order, and decides for itself which it judges.
	Directory func(files []File) [][]lintext.Violation
	// BaselineSubjects names the statements of an up file the rule would
	// judge further with the schema state the file's version starts from.
	BaselineSubjects func(file *File) []int
}

// Rules is a [lintext.Service] over Go rules: what a bundled owner registers
// for rules a [lintext.Checks] cannot express, such as those that read a
// baseline.
type Rules []Rule

// Validate reports a rule whose published form does not validate, one that
// sets other than one of Statement, File and Directory, one whose BaselineSubjects
// disagrees with its input, and two rules of one code. Each failure wraps
// lintext.ErrInvalid.
func (r Rules) Validate() error {
	codes := make([]string, 0, len(r))
	for _, rule := range r {
		if err := rule.Rule.Validate(); err != nil {
			return err
		}
		if rule.checkers() != 1 {
			return fmt.Errorf("%w: rule %s must set exactly one of Statement, File and Directory", lintext.ErrInvalid, rule.Code)
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
	files := make([]File, len(request.Files))
	for index := range request.Files {
		requested := &request.Files[index]
		files[index] = File{
			Name:             requested.Name,
			Version:          requested.Version,
			IsUp:             requested.Up,
			IsDown:           requested.Down,
			HasPair:          requested.HasPair,
			NoTransaction:    requested.NoTransaction,
			Statements:       requested.Statements,
			Target:           request.Target,
			Compatibility:    request.Compatibility,
			TransactionOwner: request.TransactionOwner,
			Baseline:         states[requested.Version],
		}
	}
	directories := make(map[string][][]lintext.Violation)
	for _, rule := range enabled {
		if rule.Directory != nil {
			directories[rule.Code] = rule.Directory(files)
		}
	}
	result := lintext.CheckResult{Complete: true}
	for index := range files {
		if err := ctx.Err(); err != nil {
			return lintext.CheckResult{}, err
		}
		file := files[index]
		for _, rule := range enabled {
			if violations, ok := directories[rule.Code]; ok {
				result.Findings = append(result.Findings, findingsOf(rule.Code, index, violationsAt(violations, index))...)
			} else {
				result.Findings = append(result.Findings, rule.findings(&file, index)...)
			}
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

// checkers counts the functions a rule sets among Statement, File and
// Directory.
func (r Rule) checkers() int {
	count := 0
	for _, set := range []bool{r.Statement != nil, r.File != nil, r.Directory != nil} {
		if set {
			count++
		}
	}
	return count
}

// violationsAt is the violations a Directory rule reported for the file at
// index, none where its answer is shorter.
func violationsAt(violations [][]lintext.Violation, index int) []lintext.Violation {
	if index < len(violations) {
		return violations[index]
	}
	return nil
}

// findingsOf is violations of the file at index, as findings of code.
func findingsOf(code string, index int, violations []lintext.Violation) []lintext.Finding {
	var findings []lintext.Finding
	for _, violation := range violations {
		findings = append(findings, lintext.Finding{Code: code, File: index, Statement: violation.Statement,
			Message: violation.Message, Subjects: slices.Clone(violation.Subjects)})
	}
	return findings
}

// findings is one rule over one file, the file at index of its request.
func (r Rule) findings(file *File, index int) []lintext.Finding {
	if r.File != nil {
		return findingsOf(r.Code, index, r.File(file))
	}
	var findings []lintext.Finding
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
