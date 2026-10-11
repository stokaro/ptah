package lintext

import (
	"context"
	"fmt"
	"slices"
)

// Violation is one violation an in-process check reports.
type Violation struct {
	// Statement is the position of the statement in its file, or -1 for a
	// violation of the file as a whole.
	Statement int
	// Message says what was found.
	Message string
}

// Check is one rule an in-process owner implements with Go functions.
// Exactly one of Statement and File is set.
type Check struct {
	// Rule is what the owner publishes.
	Rule Rule
	// Statement reports whether statement violates the rule, and the
	// message. It judges every statement of an up file, and of a down file
	// where the rule applies to down.
	Statement func(target Target, file *File, statement *Statement) (bool, string)
	// File reports the violations of files[index]. It may read the files
	// before it.
	File func(target Target, files []File, index int) []Violation
}

// Checks is an in-process [Service] over Go checks: what a built-in owner
// registers instead of answering requests by hand. Its zero value checks
// nothing.
type Checks []Check

// Validate reports a check whose rule does not validate, one that sets
// neither or both functions, and two checks of one code. Each failure wraps
// [ErrInvalid].
func (c Checks) Validate() error {
	codes := make([]string, 0, len(c))
	for _, check := range c {
		if err := check.Rule.Validate(); err != nil {
			return err
		}
		if (check.Statement == nil) == (check.File == nil) {
			return fmt.Errorf("%w: check %s must set exactly one of Statement and File", ErrInvalid, check.Rule.Code)
		}
		if slices.Contains(codes, check.Rule.Code) {
			return fmt.Errorf("%w: two checks of rule %s", ErrInvalid, check.Rule.Code)
		}
		codes = append(codes, check.Rule.Code)
	}
	return nil
}

// Rules returns the rule of every check, in order.
func (c Checks) Rules() []Rule {
	rules := make([]Rule, 0, len(c))
	for _, check := range c {
		rules = append(rules, check.Rule.Clone())
	}
	return rules
}

// CheckMigrations runs the checks the request enables over its files and
// returns their findings, statement findings in file and statement order.
// A code no check carries wraps [ErrInvalid], and cancellation returns the
// context's error; either returns no result.
func (c Checks) CheckMigrations(ctx context.Context, request CheckRequest) (CheckResult, error) {
	if err := c.Validate(); err != nil {
		return CheckResult{}, err
	}
	enabled := make([]Check, 0, len(request.Codes))
	for _, code := range request.Codes {
		position := slices.IndexFunc(c, func(check Check) bool { return check.Rule.Code == code })
		if position < 0 {
			return CheckResult{}, fmt.Errorf("%w: no check of rule %s", ErrInvalid, code)
		}
		enabled = append(enabled, c[position])
	}
	result := CheckResult{Complete: true}
	for index := range request.Files {
		if err := ctx.Err(); err != nil {
			return CheckResult{}, err
		}
		for _, check := range enabled {
			result.Findings = append(result.Findings, check.run(request, index)...)
		}
	}
	return result, ctx.Err()
}

// run is one check over one file of request.
func (c Check) run(request CheckRequest, index int) []Finding {
	file := &request.Files[index]
	var findings []Finding
	if c.File != nil {
		for _, violation := range c.File(request.Target, request.Files, index) {
			findings = append(findings, Finding{Code: c.Rule.Code, File: index, Statement: violation.Statement, Message: violation.Message})
		}
		return findings
	}
	if !file.Up && (!file.Down || !c.Rule.AppliesToDown) {
		return nil
	}
	for position := range file.Statements {
		if found, message := c.Statement(request.Target, file, &file.Statements[position]); found {
			findings = append(findings, Finding{Code: c.Rule.Code, File: index, Statement: position, Message: message})
		}
	}
	return findings
}
