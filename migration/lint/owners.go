package lint

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/lintext"
)

// ownerAnswer is what the engine owners reported for one run: each owned
// rule's findings and baseline subjects, by file name and rule name.
type ownerAnswer struct {
	findings map[string]map[string][]Finding
	subjects map[string]map[string][]int
}

// checkOwnedRules asks the owners for their findings over the whole run, in
// one call of the runtime, and returns them in the forms this package's rules
// report.
//
// The call carries every owned rule the run enables, judged the way
// [ruleRunsOnFile] judges it for the run as a whole: a rule the online gate,
// the disabled list or the dialect leaves out is not sent. What depends on one
// file -- an excluded path, a file-wide suppression -- is decided over the
// answer, by the same [ruleRunsOnFile] every other rule passes, so an owned
// rule is selected exactly as a rule of this package is.
//
// A run that enables no owned rule, or selects no file, calls no owner.
func checkOwnedRules(
	ctx context.Context,
	runtime lintext.Runtime,
	files []File,
	rules []Rule,
	opts Options,
	target Target,
) (ownerAnswer, error) {
	// A name several owners publish is asked for once, and its findings are
	// titled by the first of its rules that applies to the run's dialect.
	var names []string
	titles := make(map[string]string)
	for _, rule := range rules {
		if rule.owned && onlineRuleSelected(rule.Name, opts) &&
			!ruleDisabled(rule.Name, opts.Disabled) && ruleAppliesToDialect(rule, opts.Dialect) {
			if _, seen := titles[rule.Name]; !seen {
				names = append(names, rule.Name)
				titles[rule.Name] = rule.Title
			}
		}
	}
	if len(names) == 0 || !slices.ContainsFunc(files, func(file File) bool { return file.Selected }) {
		return ownerAnswer{}, nil
	}
	request := lintext.CheckRequest{
		Target:           lintext.Target{Name: opts.Dialect, Version: target.Version, Capabilities: target.Capabilities},
		Names:            names,
		Files:            checkFiles(files),
		Compatibility:    string(opts.Compatibility),
		TransactionOwner: opts.TransactionOwner,
		Baseline:         requestBaseline(opts),
	}
	result, err := runtime.CheckMigrations(ctx, request)
	if err != nil {
		return ownerAnswer{}, fmt.Errorf("check the lint rules the engine owners publish: %w", err)
	}
	answer := ownerAnswer{findings: make(map[string]map[string][]Finding), subjects: make(map[string]map[string][]int)}
	for _, found := range result.Findings {
		file := &files[found.File]
		finding := Finding{Rule: found.Name, Title: titles[found.Name], File: file.Path, Message: found.Message}
		if found.Statement >= 0 {
			finding.Line = file.Statements[found.Statement].Line
			finding.Context = statementFindingContext(found.Statement, reportSubjects(found.Subjects)...)
		}
		if answer.findings[file.Name] == nil {
			answer.findings[file.Name] = make(map[string][]Finding)
		}
		answer.findings[file.Name][found.Name] = append(answer.findings[file.Name][found.Name], finding)
	}
	for _, subject := range result.BaselineSubjects {
		name := files[subject.File].Name
		if answer.subjects[name] == nil {
			answer.subjects[name] = make(map[string][]int)
		}
		answer.subjects[name][subject.Name] = append(answer.subjects[name][subject.Name], subject.Statements...)
	}
	return answer, nil
}

// bind returns rules with each owned rule reading its share of the answer, so
// the rest of the analysis applies one policy to every rule: suppression,
// severity, excluded paths, subsumption, the reviewed-schema filter, and the
// baseline versions and unmet inputs a rule's subjects decide.
//
// Owners may publish one name for different engines, so a name can head
// several rules. The answer is by name, so exactly one of them reads it: the
// first that applies to the run's dialect. A run that names a dialect has one
// such rule per name; a run that names none reads every owner's answer through
// the first.
func (a ownerAnswer) bind(rules []Rule, dialect string) []Rule {
	bound := slices.Clone(rules)
	claimed := make(map[string]bool)
	for i := range bound {
		if !bound[i].owned {
			continue
		}
		name := bound[i].Name
		reads := !claimed[name] && ruleAppliesToDialect(bound[i], dialect)
		claimed[name] = claimed[name] || reads
		bound[i].CheckFile = func(file *File) []Finding {
			if !reads {
				return nil
			}
			return slices.Clone(a.findings[file.Name][name])
		}
		if bound[i].Input.readsBaseline() {
			bound[i].BaselineSubjects = func(file *File) []int {
				if !reads {
					return nil
				}
				return slices.Clone(a.subjects[file.Name][name])
			}
		}
	}
	return bound
}

// checkFiles is every file of the run as the owners read it, in run order,
// with the statements the run split. A statement's line counts the lines of
// the file as written, the line every finding reports.
func checkFiles(files []File) []lintext.File {
	checked := make([]lintext.File, 0, len(files))
	for i := range files {
		file := &files[i]
		checked = append(checked, lintext.File{
			Name:          file.Name,
			Version:       file.Version,
			Up:            file.IsUp,
			Down:          file.IsDown,
			HasPair:       file.HasPair,
			NoTransaction: file.NoTransaction,
			Repeatable:    file.Repeatable,
			Statements:    slices.Clone(file.contract),
		})
	}
	return checked
}

// requestBaseline is the schema state the run was handed, nil when it was
// handed none: the first of the two calls a baseline rule is served in.
func requestBaseline(opts Options) *lintext.Baseline {
	if len(opts.Baseline) == 0 && len(opts.BaselineIndexes) == 0 && len(opts.BaselineHypertables) == 0 &&
		len(opts.BaselineDependents) == 0 && len(opts.BaselineObjects) == 0 && len(opts.BaselineRead) == 0 {
		return nil
	}
	var reads []lintext.BaselineRead
	for _, read := range opts.BaselineRead {
		reads = append(reads, lintext.BaselineRead{Version: read.Version, Listed: slices.Clone(read.Listed)})
	}
	var objects []lintext.BaselineObject
	for _, object := range opts.BaselineObjects {
		object.Values = slices.Clone(object.Values)
		objects = append(objects, object)
	}
	return &lintext.Baseline{
		Columns:     slices.Clone(opts.Baseline),
		Indexes:     slices.Clone(opts.BaselineIndexes),
		Hypertables: slices.Clone(opts.BaselineHypertables),
		Dependents:  slices.Clone(opts.BaselineDependents),
		Objects:     objects,
		Reads:       reads,
	}
}

// reportSubjects are subjects in the contract's form as a report carries
// them.
func reportSubjects(subjects []lintext.Subject) []Subject {
	reported := make([]Subject, 0, len(subjects))
	for _, subject := range subjects {
		reported = append(reported, Subject{Kind: SubjectKind(subject.Kind), Name: subject.Name, Parent: subject.Parent,
			DataType: subject.DataType})
	}
	return reported
}
