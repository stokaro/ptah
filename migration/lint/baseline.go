package lint

import (
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintscan"
)

// baselineVersions returns the versions whose starting schema state a rule
// asked for, sorted ascending.
//
// The list is assembled from the rules rather than from a check written here.
// "The versions carrying a column rename" is true only while exactly one rule
// needs the state, and goes quietly wrong for the second: its files are never
// read, its findings can never fire, and nothing says so (stokaro/ptah#1632). A
// rule declares [InputBaselineSchema] and names the files it wants through
// [Rule.BaselineSubject], and both this list and the rule's own check go
// through that one predicate.
//
// A file no enabled rule asks about is not listed, so a directory with nothing
// to resolve costs no round trip.
func baselineVersions(files []File, opts Options, rules []Rule) []int64 {
	var versions []int64
	for i := range files {
		file := &files[i]
		if !baselineRequested(file, opts, rules) {
			continue
		}
		versions = append(versions, file.Version)
	}
	slices.Sort(versions)
	return slices.Compact(versions)
}

// baselineRequested reports whether any enabled rule wants this file's starting
// schema state.
func baselineRequested(file *File, opts Options, rules []Rule) bool {
	if !file.Selected || !file.IsUp || file.Version == 0 {
		return false
	}
	for _, rule := range rules {
		if ruleWantsBaseline(rule, file, opts) {
			return true
		}
	}
	return false
}

// ruleWantsBaseline reports whether rule asks for this file's starting schema
// state, for a statement the run actually reviews.
//
// The reviewed-schema filter is applied here rather than by each rule: a rename
// in a schema outside the scope produces no finding, so reading the database
// for it would be a round trip spent to learn nothing and reporting it as
// unresolved would be a warning about work the operator excluded on purpose.
func ruleWantsBaseline(rule Rule, file *File, opts Options) bool {
	if !rule.Input.readsBaseline() || rule.BaselineSubjects == nil {
		return false
	}
	if !ruleRunsOnFile(rule, file, opts) {
		return false
	}
	for _, index := range rule.BaselineSubjects(file) {
		if !file.scopeExcluded[index] {
			return true
		}
	}
	return false
}

// unmetInputs returns every rule that asked for a file's starting schema state
// on a run that supplied none.
//
// This is the reason the input is declared at all. A rule that needs the
// replayed schema and does not get it does not fail -- it resolves nothing and
// reports less, while the run exits 0, which the issue that asked for this
// called the hardest kind of gap to notice from CI. Saying so costs one line of
// output and turns silence into a fact a reader can act on.
func unmetInputs(files []File, opts Options, rules []Rule) []UnmetInput {
	var unmet []UnmetInput
	for i := range files {
		file := &files[i]
		if !file.Selected || file.Version == 0 {
			continue
		}
		for _, rule := range rules {
			if !ruleInputUnmet(rule, file, opts) {
				continue
			}
			unmet = append(unmet, UnmetInput{
				Rule:    rule.Code,
				Input:   rule.Input,
				File:    file.Path,
				Version: file.Version,
			})
		}
	}
	return unmet
}

// ruleInputUnmet reports whether a rule asked this file for an input the run
// did not supply.
func ruleInputUnmet(rule Rule, file *File, opts Options) bool {
	switch rule.Input {
	case InputBaselineSchema, InputBaselineRefinement:
		return file.IsUp && file.baseline.Empty() && ruleWantsBaseline(rule, file, opts)
	case InputRoutineBody:
		return ruleWantsRoutineBody(rule, file, opts)
	case InputStatementText:
		return false
	default:
		return false
	}
}

// ruleWantsRoutineBody reports whether a rule that reads a routine body was
// handed a file whose routine bodies could not be parsed.
//
// Parsing a body needs a dialect. Without one the body is not parsed, the rule
// finds nothing, and the run exits 0 -- the exact failure RuleInput exists to
// prevent, which is why this is a third input rather than a detail of the
// second (stokaro/ptah#2357).
//
// A file that defines no routine is not a gap: the rule had nothing to ask for.
func ruleWantsRoutineBody(rule Rule, file *File, opts Options) bool {
	if strings.TrimSpace(opts.Dialect) != "" {
		return false
	}
	if !ruleRunsOnFile(rule, file, opts) {
		return false
	}
	for i := range file.Statements {
		if lintscan.MayDefineRoutine(file.Statements[i].SQL) {
			return true
		}
	}
	return false
}

// UnmetInput reports one rule that asked for an analyzer input the run did not
// supply, on one file.
type UnmetInput struct {
	// Rule is the code of the rule that asked.
	Rule string
	// Input is what it asked for.
	Input RuleInput
	// File is the migration file whose analysis was thinner for the absence.
	File string
	// Version is that file's migration version.
	Version int64
}

// baselineDependents is one version's readers, indexed by the column they read.
//
// The zero value resolves nothing, which is what every run without a dev
// database gets and what keeps a drop reported exactly as it was before this
// existed.
type baselineDependents struct {
	byRef map[string][]lintext.BaselineDependent
	// schemaless carries the same fact, and licenses the same relaxation, that
	// it does on [lintkit.State].
	schemaless bool
}

// newBaselineDependentIndex groups dependents by the version whose starting
// state they describe.
func newBaselineDependentIndex(dependents []lintext.BaselineDependent) map[int64]baselineDependents {
	if len(dependents) == 0 {
		return nil
	}
	index := make(map[int64]baselineDependents)
	for _, dependent := range dependents {
		state, ok := index[dependent.Version]
		if !ok {
			state = baselineDependents{byRef: make(map[string][]lintext.BaselineDependent), schemaless: true}
		}
		if strings.TrimSpace(dependent.Schema) != "" {
			state.schemaless = false
		}
		for _, key := range baselineDependentKeys(dependent) {
			state.byRef[key] = append(state.byRef[key], dependent)
		}
		index[dependent.Version] = state
	}
	return index
}

// baselineDependentKeys are the spellings a migration might use for the column,
// in the forms [lintkit.baselineKeys] builds for the same reason.
func baselineDependentKeys(dependent lintext.BaselineDependent) []string {
	name := lintkit.NormalizeIdent(dependent.Column)
	table := lintkit.NormalizeIdent(dependent.Table)
	if dependent.Schema == "" {
		return []string{table + "\x00" + name}
	}
	return []string{
		table + "\x00" + name,
		lintkit.NormalizeIdent(dependent.Schema) + "." + table + "\x00" + name,
	}
}

// readers returns everything that reads one column of one table.
//
// Unlike [lintkit.State.Column] an ambiguous reference is not fatal here: two
// tables of that name in different schemas both read by something means the
// answer "something reads a column of this name" is still true, and the finding
// names each reader with the table it read. Reporting nothing would be the
// silence this rule exists to remove.
func (b baselineDependents) readers(tableRef, columnName string) []lintext.BaselineDependent {
	if len(b.byRef) == 0 || tableRef == "" || columnName == "" {
		return nil
	}
	if found := b.byRef[tableRef+"\x00"+columnName]; len(found) > 0 {
		return found
	}
	if !b.schemaless {
		return nil
	}
	dot := strings.LastIndex(tableRef, ".")
	if dot < 0 {
		return nil
	}
	return b.byRef[tableRef[dot+1:]+"\x00"+columnName]
}
