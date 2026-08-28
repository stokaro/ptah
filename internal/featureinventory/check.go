package featureinventory

import (
	"bytes"
	"fmt"
	"sort"

	"go.5x5.cz/ptah/internal/agentsurface"
)

// The rules this package refuses on. A rule is a code rather than a sentence
// because SelfTest asserts which rule fired, by string equality: a self-test
// that only counted refusals would stay green on a fixture that went red for
// the wrong reason, and one that matched message text would be the substring
// comparison this whole design exists to avoid.
const (
	// RuleEmptyKind fires when a row kind derived nothing. A gate that
	// discovers zero inputs reports the same success as one that checked
	// everything.
	RuleEmptyKind = "empty-kind"
	// RuleIdentifierCollision fires when two surfaces derive one identifier.
	RuleIdentifierCollision = "identifier-collision"
	// RuleUnknownClaim fires when a page's `owns:` names no derived feature.
	RuleUnknownClaim = "unknown-claim"
	// RuleDuplicateClaim fires when two pages claim one feature.
	RuleDuplicateClaim = "duplicate-claim"
	// RuleOwnedBelowFloor fires when documentation coverage fell below the
	// ratchet the artifact carries.
	RuleOwnedBelowFloor = "owned-below-floor"
	// RuleNoExamples fires when no page opts in to internal/quickstart.
	RuleNoExamples = "no-examples"
	// RuleStaleArtifact fires when the committed file is not the regeneration.
	RuleStaleArtifact = "stale-artifact"
)

// Problem is one refusal: which rule, and what a reader has to fix.
type Problem struct {
	Rule    string
	Message string
}

func (p Problem) String() string { return p.Rule + ": " + p.Message }

func sortProblems(problems []Problem) {
	sort.Slice(problems, func(i, j int) bool {
		if problems[i].Rule != problems[j].Rule {
			return problems[i].Rule < problems[j].Rule
		}
		return problems[i].Message < problems[j].Message
	})
}

// Compare adds the artifact rule to a derivation's own: the committed bytes are
// the regeneration, or they are stale.
//
// One byte comparison covers every derived field at once, which is why there is
// one gate here and not five. There is no authored column left to police
// separately.
func Compare(committed, regenerated []byte, problems []Problem) []Problem {
	if !bytes.Equal(committed, regenerated) {
		problems = append(problems, Problem{RuleStaleArtifact, fmt.Sprintf(
			"the committed inventory is not what the declarations produce; run %s", GeneratorCommand)})
	}
	sortProblems(problems)
	return problems
}

// RulesOf lists the rules a set of problems fired, deduplicated and sorted.
func RulesOf(problems []Problem) []string {
	seen := make(map[string]bool, len(problems))
	var rules []string
	for _, problem := range problems {
		if seen[problem.Rule] {
			continue
		}
		seen[problem.Rule] = true
		rules = append(rules, problem.Rule)
	}
	sort.Strings(rules)
	return rules
}

// cleanSources is the fixture every self-test case mutates: four kinds each
// deriving rows, one page claiming one of them, one executed page, no floor.
//
// The claimed row is a PACKAGE, not a verb. A fixture whose page claimed the
// verb would report unknown-claim as well as empty-kind the moment the verb
// case removed the leaves, and a case that fires two rules cannot say which of
// them the code still reads.
func cleanSources() Sources {
	return Sources{
		NativeLeaves: []agentsurface.Leaf{{Name: "schema apply"}, {Name: "db read"}},
		Ledger: []byte("## Stable Embedder API\n\n" +
			"- `" + ModulePath + "/core/renderer`\n" +
			"- `" + ModulePath + "/dbschema`\n"),
		Release:  []byte("builds:\n  - id: ptah\n    binary: ptah\n"),
		Pages:    []PageClaim{{Path: "docs/site/src/content/docs/a.md", Owns: []string{"gopkg-core-renderer"}}},
		Examples: []Example{{Page: "docs/site/src/content/docs/q.mdx"}},
	}
}

// selfTestCase is one broken fixture and the rule it must fire.
type selfTestCase struct {
	name string
	rule string
	// mutate breaks exactly one rule of the clean fixture.
	mutate func(src *Sources)
}

// selfTestCases is the fixture per rule. Every rule Derive can report has one,
// and the coverage is asserted rather than assumed: SelfTest compares the rules
// exercised here with the rules declared above.
func selfTestCases() []selfTestCase {
	return []selfTestCase{
		{name: "a kind derives nothing", rule: RuleEmptyKind, mutate: func(src *Sources) {
			src.NativeLeaves = nil
		}},
		{name: "two surfaces derive one identifier", rule: RuleIdentifierCollision, mutate: func(src *Sources) {
			src.NativeLeaves = append(src.NativeLeaves, agentsurface.Leaf{Name: "schema-apply"})
		}},
		{name: "a page claims a feature nothing derives", rule: RuleUnknownClaim, mutate: func(src *Sources) {
			src.Pages = append(src.Pages, PageClaim{Path: "b.md", Owns: []string{"cli-ptah-schema-retired"}})
		}},
		{name: "two pages claim one feature", rule: RuleDuplicateClaim, mutate: func(src *Sources) {
			src.Pages = append(src.Pages, PageClaim{Path: "b.md", Owns: []string{"gopkg-core-renderer"}})
		}},
		{name: "coverage fell below the ratchet", rule: RuleOwnedBelowFloor, mutate: func(src *Sources) {
			src.OwnedFloor = len(src.Pages) + 1
		}},
		{name: "no page opts in to the runner", rule: RuleNoExamples, mutate: func(src *Sources) {
			src.Examples = nil
		}},
	}
}

// SelfTest breaks each rule in turn and requires the derivation to notice,
// returning the cases that did not. An empty result is a pass.
//
// The control comes first and is not optional: a Derive that refused
// everything would satisfy every case below while gating nothing, and a
// self-test reduced to a bare OK still exits zero.
func SelfTest() []string {
	var failures []string

	if _, problems := Derive(cleanSources()); len(problems) != 0 {
		failures = append(failures, fmt.Sprintf(
			"the unbroken fixture reported %v; a gate that refuses everything gates nothing", RulesOf(problems)))
	}

	doc, _ := Derive(cleanSources())
	rendered, err := Render(doc)
	if err != nil {
		failures = append(failures, "rendering the unbroken fixture: "+err.Error())
	}
	if rules := RulesOf(Compare(rendered, rendered, nil)); len(rules) != 0 {
		failures = append(failures, fmt.Sprintf("an artifact equal to its regeneration reported %v", rules))
	}
	if rules := RulesOf(Compare([]byte("{}\n"), rendered, nil)); len(rules) != 1 || rules[0] != RuleStaleArtifact {
		failures = append(failures, fmt.Sprintf(
			"a stale artifact reported %v, expected [%s]", rules, RuleStaleArtifact))
	}

	covered := map[string]bool{RuleStaleArtifact: true}
	for _, test := range selfTestCases() {
		covered[test.rule] = true
		source := cleanSources()
		test.mutate(&source)
		_, problems := Derive(source)
		rules := RulesOf(problems)
		if len(rules) != 1 || rules[0] != test.rule {
			failures = append(failures, fmt.Sprintf(
				"%s reported %v, expected exactly [%s]", test.name, rules, test.rule))
		}
	}

	for _, rule := range declaredRules() {
		if !covered[rule] {
			failures = append(failures, "no self-test fixture breaks "+rule)
		}
	}
	return failures
}

// declaredRules is every rule this package can report. A rule added without a
// fixture fails SelfTest rather than shipping unmeasured.
func declaredRules() []string {
	return []string{
		RuleEmptyKind, RuleIdentifierCollision, RuleUnknownClaim,
		RuleDuplicateClaim, RuleOwnedBelowFloor, RuleNoExamples, RuleStaleArtifact,
	}
}
