package lint

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/internal/lintdialect"
	"ptah.run/internal/lintexpr"
)

// Declared rules let a project add a check without building a binary.
//
// A rule is an expression over one statement plus the severity and message to
// report when it holds. The expression language and its vocabulary live in
// internal/lintexpr; this file is the part that turns a declaration into a
// [Rule] the analyzer already knows how to run (stokaro/ptah#1706).
//
// The declaration is deliberately the SAME model for both configuration
// surfaces. `.ptah-lint.yaml` spells it as a mapping and `atlas.hcl` spells it
// as a block, and both produce a [RuleConfig] carrying Match -- so a rule
// behaves identically whichever file it was written in, and neither surface can
// grow a capability the other lacks.

// declaredRuleSeverity is what a declaration gets when it names none.
//
// Warning rather than error: a project's own rule is advisory until its author
// says otherwise, and a rule that failed the build the moment it was written
// would be discovered by breaking CI.
const declaredRuleSeverity = SeverityWarning

// compileDeclaredRules turns every declaring entry in configs into a rule.
//
// Built-in names are refused rather than overridden. Allowing a declaration to
// take a built-in's name would let a project replace a data-safety check with
// an expression that never fires, and the report would still name the built-in
// rule -- so the directory would read as checked by a rule that is not running.
func compileDeclaredRules(configs map[string]RuleConfig, builtin []Rule, dialect string) ([]Rule, error) {
	taken := make(map[string]struct{}, len(builtin))
	for _, rule := range builtin {
		taken[rule.Name] = struct{}{}
	}

	var declared []Rule
	for _, name := range slices.Sorted(maps.Keys(configs)) {
		config := configs[name]
		if !config.Declares() {
			continue
		}
		if _, exists := taken[name]; exists {
			return nil, fmt.Errorf(
				"lint rule %s is already defined; a declared rule needs a name of its own, "+
					"and configuring the existing one means an entry without `match`", name)
		}
		rule, err := compileDeclaredRule(name, config, dialect)
		if err != nil {
			return nil, err
		}
		taken[name] = struct{}{}
		declared = append(declared, rule)
	}
	return declared, nil
}

func compileDeclaredRule(name string, config RuleConfig, dialect string) (Rule, error) {
	if strings.TrimSpace(config.Message) == "" {
		return Rule{}, fmt.Errorf(
			"lint rule %s declares `match` but no `message`; a finding whose text is its own "+
				"rule name says what fired and not why it matters", name)
	}
	expression, err := lintexpr.Compile(name, config.Match)
	if err != nil {
		return Rule{}, err
	}
	dialects, err := canonicalDeclaredDialects(name, config.Dialects)
	if err != nil {
		return Rule{}, err
	}

	severity := config.Severity
	if severity == "" {
		severity = declaredRuleSeverity
	}
	title := strings.TrimSpace(config.Title)
	if title == "" {
		title = name
	}
	rule := Rule{
		Name:          name,
		Title:         title,
		Severity:      severity,
		Dialects:      dialects,
		AppliesToDown: config.AppliesToDown,
	}
	// A declared rule is a CheckFile rule even though it reasons about single
	// statements, because a CheckStatement closure receives ONLY the statement:
	// `file.path`, `file.is_up` and the target dialect would be unreachable,
	// and one Rule value serves every file, so they cannot be closed over
	// either. The loop below therefore reproduces what the analyzer's statement
	// path does -- direction gating and per-statement suppression -- rather
	// than skipping them, which is the difference between a declared rule that
	// honors `nolint` and one that ignores it.
	rule.CheckFile = func(file *File) []Finding {
		if !file.IsUp && !config.AppliesToDown {
			return nil
		}
		var findings []Finding
		for i := range file.Statements {
			statement := &file.Statements[i]
			if statementSuppressesRule(statement, name) {
				continue
			}
			hit, message := evaluateDeclared(expression, name, config.Message, file, statement, dialect)
			if !hit {
				continue
			}
			findings = append(findings, Finding{
				Rule:    name,
				Title:   title,
				File:    file.Path,
				Line:    statement.Line,
				Message: message,
			})
		}
		return findings
	}
	return rule, nil
}

// evaluateDeclared runs one statement through the rule expression.
//
// An expression that fails at evaluation is reported AS A FINDING rather than
// swallowed. There is no error channel out of a rule, and the two alternatives
// are worse: returning false makes a broken rule indistinguishable from a clean
// file, and aborting the run turns one malformed project rule into a lint that
// reports nothing at all.
func evaluateDeclared(
	expression *lintexpr.Expression,
	name, message string,
	file *File,
	statement *Statement,
	dialect string,
) (bool, string) {
	hit, err := expression.Evaluate(name, lintexpr.Scope{
		SQL:       statement.SQL,
		Canonical: statement.Canonical,
		Words:     statement.Words,
		Line:      statement.Line,
		Path:      file.Path,
		IsUp:      file.IsUp,
		IsDown:    file.IsDown,
		Dialect:   dialect,
	})
	if err != nil {
		return true, err.Error()
	}
	if !hit {
		return false, ""
	}
	return true, message
}

// canonicalDeclaredDialects normalizes the dialect list and refuses an unknown
// one, so a rule scoped to a misspelled dialect is reported rather than
// silently never running.
func canonicalDeclaredDialects(name string, dialects []string) ([]string, error) {
	if len(dialects) == 0 {
		return nil, nil
	}
	canonical := make([]string, 0, len(dialects))
	for _, dialect := range dialects {
		name, ok := lintdialect.Canonical(dialect)
		if !ok || name == "" {
			return nil, fmt.Errorf(
				"lint rule %s is scoped to unsupported dialect %q: expected %s",
				name, dialect, lintdialect.Expected)
		}
		canonical = append(canonical, name)
	}
	return canonical, nil
}
