package atlasschema

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"ptah.run/internal/atlascodes"
	"ptah.run/migration/lint"
	"ptah.run/migration/risk"
)

// planLintFileName is the name the planned statements are linted under. The
// lint engine reads migration files, so the plan is presented as one, and the
// name is what findings point at.
const planLintFileName = "0001_schema_apply.up.sql"

// PlanLintOptions configures a lint pass over a planned schema apply.
type PlanLintOptions struct {
	// Dialect gates dialect-specific rules and selects the lexer.
	Dialect string
	// RuleConfigs carries the per-rule severities the project's lint policy
	// declares. It also decides which rules run at all: see [LintPlan].
	RuleConfigs map[string]lint.RuleConfig
	// Disabled lists additional rules to skip, as the compatibility surface
	// spells them: Ptah rule names and categories, Atlas check codes or
	// analyzer names.
	Disabled []string
}

// LintPlan runs the lint rules the project's policy configures over the
// statements a schema apply would execute, and returns the blocking findings.
//
// Only rules the policy names run. The engine ships rules for concerns a
// declarative apply cannot answer -- a plan is not a migration directory, so it
// has no paired down file and no history to be non-linear against -- and
// enabling the whole registry would refuse applies over findings the operator
// never asked about. Naming the policy's own codes keeps the pass equal to what
// the atlas.hcl `lint` block declares and nothing else, which is also why an
// empty policy is not a lint pass at all: the caller skips it entirely.
//
// The statements are linted as a migration file in a scratch directory because
// the engine reads a filesystem. Nothing is left behind.
func LintPlan(ctx context.Context, runtime lint.Runtime, statements []string, opts PlanLintOptions) ([]lint.Finding, error) {
	configs := atlascodes.RuleConfigKeys(opts.RuleConfigs)
	enabled := lintPlanEnabledSelectors(configs)
	if len(enabled) == 0 {
		return nil, nil
	}
	rules, err := lint.Rules(runtime)
	if err != nil {
		return nil, fmt.Errorf("lint schema apply plan: %w", err)
	}
	dir, err := os.MkdirTemp("", "ptah-apply-lint-")
	if err != nil {
		return nil, fmt.Errorf("stage schema apply lint input: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, planLintFileName)
	if err := os.WriteFile(path, []byte(FormatMigrationSQL(statements)), 0o600); err != nil {
		return nil, fmt.Errorf("stage schema apply lint input: %w", err)
	}

	findings, err := lint.LintFS(ctx, runtime, os.DirFS(dir), lint.Options{
		Dialect:     opts.Dialect,
		Disabled:    append(lintPlanDisabledNames(rules, enabled), atlascodes.SelectorList(opts.Disabled)...),
		RuleConfigs: configs,
	})
	if err != nil {
		return nil, fmt.Errorf("lint schema apply plan: %w", err)
	}
	blocking := make([]lint.Finding, 0, len(findings))
	for _, finding := range findings {
		if risk.IsBlocking(finding.Severity) {
			blocking = append(blocking, finding)
		}
	}
	return blocking, nil
}

// lintPlanEnabledSelectors returns the non-empty rule selectors the policy
// declares, as the lint core reads them: a policy entry spelled the Atlas way
// has already become the rule names it covers, so a rated entry is never
// left in the disabled set below and an error-rated policy never silently
// fails to run (stokaro/ptah#1631).
func lintPlanEnabledSelectors(configs map[string]lint.RuleConfig) []string {
	enabled := make([]string, 0, len(configs))
	for selector := range configs {
		if strings.TrimSpace(selector) != "" {
			enabled = append(enabled, selector)
		}
	}
	return enabled
}

// lintPlanDisabledNames lists every rule that no enabled selector covers, so
// the pass runs exactly the policy's rules.
func lintPlanDisabledNames(rules []lint.Rule, enabled []string) []string {
	var disabled []string
	for _, rule := range rules {
		covered := slices.ContainsFunc(enabled, func(selector string) bool { return lint.SelectorSelects(selector, rule.Name) })
		if !covered && !slices.Contains(disabled, rule.Name) {
			disabled = append(disabled, rule.Name)
		}
	}
	return disabled
}

// FormatPlanLintFindings renders blocking findings for a refusal message. A
// finding is named the way every other ptah-compat lint report names it: by
// the Atlas check code its rule reports on dialect, and by its Ptah rule name
// when Atlas has no check for it.
func FormatPlanLintFindings(findings []lint.Finding, dialect string) string {
	var out strings.Builder
	for _, finding := range findings {
		out.WriteString("\n  - ")
		out.WriteString(atlascodes.RuleFor(finding.Rule, dialect).Code)
		out.WriteString(" ")
		out.WriteString(string(finding.Severity))
		out.WriteString(": ")
		out.WriteString(finding.Message)
	}
	return out.String()
}
