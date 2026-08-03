package atlasschema

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/risk"
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
	// Disabled lists additional rule codes or prefixes to skip.
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
func LintPlan(statements []string, opts PlanLintOptions) ([]lint.Finding, error) {
	enabled := lintPlanEnabledCodes(opts.RuleConfigs)
	if len(enabled) == 0 {
		return nil, nil
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

	findings, err := lint.LintFS(os.DirFS(dir), lint.Options{
		Dialect:     opts.Dialect,
		Disabled:    append(lintPlanDisabledCodes(enabled), opts.Disabled...),
		RuleConfigs: opts.RuleConfigs,
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

// lintPlanEnabledCodes returns the non-empty rule code selectors the policy
// declares.
func lintPlanEnabledCodes(configs map[string]lint.RuleConfig) []string {
	enabled := make([]string, 0, len(configs))
	for code := range configs {
		if strings.TrimSpace(code) != "" {
			enabled = append(enabled, code)
		}
	}
	return enabled
}

// lintPlanDisabledCodes lists every registered rule that no enabled selector
// covers, so the pass runs exactly the policy's rules.
func lintPlanDisabledCodes(enabled []string) []string {
	var disabled []string
	for _, rule := range lint.Rules() {
		if !lintPlanRuleEnabled(rule.Code, enabled) {
			disabled = append(disabled, rule.Code)
		}
	}
	return disabled
}

func lintPlanRuleEnabled(code string, enabled []string) bool {
	for _, selector := range enabled {
		if strings.HasPrefix(code, selector) {
			return true
		}
	}
	return false
}

// FormatPlanLintFindings renders blocking findings for a refusal message.
func FormatPlanLintFindings(findings []lint.Finding) string {
	var out strings.Builder
	for _, finding := range findings {
		out.WriteString("\n  - ")
		out.WriteString(finding.Rule)
		out.WriteString(" ")
		out.WriteString(string(finding.Severity))
		out.WriteString(": ")
		out.WriteString(finding.Message)
	}
	return out.String()
}
