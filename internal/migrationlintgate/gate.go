// Package migrationlintgate applies migration lint policy to pending database
// migrations before execution.
package migrationlintgate

import (
	"fmt"
	"io/fs"
	"strings"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/risk"
)

// Policy is a validated apply-time lint policy resolved for a live database.
type Policy struct {
	dialect  string
	disabled []string
	rules    map[string]lint.RuleConfig
}

// LoadPolicy loads the conventional lint policy and resolves it against the
// connected database dialect.
func LoadPolicy(fsys fs.FS, databaseDialect string) (Policy, error) {
	cfg, err := lint.LoadConfigFS(fsys, lint.ConfigFileName)
	if err != nil {
		return Policy{}, err
	}
	if cfg.Dialect != "" {
		if cfg.Dialect != databaseDialect {
			return Policy{}, fmt.Errorf(
				"lint dialect %q does not match database dialect %q",
				cfg.Dialect,
				databaseDialect,
			)
		}
		databaseDialect = cfg.Dialect
	}
	policy := Policy{
		dialect:  databaseDialect,
		disabled: append([]string{"MF", "BC", "PG", "MY"}, cfg.DisabledRules...),
		rules:    cfg.Rules,
	}
	if err := lint.ValidateOptions(policy.options("")); err != nil {
		return Policy{}, err
	}
	return policy, nil
}

// Analyze loads policy and returns blocking data-safety findings for the
// selected pending migration versions.
func Analyze(
	fsys fs.FS,
	pending []int64,
	databaseDialect string,
	pathPrefix string,
) ([]lint.Finding, error) {
	policy, err := LoadPolicy(fsys, databaseDialect)
	if err != nil {
		return nil, err
	}
	return AnalyzeWithPolicy(fsys, pending, policy, pathPrefix)
}

// AnalyzeWithPolicy returns blocking data-safety findings using a policy that
// was loaded before migration planning.
func AnalyzeWithPolicy(
	fsys fs.FS,
	pending []int64,
	policy Policy,
	pathPrefix string,
) ([]lint.Finding, error) {
	options := policy.options(pathPrefix)
	options.Selection = lint.VersionSelection{
		Versions:   pending,
		Restricted: true,
	}
	findings, err := lint.LintFS(fsys, options)
	if err != nil {
		return nil, err
	}
	blocking := make([]lint.Finding, 0, len(findings))
	for _, finding := range findings {
		if strings.HasPrefix(finding.Rule, "DS") && risk.IsBlocking(finding.Severity) {
			blocking = append(blocking, finding)
		}
	}
	return blocking, nil
}

func (p Policy) options(pathPrefix string) lint.Options {
	return lint.Options{
		Dialect:     p.dialect,
		Disabled:    p.disabled,
		PathPrefix:  pathPrefix,
		RuleConfigs: p.rules,
	}
}
