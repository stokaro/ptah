// Package migrationlintgate applies migration lint policy to pending database
// migrations before execution.
package migrationlintgate

import (
	"fmt"
	"io/fs"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/lintdialect"
	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/risk"
)

// ReportedFamily is the only identifier family this gate ever blocks on.
//
// The apply gate is a data-safety stop, not the lint pass. A finding outside
// this family is dropped even when the rule that produced it ran, so an
// operator reading the rule tables must not assume that reaching an apply
// means every rule was consulted. The reference page's generated section
// renders this constant, and a test pins the one hand-written cell that names
// it, so narrowing or widening the gate fails the documentation gate rather
// than leaving a stale promise on the page.
const ReportedFamily = "DS"

// disabledFamilies are the families the gate turns off before analysis.
//
// They are the advisory ones: migration-file form, backward compatibility, and
// the two engine-specific locking families. Running them here would refuse an
// apply over findings that are not about the data already in the tables.
var disabledFamilies = []string{"MF", "BC", "PG", "MY"}

// DisabledFamilies returns the identifier families this gate disables, in the
// order it disables them. The slice is a copy: a caller that appended to a
// shared one would grow the gate's own policy.
func DisabledFamilies() []string {
	return slices.Clone(disabledFamilies)
}

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
	// The policy dialect is an assertion about the directory, not a selector.
	// What gets linted is decided by databaseDialect below, which the wire
	// reports and the operator cannot mistype; a policy naming the same engine
	// by another spelling, or another member of the same family, changes
	// nothing about the analysis. So the comparison is lintdialect's, shared
	// with the standalone lint command's --dev-url check so the two commands
	// accept exactly the same policy files (stokaro/ptah#270).
	if !lintdialect.Compatible(cfg.Dialect, databaseDialect) {
		return Policy{}, fmt.Errorf(
			"lint dialect %q does not match database dialect %q",
			cfg.Dialect,
			databaseDialect,
		)
	}
	// Store the canonical spelling, never the caller's: lint matches
	// Rule.Dialects and picks its lexer mode by exact string comparison and
	// validates neither, so an alias here would run clean while selecting the
	// wrong scanner. A dialect this package cannot resolve is passed through
	// rather than blanked, because blanking would turn every rule back on.
	if canonical, ok := lintdialect.Canonical(databaseDialect); ok {
		databaseDialect = canonical
	}
	policy := Policy{
		dialect:  databaseDialect,
		disabled: append(DisabledFamilies(), cfg.DisabledRules...),
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
	databaseDialect,
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
		if strings.HasPrefix(finding.Rule, ReportedFamily) && risk.IsBlocking(finding.Severity) {
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
