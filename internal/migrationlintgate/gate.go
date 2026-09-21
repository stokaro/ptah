// Package migrationlintgate applies migration lint policy to pending database
// migrations before execution.
package migrationlintgate

import (
	"fmt"
	"io/fs"
	"slices"
	"strings"

	"ptah.run/internal/lintdialect"
	"ptah.run/migration/lint"
	"ptah.run/migration/risk"
)

// ReportedFamily is the identifier family this gate blocks on by itself.
//
// The apply gate is a data-safety stop, not the lint pass. A finding outside
// this family is dropped even when the rule that produced it ran, so an
// operator reading the rule tables must not assume that reaching an apply
// means every rule was consulted. The reference page's generated section
// renders this constant, and a test pins the one hand-written cell that names
// it, so narrowing or widening the gate fails the documentation gate rather
// than leaving a stale promise on the page.
//
// A policy widens the gate by family through its `gate` section
// ([lint.GateConfig]): a family named there runs even when it is one the
// gate disables by default, and its error-severity findings refuse the apply
// the way DS findings do. The widening is the policy's, committed beside the
// migrations, so the default stays a data-safety stop (stokaro/ptah#2942).
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
	dialect string
	// target is the server the analysis plans against, resolved from the
	// policy's server-version against the dialect the connection reported.
	target   lint.Target
	disabled []string
	rules    map[string]lint.RuleConfig
	// families are the identifier families whose blocking findings refuse
	// the apply: [ReportedFamily] and whatever the policy's gate section adds.
	families []string
}

// ServerVersionNote is what the policy's declared server version resolved to
// when it named no measured release line, and is empty otherwise.
//
// The gate plans against a capability ladder, and a version between measured
// lines lands on the nearest one below. An apply that proceeded without saying
// so would have gated on a release nobody named, which reads exactly like a
// gate that planned for the server in front of it.
func (p Policy) ServerVersionNote() string {
	return p.target.Note
}

// BlockingFamilies returns the families this policy refuses an apply on.
func (p Policy) BlockingFamilies() []string {
	return slices.Clone(p.families)
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
	families := cfg.GateFamilies()
	// A family the policy gates on must run, so it leaves the gate's own
	// disabled list; the policy's disabled-rules still apply to it.
	var disabled []string
	for _, family := range DisabledFamilies() {
		if !slices.Contains(families, family) {
			disabled = append(disabled, family)
		}
	}
	// The apply path is where the version a policy declares finally has a
	// dialect to be resolved against: the configuration may leave the dialect
	// out, and here the wire reported it. Resolving it is also what refuses a
	// value that names no server, which `migrations lint` refuses and this
	// would otherwise accept and ignore.
	//
	// Only where the linter has rules for that engine, though. Oracle is the
	// one it does not, and the gate still runs there: the DS family is
	// dialect-independent and protects an Oracle apply the same way it
	// protects every other. A policy that also declared a version on such a
	// connection is refused rather than ignored, because that declaration
	// cannot be honored.
	target, err := policyTarget(databaseDialect, cfg.ServerVersion)
	if err != nil {
		return Policy{}, err
	}
	policy := Policy{
		dialect:  databaseDialect,
		target:   target,
		disabled: append(disabled, cfg.DisabledRules...),
		rules:    cfg.Rules,
		families: families,
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
		if policy.gatesOn(finding.Rule) && risk.IsBlocking(finding.Severity) {
			blocking = append(blocking, finding)
		}
	}
	return blocking, nil
}

// gatesOn reports whether a rule belongs to a family the policy blocks on.
func (p Policy) gatesOn(rule string) bool {
	for _, family := range p.families {
		if strings.HasPrefix(rule, family) {
			return true
		}
	}
	return false
}

func (p Policy) options(pathPrefix string) lint.Options {
	return lint.Options{
		Dialect:     p.dialect,
		Target:      p.target,
		Disabled:    p.disabled,
		PathPrefix:  pathPrefix,
		RuleConfigs: p.rules,
	}
}

// policyTarget resolves the server the apply-time gate plans against, or
// leaves it unresolved on an engine the linter has no rules for.
func policyTarget(databaseDialect, serverVersion string) (lint.Target, error) {
	if _, ok := lintdialect.Canonical(databaseDialect); ok {
		return lint.ResolveTarget(databaseDialect, serverVersion)
	}
	if serverVersion != "" {
		return lint.Target{}, fmt.Errorf(
			"lint server-version %q cannot be resolved against database dialect %q: "+
				"lint has no rules for it, so there is no capability set to refine",
			serverVersion, databaseDialect,
		)
	}
	return lint.Target{}, nil
}
