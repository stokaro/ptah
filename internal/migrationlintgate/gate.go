// Package migrationlintgate applies migration lint policy to pending database
// migrations before execution.
package migrationlintgate

import (
	"context"
	"fmt"
	"io/fs"
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/internal/lintdialect"
	"ptah.run/internal/servertarget"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/risk"
)

// ReportedRules returns the rules this gate blocks on by itself, by name.
//
// The apply gate is a data-safety stop, not the lint pass. A finding outside
// these rules is dropped even when the rule that produced it ran, so an
// operator reading the rule tables must not assume that reaching an apply
// means every rule was consulted. The reference page's generated section
// renders this list, and a test pins the one hand-written cell that names
// it, so narrowing or widening the gate fails the documentation gate rather
// than leaving a stale promise on the page.
//
// A policy widens the gate through its `gate` section ([lint.GateConfig]):
// a rule name or category named there runs even when the gate disables it by
// default, and its error-severity findings refuse the apply the way these
// findings do. The widening is the policy's, committed beside the migrations,
// so the default stays a data-safety stop (stokaro/ptah#2942). The slice is a
// copy.
func ReportedRules() []string {
	return slices.Clone(lint.DefaultGateRules)
}

// disabledRules are the rules the gate turns off before analysis, by name.
//
// They are the advisory ones: migration-file form, backward compatibility, and
// the engine-specific locking and rewrite rules of PostgreSQL and MySQL
// (including MariaDB). Running them here would refuse an apply over findings
// that are not about the data already in the tables. They are listed by name
// because no category holds exactly them: several categories mix these rules
// with ones the gate leaves on.
var disabledRules = []string{
	"file-form/missing-down", "file-form/empty-migration", "file-form/refused-file", "file-form/file-name",
	"data-dependent/add-unique-index", "data-dependent/rebuild-index-as-unique",
	"rollout/rename", "rollout/drop-table", "rollout/drop-column",
	"locking/create-index", "transaction/add-enum-value", "transaction/concurrent-index", "transaction/per-chunk-index",
	"locking/add-primary-key", "locking/add-unique-constraint", "locking/drop-index", "locking/partitioned-index",
	"locking/add-exclusion-constraint", "design/column-alignment", "rewrite/alter-column-type",
	"rewrite/in-place-column-change", "rewrite/volatile-default", "locking/set-not-null",
	"locking/add-primary-key-on-nullable", "locking/add-check", "locking/add-foreign-key", "rewrite/set-persistence",
	"locking/create-trigger", "rewrite/add-stored-generated-column", "rewrite/add-identity-column",
	"rewrite/set-access-method", "locking/replace-primary-key", "security/definer-without-search-path",
	"rollout/weaken-replica-identity", "design/disable-autovacuum", "naming/identifier-too-long",
	"naming/identifier-collision",
	"locking/blocking-alter-table", "side-effect/inline-reference", "rewrite/remove-enum-value",
	"rewrite/reorder-enum-values", "rewrite/insert-enum-value", "rewrite/widen-enum", "rewrite/remove-set-member",
	"rewrite/reorder-set-members", "rewrite/insert-set-member", "rewrite/widen-set", "rewrite/add-primary-key",
	"rewrite/drop-primary-key", "locking/add-fulltext-index", "locking/add-spatial-index", "rewrite/convert-charset",
	"rewrite/replace-primary-key", "rewrite/change-engine", "rewrite/change-partitioning",
	"locking/add-auto-increment-column", "rewrite/modify-generated-column", "locking/enforce-check",
	"destructive/drop-system-versioning", "rewrite/set-not-null",
}

// DisabledRules returns the rule names this gate disables, in the order it
// disables them. The slice is a copy: a caller that appended to a shared one
// would grow the gate's own policy.
//
// A name several engines share disables every engine's rule of that name:
// `rewrite/set-not-null` also names the SQLite rule, which the gate leaves
// disabled too. It reports nothing the gate blocks on, so the stop is the
// same.
func DisabledRules() []string {
	return slices.Clone(disabledRules)
}

// Policy is a validated apply-time lint policy resolved for a live database.
type Policy struct {
	dialect string
	// target is the server the analysis plans against, resolved from the
	// policy's server-version against the dialect the connection reported.
	target   lint.Target
	disabled []string
	rules    map[string]lint.RuleConfig
	// blocking are the rule names and categories whose blocking findings
	// refuse the apply: [ReportedRules] and whatever the policy's gate
	// section adds.
	blocking []string
	// online is the online mode, selected by the policy's `online` key.
	online bool
	// transactionOwner is who opens the transaction each file runs in, as
	// the apply runs the files.
	transactionOwner lintext.TransactionOwner
	// dirFormat is the format the apply reads the directory in, which decides
	// whether an R__<identity>.sql file is a repeatable migration.
	dirFormat migrationfile.DirFormat
	// atlasRevisionTable says the apply records versions in the Atlas revision
	// table; see [lint.Options.AtlasRevisionTable].
	atlasRevisionTable bool
}

// Pending is the work an apply selected: the versioned migrations by version,
// and the repeatable migrations by their slash-separated path in the
// migration directory.
type Pending struct {
	Versions    []int64
	Repeatables []string
	// GeneratedAtStart is the generated columns the target reports before the
	// first pending version runs, under that version
	// ([lint.Options.GeneratedAtStart]). Empty leaves every drop of a column
	// the migration adds back as generated reported.
	GeneratedAtStart []lintext.BaselineColumn
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

// BlockingRules returns the rule names and categories this policy refuses an
// apply on.
func (p Policy) BlockingRules() []string {
	return slices.Clone(p.blocking)
}

// LoadPolicy loads the conventional lint policy, validated against the rules
// of runtime, and resolves it against the connected database dialect.
func LoadPolicy(runtime lintext.Runtime, fsys fs.FS, databaseDialect string) (Policy, error) {
	cfg, err := lint.LoadConfigFS(runtime, fsys, lint.ConfigFileName)
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
	served, _ := runtime.(schemaext.TargetResolver)
	if !lintdialect.CompatibleIn(cfg.Dialect, databaseDialect, served) {
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
	if canonical, ok := lintdialect.CanonicalIn(databaseDialect, served); ok {
		databaseDialect = canonical
	}
	blocking := cfg.GateRules()
	// A rule the policy gates on must run, so it leaves the gate's own
	// disabled list; the policy's disabled-rules still apply to it.
	disabled, err := disabledRegisteredRules(runtime, blocking)
	if err != nil {
		return Policy{}, err
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
		blocking: blocking,
		online:   cfg.RequiresOnline(),
	}
	if err := validateOnlineDialect(cfg, databaseDialect); err != nil {
		return Policy{}, err
	}
	if err := lint.ValidateOptions(runtime, policy.options("")); err != nil {
		return Policy{}, err
	}
	return policy, nil
}

// Internal defaults may name an owner absent from this runtime. Omit those
// defaults, and any the policy gates on, but keep user selectors intact so an
// unsupported policy is refused.
func disabledRegisteredRules(runtime lintext.Runtime, enabled []string) ([]string, error) {
	rules, err := lint.Rules(runtime)
	if err != nil {
		return nil, err
	}
	var disabled []string
	for _, name := range DisabledRules() {
		registered := slices.ContainsFunc(rules, func(rule lint.Rule) bool { return rule.Name == name })
		gated := slices.ContainsFunc(enabled, func(selector string) bool { return lint.SelectorSelects(selector, name) })
		if registered && !gated {
			disabled = append(disabled, name)
		}
	}
	return disabled, nil
}

// Analyze loads policy and returns blocking data-safety findings for the
// selected pending migrations.
func Analyze(
	ctx context.Context,
	runtime lint.Runtime,
	fsys fs.FS,
	pending Pending,
	databaseDialect,
	pathPrefix string,
) ([]lint.Finding, error) {
	policy, err := LoadPolicy(runtime, fsys, databaseDialect)
	if err != nil {
		return nil, err
	}
	return AnalyzeWithPolicy(ctx, runtime, fsys, pending, policy, pathPrefix)
}

// AnalyzeWithPolicy returns blocking data-safety findings using a policy that
// was loaded before migration planning.
func AnalyzeWithPolicy(
	ctx context.Context,
	runtime lint.Runtime,
	fsys fs.FS,
	pending Pending,
	policy Policy,
	pathPrefix string,
) ([]lint.Finding, error) {
	options := policy.options(pathPrefix)
	options.GeneratedAtStart = pending.GeneratedAtStart
	options.Selection = lint.VersionSelection{
		Versions:    pending.Versions,
		Repeatables: pending.Repeatables,
		Restricted:  true,
	}
	findings, err := lint.LintFS(ctx, runtime, fsys, options)
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

// gatesOn reports whether a rule is one the policy blocks on, by name or by
// category.
func (p Policy) gatesOn(rule string) bool {
	return slices.ContainsFunc(p.blocking, func(selector string) bool { return lint.SelectorSelects(selector, rule) })
}

func (p Policy) options(pathPrefix string) lint.Options {
	return lint.Options{
		Dialect:            p.dialect,
		Target:             p.target,
		Disabled:           p.disabled,
		PathPrefix:         pathPrefix,
		RuleConfigs:        p.rules,
		RequireOnline:      p.online,
		TransactionOwner:   p.transactionOwner,
		DirFormat:          p.dirFormat,
		AtlasRevisionTable: p.atlasRevisionTable,
	}
}

// WithAtlasRevisionTable returns the policy for an apply that records versions
// in the Atlas revision table, which keeps the version R that R__x.sql spells
// in the Atlas format. Judged for Ptah's table, which refuses the directory for
// that file, the gate would report the file under file-form/refused-file although the apply
// runs it.
func (p Policy) WithAtlasRevisionTable() Policy {
	p.atlasRevisionTable = true
	return p
}

// WithDirFormat returns the policy reading the directory in the format the
// apply reads it in. A gate that read it in another format could take a
// repeatable migration the apply runs for a file it never selects.
func (p Policy) WithDirFormat(format migrationfile.DirFormat) Policy {
	p.dirFormat = format
	return p
}

// WithTransactionOwner returns the policy judging transaction control for
// files that run in transactions owner opens, as the apply runs them: a gate
// that judged a golang-migrate file for a migrator that wraps it would refuse
// the transaction control the apply runs as written.
func (p Policy) WithTransactionOwner(owner lintext.TransactionOwner) Policy {
	p.transactionOwner = owner
	return p
}

// RequiresOnline reports whether the policy selected the online mode.
//
// The apply path reads it for the one requirement no analysis of the SQL can
// carry: on PostgreSQL an ALTER that takes ACCESS EXCLUSIVE for an instant
// still queues behind whatever holds a conflicting lock, and every later
// reader and writer of that table queues behind the ALTER. A statement this
// mode calls online can therefore take an application down for as long as
// somebody else's SELECT runs, and the only defense is a lock timeout that
// turns the wait into a retry.
func (p Policy) RequiresOnline() bool {
	return p.online
}

// validateOnlineDialect refuses the online mode on a database the mode has no
// measurement for.
//
// The configuration validates its own dialect when it names one, and a policy
// that leaves the dialect to the connection reaches here with the engine the
// wire reported. Both ends need the refusal: a mode that ran on an engine
// nobody measured and reported nothing would be claiming a guarantee it never
// checked.
func validateOnlineDialect(cfg *lint.Config, databaseDialect string) error {
	if !cfg.RequiresOnline() {
		return nil
	}
	return lint.ValidateOnlineDialect(databaseDialect)
}

// policyTarget resolves the server the apply-time gate plans against, or
// leaves it unresolved on an engine the linter has no rules for. An engine an
// executable provider serves resolves a version to a release line its
// provider declares facts for.
func policyTarget(databaseDialect, serverVersion string) (lint.Target, error) {
	if _, ok := lintdialect.Canonical(databaseDialect); ok {
		return lint.ResolveTarget(databaseDialect, serverVersion)
	}
	if _, declared := capability.ProvidedReleaseLine(databaseDialect, serverVersion); declared && serverVersion != "" {
		resolved, err := servertarget.Resolve(databaseDialect, serverVersion)
		if err != nil {
			return lint.Target{}, fmt.Errorf("lint server-version: %w", err)
		}
		return lint.Target{Dialect: databaseDialect, Version: serverVersion, Capabilities: resolved.Capabilities}, nil
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
