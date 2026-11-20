// Package lint inspects migration files for production-unsafe patterns and
// emits findings named for their rule, in the spirit of sqlcheck and `atlas migrate
// lint` (issue #151).
//
// # Rule names
//
// A rule is identified by its name, `category/name`, the spelling a finding
// reports, a selector selects and a baseline records. The category says what
// the statement risks (`destructive`, `rollout`, `locking`, `rewrite`, and so
// on, declared in [ptah.run/internal/lintnames]); the engine is the rule's
// Dialects. A selector is a name or a category and matches exactly, never as a
// prefix. A hazard checked on several engines is one rule name: each engine's
// owner supplies its check, the dialects of the checks do not overlap, and
// their default severities match. Ptah rules have no numeric code, and a word
// spelled as one is refused wherever a selector is read.
//
// # How statements are matched
//
// Each *.up.sql file is split into statements by a dialect-aware scanner:
// string literals, quoted identifiers, comments (including the MySQL-family
// # line comments and /*!...*/ executable comments) and PostgreSQL
// dollar-quoted bodies never confuse the splitter, and Options.Dialect
// selects which of those syntaxes apply. A run that names a dialect asks its
// owner to split the files, which on YDB is the provider's YQL split, the one
// its migration units come from. Every statement is then checked through the token
// form exposed to rules:
//
//   - Statement.Words — the token-word sequence rules scan,
//     anchored at ALTER TABLE clause boundaries; string literals and quoted
//     identifiers stay opaque single words, so data or a column named like a
//     keyword can neither trigger nor mask a rule.
//
// Statement-level rules run on up migrations by default: a down migration
// dropping what its up created is the expected shape, not a hazard. A rule
// whose subject is the cost or the executability of a statement rather than the
// schema change it expresses sets [Rule.AppliesToDown] and is checked in both
// directions -- PostgreSQL charges the same lock for a blocking DROP INDEX
// whether a migration is being applied or rolled back. File-level form rules
// (naming, pairing) look at every *.sql file.
//
// # Rules the engine owners check
//
// An engine owner may publish rules of its own through the runtime a run is
// given, as [ptah.run/core/lintext] data. The PostgreSQL owner publishes its
// locking, rewrite and transaction rules and the online rules online/unproven and
// online/lock-held-across-scan that way, the MySQL owner its rules and its
// share of online/unproven, and the YDB provider its ydb rules. [Rules]
// lists them after this package's own. A run asks the owners for their
// findings once, over every file, and applies the same policy to them as to
// every other finding: suppression directives, severity overrides, excluded
// paths, subsumption and the reviewed-schema filter. A run that names no
// dialect sends every owner's rules over the dialect-blind split, as it runs
// every rule of this package.
//
// # Facts only an owner recognizes
//
// Some rules of this package need to know what a statement does where only
// its dialect's owner can tell: destructive/drop-object reports a drop that loses state a new
// object does not get back, such as a YDB topic's messages; data-dependent/add-unique-index and data-dependent/rebuild-index-as-unique
// read YQL's unique index; destructive/drop-table, rollout/drop-table and the rename rules pass over the
// steps of a table rebuild, which YDB's owner matches against the directory's
// own history. The owner's split reports these as statement effects, and the
// rules read the effects instead of naming a dialect. A run that names no
// dialect asks every owner to judge the drops over the dialect-blind split, in
// one call; a statement an owner declines is reported as unjudged
// ([Analysis.UnjudgedStatements]).
package lint

import (
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintnames"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/risk"
)

// Severity is the urgency of a finding.
type Severity = risk.Severity

const (
	// SeverityWarning marks patterns that deserve review before a prod
	// rollout but are not necessarily destructive.
	SeverityWarning Severity = risk.Warning
	// SeverityError marks patterns that destroy data or database objects.
	SeverityError Severity = risk.Error
	// SeverityInfo marks findings that are reported and never gated on. A rule
	// configured this way appears in every report and changes no exit code,
	// which is what lets one be introduced to a repository that still violates
	// it (stokaro/ptah#1633).
	SeverityInfo Severity = risk.Info
)

// Rule is one lint check. Exactly one of CheckStatement / CheckFile is set,
// except on a rule an engine owner publishes: [Rules] returns that one with
// neither, because its owner checks it.
type Rule struct {
	// Name is the stable identifier, `category/name`, such as
	// `destructive/drop-table`: two lowercase segments joined by a slash, the
	// first a declared category (see [ptah.run/internal/lintnames]). Rules for
	// different dialects may share a name when they report one hazard, their
	// Dialects do not overlap and their default severities match.
	Name string
	// Title is the short human-readable name of the hazard.
	Title string
	// Severity is the default severity of findings from this rule.
	Severity Severity
	// Dialects restricts the rule to specific target dialects; empty means
	// every dialect.
	Dialects []string
	// CheckStatement inspects one statement of a migration and reports whether
	// the rule fires, with a specific message. It sees up migrations only
	// unless AppliesToDown is set.
	CheckStatement func(stmt *Statement) (bool, string)
	// AppliesToDown extends a CheckStatement rule to the down half of a
	// migration. It is off by default because most statement rules describe a
	// forward schema change, where the rollback is the remedy rather than the
	// hazard. Set it for a rule that describes the cost or the executability of
	// a statement, which the database charges identically in either direction:
	// a blocking DROP INDEX holds the same lock whether a migration is being
	// applied or rolled back.
	//
	// It has no effect on a CheckFile rule, which receives every file already
	// and decides for itself; those read [File.IsUp] and [File.IsDown].
	AppliesToDown bool
	// CheckFile inspects file-level form and returns full findings.
	CheckFile func(file *File) []Finding
	// Subsumes names rules whose finding on the same statement this rule's
	// finding replaces. A rule that can say which ENUM member a MODIFY removed
	// and that the server copies the table for it leaves nothing for "a column
	// type changed" and "this ALTER usually rebuilds the table" to add, and
	// reporting all three reads as three hazards where there is one. The
	// generic finding is dropped only when the specific one actually fired,
	// so a run that could not resolve the specific one -- no baseline, say --
	// still reports the statement under the generic rule.
	//
	// Every code named here must be a rule the run applies; an analysis
	// refuses a rule that subsumes an unknown one, because a misspelled code
	// would suppress nothing and say nothing.
	Subsumes []string
	// Input declares what this rule reads. The zero value,
	// [InputStatementText], is the migration SQL and nothing else.
	//
	// It is a declaration rather than a comment: [Analysis.BaselineVersions]
	// is computed from it, so a rule that needs the starting schema state and
	// does not say so is handed an empty one -- and a rule that says so and
	// does not get it is reported by [Analysis.UnmetInputs] instead of quietly
	// finding less (stokaro/ptah#1632).
	Input RuleInput
	// BaselineSubjects returns the indexes of the statements in this file whose
	// analysis would say more with the schema state its version starts from.
	//
	// It is required for, and only meaningful to, [InputBaselineSchema] and
	// [InputBaselineRefinement].
	// Statement indexes rather than a yes/no answer, because the reviewed-schema
	// filter works at that granularity: a rename in a schema the run does not
	// review is neither worth a dev-database round trip nor worth reporting as
	// unresolved, and answering per file could not tell the two apart.
	//
	// A directory with nothing for the rule to resolve therefore costs no round
	// trip at all.
	BaselineSubjects func(file *File) []int
	// owned marks a rule an engine owner publishes and checks, whose findings
	// and baseline subjects an analysis reads from the owner's answer.
	owned bool
}

// RuleInput is the analyzer input a [Rule] reads.
//
// Ptah's analyzers read migration SQL; Atlas replays the directory against a
// dev database and analyzes the resulting schema. A concern whose subject
// exists only in the post-state -- the type of a column a RENAME introduces,
// say -- is unreachable from the text, so those rules take a second input.
// Which of the two a rule takes is declared here rather than inferred, because
// the failure mode of getting it wrong is silence: the rule runs, resolves
// nothing, and reports less than the tool it replaces while exiting 0.
type RuleInput uint8

const (
	// InputStatementText reads the migration SQL and nothing else. It is the
	// zero value, so a rule says nothing about its input only when its input
	// is the text.
	InputStatementText RuleInput = iota
	// InputBaselineSchema additionally reads the schema state the analyzed
	// version starts from, replayed onto a dev database by the caller and
	// handed back through [Options.Baseline]; see [lintext.BaselineColumn].
	InputBaselineSchema
	// InputRoutineBody additionally reads the parsed body of a routine the
	// statement defines; see [Statement.Routine].
	//
	// It is a third input rather than a detail of the second because it fails
	// the same way and for its own reason: parsing a body needs a dialect, and
	// without --dialect the body is not parsed, the rule finds nothing, and the
	// run exits 0. Declaring the input is what turns that into a named gap
	// instead of a smaller report (stokaro/ptah#2357).
	InputRoutineBody
	// InputBaselineRefinement reads the migration SQL and, when the run
	// supplies it, the schema state the version starts from as well. The
	// rule reports from the text alone, and the state refines the report: an
	// exemption the text cannot see, or a recognition the text cannot make.
	// It is declared the way [InputBaselineSchema] is, with the statements
	// it wants the state for in [Rule.BaselineSubjects], and a run that
	// supplies none is named as unmet the same way, because a report that
	// went without the refinement is thinner in a direction the reader
	// cannot see from the findings (stokaro/ptah#2957).
	InputBaselineRefinement
)

// readsBaseline reports whether an input kind reads the schema state a
// version starts from, whether as its subject or as a refinement of the
// text. One predicate for both, because the two are read from the same
// place and requested the same way; only what a rule does without the state
// differs.
func (i RuleInput) readsBaseline() bool {
	return i == InputBaselineSchema || i == InputBaselineRefinement
}

// String names the input for diagnostics.
func (i RuleInput) String() string {
	switch i {
	case InputRoutineBody:
		return "routine body"
	case InputBaselineSchema:
		return "baseline schema"
	case InputBaselineRefinement:
		return "baseline schema that refines the statement text"
	case InputStatementText:
		return "statement text"
	default:
		return "unknown input"
	}
}

// RuleConfig customizes one rule for a lint run.
type RuleConfig struct {
	// Severity overrides the rule's default severity when set. On a DECLARED
	// rule it is the rule's own severity rather than an override, and defaults
	// to warning.
	Severity Severity `yaml:"severity,omitempty"`
	// Exclude lists slash-separated path globs where this rule is skipped.
	Exclude []string `yaml:"exclude,omitempty"`

	// Match DECLARES a rule rather than configuring one. It is an expression
	// over `statement`, `file` and `dialect` that decides whether the rule
	// fires; see [ptah.run/migration/lint] package documentation for the
	// vocabulary.
	//
	// Its presence is what separates the two uses of this type: an entry with
	// Match defines a new rule, an entry without it configures a rule that
	// already exists. A name that already belongs to a built-in rule cannot be
	// declared, so a typo can never quietly replace a built-in check with a
	// weaker one of the same name.
	Match string `yaml:"match,omitempty"`
	// Message is the finding text a declared rule reports. It is required when
	// Match is set: a finding whose message is the rule name tells a reader
	// what fired and nothing about why it matters.
	Message string `yaml:"message,omitempty"`
	// Title is the declared rule's short name in reports. It defaults to the
	// rule name.
	Title string `yaml:"title,omitempty"`
	// Dialects restricts a declared rule to specific target dialects. Empty
	// runs it under every dialect.
	Dialects []string `yaml:"dialects,omitempty"`
	// AppliesToDown extends a declared rule to the down half of a migration.
	// It is off by default for the same reason it is off on a built-in rule.
	AppliesToDown bool `yaml:"applies-to-down,omitempty"`
}

// Declares reports whether this entry declares a rule rather than configuring
// an existing one.
func (c RuleConfig) Declares() bool { return strings.TrimSpace(c.Match) != "" }

// Options configures a lint run.
type Options struct {
	// Compatibility selects command-surface-specific semantics. The zero value
	// is native Ptah behavior.
	Compatibility CompatibilityProfile
	// ChangeCount selects how [File.Changes] counts the schema changes of
	// each up file. The zero value counts the changes each statement makes;
	// see [ChangeCount] for the counts that match the community Atlas
	// binary's summary.
	ChangeCount ChangeCount
	// Dialect gates dialect-specific rules — "postgres" enables PG rules,
	// "mysql"/"mariadb" enable MY rules, "ydb" enables YD rules — and selects
	// the dialect's lexing behavior (comment forms, string escape rules,
	// dollar quotes). Empty runs every rule under a hybrid lexer — maximum
	// visibility when the target is unknown — except the YD rules, which
	// read YQL and say nothing about text the hybrid lexer read.
	//
	// The value must be a canonical dialect name. [AnalyzeFS] and [LintFS]
	// take it as given rather than alias-folding it, so a spelling such as
	// "postgresql" is not recognized as "postgres" and the PG rules it was
	// meant to enable do not run. [LoadConfig] and [LoadConfigFS] canonicalize
	// the dialect a configuration names and reject unsupported ones; resolve a
	// user-typed spelling through them and store what they return.
	Dialect string
	// Disabled lists the rules to skip, each a rule name or a category:
	// "destructive/drop-table" disables one rule and "destructive" every rule
	// named under that category. A selector matches exactly, never as a
	// prefix.
	Disabled []string
	// PathPrefix is prepended (with /) to file names in findings so they
	// point at real repository paths in CI annotations.
	PathPrefix string
	// SchemaScope names the single schema under review, normally the schema the
	// dev database URL restricts analysis to. Findings and schema changes that
	// name an object in a different schema are excluded: that object is not in
	// the state the run compares against, so changing it is not a covered
	// change. Empty puts every schema under review and filters nothing.
	SchemaScope string
	// Selection restricts linting to parsed migration versions while retaining
	// the full captured directory for reporting.
	Selection VersionSelection
	// DirFormat selects the migration filename/parser rules used by file-form
	// linting. Empty uses auto detection.
	DirFormat migrationfile.DirFormat
	// AtlasRevisionTable says the run records versions in the Atlas revision
	// table, which keeps any version an Atlas file spells. The zero value is
	// Ptah's revision table, which records numbers: a run over it refuses a
	// directory read in the Atlas format that holds a version such as R, the
	// one R__x.sql spells, and lint reports the file at error severity.
	AtlasRevisionTable bool
	// TransactionOwner says who opens the transaction each migration runs in
	// when the tool that applies the directory runs it, which the rules about
	// transaction control read. The zero value is a migrator that wraps each
	// file, which is what Ptah's and Atlas's do; a caller that knows the
	// directory is applied by a tool that opens none, such as golang-migrate,
	// sets [lintext.TransactionOwnerFile]. Such a run also judges an up or
	// down file that names no transaction mode and holds one statement the
	// engine refuses inside a transaction as one that runs without a
	// transaction ([File.NoTransaction]), as the migrator applies and rolls
	// it back.
	TransactionOwner lintext.TransactionOwner
	// AtlasTemplateData supplies data for Atlas SQL template migrations.
	// When nil, templates render with migrationfile.AtlasTemplateData{}.
	AtlasTemplateData any
	// AtlasSelectors reads the selectors of `atlas:nolint` comments under
	// [CompatibilityProfileAtlas]: it maps an Atlas check code or analyzer name
	// to the Ptah rule names it silences. Only the compatibility surface sets
	// it; Ptah holds no table from Atlas codes to rules. Left nil, an
	// `atlas:nolint` comment that names a selector silences no rule.
	// Native analysis refuses `atlas:nolint` and never calls it.
	AtlasSelectors AtlasSelectors
	// ExtraRules appends caller-provided rules to the rules of the runtime
	// for this run: an in-process analyzer's way in. An analyzer that runs in
	// an engine owner publishes its rules through the runtime instead.
	ExtraRules []Rule
	// RuleConfigs carries per-rule severity and path-scoping overrides,
	// normally loaded from .ptah-lint.yaml.
	RuleConfigs map[string]RuleConfig
	// RequireOnline selects the online mode: the ON family runs, and every
	// statement it cannot prove takes no conflicting lock on this engine is
	// reported at error severity.
	//
	// It is an option rather than a rule selector because the family is an
	// allowlist. Left on by default it would report most statements of most
	// migrations, which is correct for the guarantee and useless as advice.
	//
	// The mode covers PostgreSQL, MySQL and MariaDB. Another dialect is
	// refused by the caller that selects the mode rather than reported clean
	// here, because a run that proved nothing and found nothing is the one
	// answer this family must never give.
	RequireOnline bool
	// Unparsed selects the statements rule coverage/unparsed-statement reports when the SQL parser
	// could not read them. The zero value reports none, for a caller whose
	// statements a server reads next or that wrote them itself.
	Unparsed UnparsedScope
	// Baseline carries the schema state each analyzed version starts from,
	// normally read from the dev database after replaying the migrations that
	// precede that version. Empty analyzes SQL text alone, which is what every
	// run without a dev database does. See [lintext.BaselineColumn].
	Baseline []lintext.BaselineColumn

	// GeneratedAtStart carries the generated columns a live database reports
	// for the version about to run, for a run that reads no starting state:
	// the apply gate of `ptah migrations up` passes the target's under the
	// first pending version. Only the drop rules read it, to tell a generated
	// column a migration adds back as generated from a column that stores what
	// clients wrote ([lintext.BaselineColumn.Generated]). A version Baseline
	// describes reads Baseline instead.
	GeneratedAtStart []lintext.BaselineColumn

	// BaselineDependents carries what reads each column in the state a version
	// starts from, for the rule that reports a drop by naming what breaks.
	//
	// Separate from Baseline because it is resolved rather than read: the
	// columns are a catalog query and these come from analyzing view and
	// routine bodies, so a caller can supply one without the other and the
	// unmet-input report says which is missing.
	BaselineDependents []lintext.BaselineDependent

	// BaselineIndexes carries the indexes of the state each version starts
	// from, read in the same catalog read as Baseline, for the rules whose
	// answer turns on whether a column is indexed, which index a statement
	// names, or whether the rows are already proven unique. See
	// [lintext.BaselineIndex].
	BaselineIndexes []lintext.BaselineIndex

	// BaselineHypertables carries the TimescaleDB hypertables of the state
	// each version starts from, read in the same catalog read as Baseline,
	// for the rule whose remedy the engine refuses on a hypertable. See
	// [lintext.BaselineHypertable].
	BaselineHypertables []lintext.BaselineHypertable

	// BaselineObjects carries the types, views, materialized views,
	// triggers and sequences of the state each version starts from, read in
	// the same catalog read as Baseline, of the kinds each entry of
	// BaselineRead lists in full. They show an IF EXISTS drop of one to drop
	// nothing. See [lintext.BaselineObject].
	BaselineObjects []lintext.BaselineObject

	// BaselineRead lists the versions whose starting state the caller read,
	// normally every version [Analysis.BaselineVersions] asked for, with the
	// kinds of object each read lists in full. A listed version with no row
	// in the Baseline fields starts from an empty schema, which is what a
	// directory's first version meets on a clean dev database. A version with
	// no row that is not listed is a state nobody read, about which the rules
	// assume nothing. A version with rows was read whether or not it is
	// listed, and lists no kind of object in full. See [lintext.BaselineRead].
	BaselineRead []lintext.BaselineRead

	// Naming is the naming convention the NM rules enforce, normally the
	// `naming` section of .ptah-lint.yaml or the `lint { naming { } }` block
	// of a project file. Nil leaves the six rules silent: a convention is the
	// project's to state, and the rules carry none of their own.
	Naming *NamingConfig

	// Target is the server the analyzed migrations will run against, resolved
	// through [ResolveTarget] from a declared version or from the dev database
	// a run connected to. Every analyzed [File] and [Statement] carries it, so
	// a rule whose answer depends on the server reads it there.
	//
	// The zero value names no server. [AnalyzeFS] then resolves the dialect
	// default into it, so a rule never meets an empty capability set that it
	// would have to tell apart from a server answering false to everything.
	Target Target
}

// parseKnownMigrationName reads the migration a directory entry names. The
// name is the path inside the directory; the parsers read its base.
func parseKnownMigrationName(name string, dirFormat migrationfile.DirFormat) (*migrationfile.File, error) {
	base := path.Base(name)
	switch dirFormat {
	case migrationfile.DirFormatPtah:
		return migrationfile.ParseFileName(base)
	case migrationfile.DirFormatAtlas:
		parsed, err := migrationfile.ParseAtlasFileName(base)
		// Atlas reads the top level of a directory alone, so a nested file
		// whose name carries no number, such as a template under shared/, is
		// no migration there.
		if err == nil && base != name && parsed.Version == 0 && !parsed.Repeatable {
			return nil, fmt.Errorf("%s is not an Atlas migration file", name)
		}
		return parsed, err
	}
	if parsed, err := migrationfile.ParseFileName(base); err == nil {
		return parsed, nil
	}
	return migrationfile.ParseAtlasFileNameForAutoDetection(base)
}

// runRules applies every enabled rule to one prepared file.
func runRules(file *File, opts Options, rules []Rule) []Finding {
	return dropSubsumedFindings(collectFindings(file, opts, rules), rules)
}

// dropSubsumedFindings applies [Rule.Subsumes]: a finding is removed when
// another finding on the same statement comes from a rule that names its
// rule. The pass is a function of the collected findings alone, so it is
// deterministic and independent of rule order.
func dropSubsumedFindings(findings []Finding, rules []Rule) []Finding {
	subsumedBy := make(map[string][]string, len(rules))
	for _, rule := range rules {
		for _, code := range rule.Subsumes {
			subsumedBy[code] = append(subsumedBy[code], rule.Name)
		}
	}
	if len(subsumedBy) == 0 {
		return findings
	}
	fired := make(map[string]bool, len(findings))
	for _, finding := range findings {
		if finding.Context != nil {
			fired[subsumptionKey(finding.Context.StatementIndex, finding.Rule)] = true
		}
	}
	kept := findings[:0]
	for _, finding := range findings {
		subsumed := false
		if finding.Context != nil {
			for _, winner := range subsumedBy[finding.Rule] {
				if fired[subsumptionKey(finding.Context.StatementIndex, winner)] {
					subsumed = true
					break
				}
			}
		}
		if !subsumed {
			kept = append(kept, finding)
		}
	}
	return kept
}

// subsumptionKey identifies one rule's finding on one statement.
func subsumptionKey(statement int, code string) string {
	return strconv.Itoa(statement) + "\x00" + code
}

func collectFindings(file *File, opts Options, rules []Rule) []Finding {
	var findings []Finding
	for _, rule := range rules {
		if !ruleRunsOnFile(rule, file, opts) {
			continue
		}
		severity := ruleSeverity(rule, opts.RuleConfigs)
		if rule.CheckFile != nil {
			for _, finding := range rule.CheckFile(file) {
				if fileFindingSuppressed(file, finding) {
					continue
				}
				finding.Severity = severity
				attachUniqueStatementContext(file, &finding)
				findings = append(findings, finding)
			}
			continue
		}
		if !file.IsUp && !rule.AppliesToDown {
			continue
		}
		for i := range file.Statements {
			if statementSuppressesRule(&file.Statements[i], rule.Name) {
				continue
			}
			if hit, message := rule.CheckStatement(&file.Statements[i]); hit {
				findings = append(findings, Finding{
					Rule:     rule.Name,
					Title:    rule.Title,
					Severity: severity,
					File:     file.Path,
					Line:     file.Statements[i].Line,
					Message:  message,
					// Statement rules report the statement, not the objects in
					// it: a rule that needs subjects needs file scope to build
					// them (see columnDroppedRule) and is written as CheckFile.
					Context: statementFindingContext(i),
				})
			}
		}
	}
	return findings
}

func rulesForOptions(runtime lintext.Runtime, opts Options) ([]Rule, error) {
	rules, err := Rules(runtime)
	if err != nil {
		return nil, err
	}
	rules = append(rules, cloneRules(opts.ExtraRules)...)
	declared, err := compileDeclaredRules(opts.RuleConfigs, rules, opts.Dialect)
	if err != nil {
		return nil, err
	}
	return append(rules, declared...), nil
}

func ruleSeverity(rule Rule, configs map[string]RuleConfig) Severity {
	config, ok := ruleConfigForName(rule.Name, configs)
	if !ok || config.Severity == "" {
		return rule.Severity
	}
	return config.Severity
}

func ruleExcludedForFile(name string, file *File, configs map[string]RuleConfig) bool {
	config, ok := ruleConfigForName(name, configs)
	if !ok {
		return false
	}
	for _, pattern := range config.Exclude {
		if pathGlobMatches(pattern, file.Name) || pathGlobMatches(pattern, file.Path) {
			return true
		}
	}
	return false
}

// ruleConfigForName returns the `rules:` entry that governs the rule: the one
// keyed by its name, else the one keyed by its category, written `locking` or
// `locking/`. The two selectors the entry could be keyed by are the two
// [lintnames.Selects] accepts, so the section and every other selector agree.
func ruleConfigForName(name string, configs map[string]RuleConfig) (RuleConfig, bool) {
	if len(configs) == 0 {
		return RuleConfig{}, false
	}
	for _, key := range lintnames.ConfigKeys(name) {
		if config, ok := configs[key]; ok {
			return config, true
		}
	}
	for _, key := range lintnames.GroupKeys(name) {
		if config, ok := configs[key]; ok {
			return config, true
		}
	}
	return RuleConfig{}, false
}

func statementSuppressesRule(stmt *Statement, name string) bool {
	return suppressesRule(stmt.suppressedRules, name)
}

func fileSuppressesRule(file *File, name string) bool {
	return suppressesRule(file.suppressedRules, name)
}

func suppressesRule(entries []noLintTarget, name string) bool {
	return slices.ContainsFunc(entries, func(entry noLintTarget) bool {
		return entry.Matches(name)
	})
}

func statementFindingContext(index int, subjects ...Subject) *FindingContext {
	return &FindingContext{
		StatementIndex: index,
		Subjects:       slices.Clone(subjects),
	}
}

func attachUniqueStatementContext(file *File, finding *Finding) {
	if finding.Context != nil || finding.Line == 0 {
		return
	}
	index := -1
	for i := range file.Statements {
		if file.Statements[i].Line != finding.Line {
			continue
		}
		if index >= 0 {
			return
		}
		index = i
	}
	if index >= 0 {
		finding.Context = statementFindingContext(index)
	}
}

func fileFindingSuppressed(file *File, finding Finding) bool {
	if finding.Context != nil {
		index := finding.Context.StatementIndex
		return index >= 0 &&
			index < len(file.Statements) &&
			statementSuppressesRule(&file.Statements[index], finding.Rule)
	}
	if finding.Line == 0 {
		return false
	}
	index := -1
	for i := range file.Statements {
		if file.Statements[i].Line != finding.Line {
			continue
		}
		if index >= 0 {
			return false
		}
		index = i
	}
	return index >= 0 && statementSuppressesRule(&file.Statements[index], finding.Rule)
}

func pathGlobMatches(pattern, value string) bool {
	value = path.Clean(value)
	if pattern == "." || value == "." {
		return false
	}
	if ok, err := path.Match(pattern, value); err == nil && ok {
		return true
	}
	return matchGlobSegments(strings.Split(pattern, "/"), strings.Split(value, "/"))
}

func matchGlobSegments(pattern, value []string) bool {
	if len(pattern) == 0 {
		return len(value) == 0
	}
	if pattern[0] == "**" {
		for i := 0; i <= len(value); i++ {
			if matchGlobSegments(pattern[1:], value[i:]) {
				return true
			}
		}
		return false
	}
	if len(value) == 0 {
		return false
	}
	ok, err := path.Match(pattern[0], value[0])
	if err != nil || !ok {
		return false
	}
	return matchGlobSegments(pattern[1:], value[1:])
}

// SelectorSelects reports whether a rule selector, as written in
// `disabled-rules`, `--disable`, a `rules:` key or a `ptah:nolint`
// directive, selects the rule called name.
//
// A selector is a rule name, `locking/add-foreign-key`, or a category,
// `locking` or `locking/`, which selects every rule named under it. Both match
// exactly and never by prefix: `locking/add` selects nothing, so a selector
// cannot reach a rule its author did not spell. One predicate decides every
// selector of every linter; see [lintnames.Selects].
func SelectorSelects(selector, name string) bool {
	return lintnames.Selects(strings.TrimSpace(selector), name)
}

// knownNames is the set of rule names a selector is judged against: the
// rules this run registers.
func knownNames(rules []Rule) lintnames.Set {
	names := make([]string, 0, len(rules))
	for _, rule := range rules {
		names = append(names, rule.Name)
	}
	return lintnames.NewSet("migration lint", names)
}

// ruleDisabled reports whether name matches any disabled entry by
// [SelectorSelects].
func ruleDisabled(name string, disabled []string) bool {
	return slices.ContainsFunc(disabled, func(entry string) bool { return SelectorSelects(entry, name) })
}

// ruleAppliesToDialect reports whether a rule runs for the configured target
// dialect. An empty configured dialect runs everything.
func ruleAppliesToDialect(rule Rule, dialect string) bool {
	if len(rule.Dialects) == 0 || dialect == "" {
		return true
	}
	return slices.Contains(rule.Dialects, dialect)
}

// strictNameRe is the documented migration naming convention, encoded
// independently of the migrator's parser: WellFormedName checks this strict
// form while Direction follows the migrator, so if the two ever diverge —
// an unescaped dot in the migrator's pattern makes 0000000001_cleanup.sql run
// as an up migration (stokaro/ptah#245) — lint keeps scanning whatever the
// migrator would execute and file-form/file-name explains the ambiguity.
var strictNameRe = regexp.MustCompile(`^\d{10}_.+\.(up|down)\.sql$`)

// noLintTarget is one resolved nolint selector: a rule name, a category, or,
// for a directive written without a selector, every rule.
type noLintTarget struct{ selector string }

// everyRuleTarget silences every rule, which is what a bare directive means.
func everyRuleTarget() noLintTarget { return noLintTarget{} }

// selectorTarget silences the rule or category selector names, by
// [lintnames.Selects].
func selectorTarget(selector string) noLintTarget { return noLintTarget{selector: selector} }

// Matches reports whether the target silences the rule called name.
func (t noLintTarget) Matches(name string) bool {
	return t.MatchesEveryRule() || lintnames.Selects(t.selector, name)
}

// MatchesEveryRule reports whether the target silences every rule.
func (t noLintTarget) MatchesEveryRule() bool { return t.selector == "" }

// AtlasSelectors translates the selector of an `atlas:nolint` comment, an
// Atlas check code or analyzer name, into the Ptah rule names it silences.
// Only the compatibility surface supplies one; native analysis holds no table
// from Atlas codes to rules.
type AtlasSelectors func(selector string) []string

// parseNoLintDirective reads the rules one comment above a statement
// suppresses. A `ptah:nolint` naming a rule name or category migration lint
// does not know, or spelled as a rule code, is refused: matched against
// nothing, the misspelling would leave the finding reported with the author
// believing it silenced.
//
// An `atlas:nolint` comment is Atlas's directive. The compatibility profile
// reads it through atlas, which maps the Atlas selector it names to rules;
// native analysis refuses it.
func parseNoLintDirective(
	comment string,
	compatibility CompatibilityProfile,
	known lintnames.Set,
	atlas AtlasSelectors,
) ([]noLintTarget, error) {
	trimmed := strings.TrimSpace(comment)
	if !strings.HasPrefix(trimmed, "--") && !strings.HasPrefix(trimmed, "#") {
		return nil, nil
	}
	if rules, ok, err := parseNoLintMarker(comment, "ptah:nolint", func(rest string) ([]noLintTarget, error) {
		return parsePtahNoLint(rest, known)
	}); ok {
		return rules, err
	}
	rules, _, err := parseNoLintMarker(comment, "atlas:nolint", func(rest string) ([]noLintTarget, error) {
		if compatibility != CompatibilityProfileAtlas {
			return nil, errAtlasNoLintNative
		}
		return parseAtlasNoLintRules(rest, atlas), nil
	})
	return rules, err
}

// errAtlasNoLintNative is the refusal of an `atlas:nolint` comment by native
// analysis. It names the directive Ptah reads and no Atlas code.
var errAtlasNoLintNative = errors.New("atlas:nolint is an Atlas directive that ptah-compat reads; " +
	"native Ptah reads ptah:nolint with a rule name, such as ptah:nolint " + exampleRuleName)

// exampleRuleName is the rule name refusals show.
const exampleRuleName = "locking/add-foreign-key"

// parseAtlasNoLintRules resolves the selectors of an `atlas:nolint` comment
// through atlas. Words are split at the separators an Atlas selector list may
// use, and each is upper-cased the way the community binary reads it. A nil
// atlas resolves every selector to no rule; a directive without one still
// means every rule.
func parseAtlasNoLintRules(rest string, atlas AtlasSelectors) []noLintTarget {
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return []noLintTarget{everyRuleTarget()}
	}
	var rules []noLintTarget
	for part := range strings.FieldsFuncSeq(rest, isNoLintSeparator) {
		part = strings.ToUpper(strings.TrimSpace(part))
		if part == "" || atlas == nil {
			continue
		}
		for _, name := range atlas(part) {
			rules = append(rules, selectorTarget(name))
		}
	}
	return rules
}

func parseAtlasFileNoLint(sql string, atlas AtlasSelectors) ([]noLintTarget, bool) {
	lines := strings.Split(sql, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(strings.TrimPrefix(line, "\uFEFF"))
		if trimmed == "" {
			continue
		}
		rules, ok := parseAtlasNoLintLine(trimmed, atlas)
		if !ok {
			return nil, false
		}
		for _, following := range lines[i+1:] {
			trimmedFollowing := strings.TrimSpace(following)
			if trimmedFollowing == "" {
				return rules, slices.ContainsFunc(rules, noLintTarget.MatchesEveryRule)
			}
			if strings.HasPrefix(trimmedFollowing, "--") {
				continue
			}
			return nil, false
		}
		return nil, false
	}
	return nil, false
}

func parseAtlasNoLintLine(line string, atlas AtlasSelectors) ([]noLintTarget, bool) {
	comment, ok := strings.CutPrefix(strings.TrimSpace(line), "--")
	if !ok {
		return nil, false
	}
	body := strings.TrimSpace(comment)
	const marker = "atlas:nolint"
	if len(body) < len(marker) || !strings.EqualFold(body[:len(marker)], marker) {
		return nil, false
	}
	rest := body[len(marker):]
	if rest != "" && !isNoLintSeparator(rune(rest[0])) {
		return nil, false
	}
	return parseAtlasNoLintRules(rest, atlas), true
}

func parseNoLintMarker(
	comment,
	marker string,
	parse func(rest string) ([]noLintTarget, error),
) ([]noLintTarget, bool, error) {
	idx := strings.Index(strings.ToLower(comment), marker)
	if idx < 0 {
		return nil, false, nil
	}
	rest := comment[idx+len(marker):]
	if rest != "" && !isNoLintSeparator(rune(rest[0])) {
		return nil, false, nil
	}
	rules, err := parse(rest)
	return rules, true, err
}

// parsePtahNoLint reads the selectors of a `ptah:nolint` directive.
//
// The directive is a run of selectors followed by free text. The selectors
// come first and the text starts at the first word that is not one, or at a
// word beginning with `--`; nothing after that point selects a rule, so a
// word in the reason can never widen the suppression. A selector is:
//
//   - a rule name or a category written with a slash (`locking/`);
//   - a bare category (`locking`), as the first word only, because a lower-case
//     word later in a line is as likely prose as a selector.
//
// A word is refused when it is clearly meant as a name and is wrong: its first
// segment is a declared category (`locking/typo`), or it is one lower-case word
// ending in a slash (`lockin/`). A word spelled as a rule code, such as
// `destructive/drop-column` or `destructive/drop-table/DS102`, is refused too: Ptah rules are named, and a code
// is not a rule name. Any other word holding a slash, such as a URL or
// `and/or`, is free text. A directive with no selector and no text suppresses
// every rule; one with text and no selector suppresses none.
func parsePtahNoLint(rest string, known lintnames.Set) ([]noLintTarget, error) {
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return []noLintTarget{everyRuleTarget()}, nil
	}
	words := strings.FieldsFunc(rest, func(r rune) bool { return r != '/' && isNoLintSeparator(r) })
	rules := make([]noLintTarget, 0, len(words))
	for position, word := range words {
		if code := codeShapedWord(word); code != "" {
			return nil, fmt.Errorf("ptah:nolint: %w", lintnames.RefuseCode(code))
		}
		var (
			resolved   []noLintTarget
			isSelector bool
			err        error
		)
		if _, bare := lintnames.CategoryFor(word); bare && position == 0 {
			resolved, isSelector, err = resolveNoLintName(word, known)
		} else if lintnames.MeantAsSelector(word) {
			resolved, isSelector, err = resolveNoLintName(word, known)
		}
		if err != nil {
			return nil, fmt.Errorf("ptah:nolint: %w", err)
		}
		if !isSelector {
			break
		}
		rules = append(rules, resolved...)
	}
	return rules, nil
}

// codeShapedWord returns the part of a nolint word that is spelled as a rule
// code, or the empty string. A code is upper-case letters and at least one
// digit; a word of codes joined by slashes counts, and a word with any other
// part, a URL for one, does not.
func codeShapedWord(word string) string {
	first := ""
	for part := range strings.SplitSeq(word, "/") {
		if part == "" {
			continue
		}
		if !lintnames.IsCode(part) || !strings.ContainsAny(part, "0123456789") {
			return ""
		}
		if first == "" {
			first = part
		}
	}
	return first
}

// resolveNoLintName resolves a rule name or category, and refuses one that
// selects no rule migration lint has.
func resolveNoLintName(word string, known lintnames.Set) ([]noLintTarget, bool, error) {
	selected, err := known.Resolve(word)
	if err != nil {
		return nil, true, err
	}
	rules := make([]noLintTarget, 0, len(selected))
	for _, name := range selected {
		rules = append(rules, selectorTarget(name))
	}
	return rules, true, nil
}

func isNoLintSeparator(r rune) bool {
	return strings.ContainsRune(",;:/*)([]{}\"'` \t\r\n", r)
}

// UnparsedScope says which statements of a run no server reads, so that a parse
// failure on one of them is the only thing anyone will learn about it.
//
// A parse failure does not say the statement is wrong. The parser refuses valid
// SQL it does not model as well as text that is no SQL at all, and the two read
// alike to it, so the answer is a report rather than a refusal.
type UnparsedScope uint8

const (
	// UnparsedNone reports no statement. It is the zero value.
	UnparsedNone UnparsedScope = iota
	// UnparsedDown reports the down files alone, for a run that replays the up
	// files on a dev database, which never runs a down file.
	UnparsedDown
	// UnparsedAll reports every statement, for a run with no dev database.
	UnparsedAll
)

// covers reports whether the statements of file are in the scope.
func (s UnparsedScope) covers(file *File) bool {
	switch s {
	case UnparsedAll:
		return true
	case UnparsedDown:
		return file.IsDown
	default:
		return false
	}
}

// onlineRuleSelected reports whether an ON rule may run.
//
// The family is an allowlist, so on an ordinary run it would report almost
// every statement in almost every migration. It runs only where a caller asked
// for the guarantee, and the selection lives here rather than in the rules
// because [ruleRunsOnFile] is the one place that decides what runs at all --
// a second gate would be a second answer to the same question.
func onlineRuleSelected(name string, opts Options) bool {
	return opts.RequireOnline || lintnames.CategoryOf(name) != OnlineCategory
}

// ruleRunsOnFile reports whether rule is enabled for this file under opts.
//
// It is shared with [baselineVersions] on purpose: the set of rules that run on
// a file and the set that may ask for that file's starting schema state have to
// be the same set, or a run reads a dev database for a rule it then skips, or
// skips a read for a rule it then runs (stokaro/ptah#1632).
func ruleRunsOnFile(rule Rule, file *File, opts Options) bool {
	return onlineRuleSelected(rule.Name, opts) &&
		!ruleDisabled(rule.Name, opts.Disabled) &&
		!fileSuppressesRule(file, rule.Name) &&
		ruleAppliesToDialect(rule, opts.Dialect) &&
		!ruleExcludedForFile(rule.Name, file, opts.RuleConfigs)
}
