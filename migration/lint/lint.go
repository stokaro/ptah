// Package lint inspects migration files for production-unsafe patterns and
// emits rule-coded findings, in the spirit of sqlcheck and `atlas migrate
// lint` (issue #151).
//
// # Rule codes
//
//   - DS — data safety (dropping tables/columns, lossy type changes)
//   - MF — migration form (missing down file, empty migration, naming)
//   - BC — breaking-change safety (renames breaking deployed readers)
//   - PG — PostgreSQL-specific hazards
//   - MY — MySQL/MariaDB-specific hazards
//
// # How statements are matched
//
// Each *.up.sql file is split into statements by a dialect-aware scanner:
// string literals, quoted identifiers, comments (including the MySQL-family
// # line comments and /*!...*/ executable comments) and PostgreSQL
// dollar-quoted bodies never confuse the splitter, and Options.Dialect
// selects which of those syntaxes apply. Every statement is then checked through
// the token form exposed to rules:
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
package lint

import (
	"path"
	"regexp"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/atlaslint"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/risk"
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

// Rule is one lint check. Exactly one of CheckStatement / CheckFile is set.
type Rule struct {
	// Code is the stable identifier (DS101, PG101, ...). It starts with an
	// uppercase ASCII letter and contains only uppercase ASCII letters and digits.
	Code string
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
	// It is required for, and only meaningful to, [InputBaselineSchema].
	// Statement indexes rather than a yes/no answer, because the reviewed-schema
	// filter works at that granularity: a rename in a schema the run does not
	// review is neither worth a dev-database round trip nor worth reporting as
	// unresolved, and answering per file could not tell the two apart.
	//
	// A directory with nothing for the rule to resolve therefore costs no round
	// trip at all.
	BaselineSubjects func(file *File) []int
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
	// handed back through [Options.Baseline]; see [BaselineColumn].
	InputBaselineSchema
)

// String names the input for diagnostics.
func (i RuleInput) String() string {
	switch i {
	case InputBaselineSchema:
		return "baseline schema"
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
	// fires; see [go.5x5.cz/ptah/migration/lint] package documentation for the
	// vocabulary.
	//
	// Its presence is what separates the two uses of this type: an entry with
	// Match defines a new rule, an entry without it configures a rule that
	// already exists. A code that already belongs to a built-in rule cannot be
	// declared, so a typo can never quietly replace a built-in check with a
	// weaker one of the same name.
	Match string `yaml:"match,omitempty"`
	// Message is the finding text a declared rule reports. It is required when
	// Match is set: a finding whose message is the rule code tells a reader
	// what fired and nothing about why it matters.
	Message string `yaml:"message,omitempty"`
	// Title is the declared rule's short name in reports. It defaults to the
	// rule code.
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
	// Dialect gates dialect-specific rules — "postgres" enables PG rules,
	// "mysql"/"mariadb" enable MY rules — and selects the dialect's lexing
	// behavior (comment forms, string escape rules, dollar quotes). Empty
	// runs every rule under a hybrid lexer — maximum visibility when the
	// target is unknown.
	Dialect string
	// Disabled lists rule codes or code prefixes to skip: "DS101" disables
	// one rule, "DS" the whole data-safety family.
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
	DirFormat migrator.MigrationDirFormat
	// AtlasTemplateData supplies data for Atlas SQL template migrations.
	// When nil, templates render with migrator.AtlasTemplateData{}.
	AtlasTemplateData any
	// ExtraRules appends caller-provided rules to the built-in registry for
	// this run. It is the preferred API for out-of-tree analyzers that should
	// not mutate global process state.
	ExtraRules []Rule
	// RuleConfigs carries per-rule severity and path-scoping overrides,
	// normally loaded from .ptah-lint.yaml.
	RuleConfigs map[string]RuleConfig
	// Baseline carries the schema state each analyzed version starts from,
	// normally read from the dev database after replaying the migrations that
	// precede that version. Empty analyzes SQL text alone, which is what every
	// run without a dev database does. See [BaselineColumn].
	Baseline []BaselineColumn
}

func parseKnownMigrationName(name string, dirFormat migrator.MigrationDirFormat) (*migrator.MigrationFile, error) {
	switch dirFormat {
	case migrator.MigrationDirFormatPtah:
		return migrator.ParseMigrationFileName(name)
	case migrator.MigrationDirFormatAtlas:
		return migrator.ParseAtlasMigrationFileName(name)
	}
	if parsed, err := migrator.ParseMigrationFileName(name); err == nil {
		return parsed, nil
	}
	return migrator.ParseAtlasMigrationFileNameForAutoDetection(name)
}

// runRules applies every enabled rule to one prepared file.
func runRules(file *File, opts Options, rules []Rule) []Finding {
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
			if statementSuppressesRule(&file.Statements[i], rule.Code) {
				continue
			}
			if hit, message := rule.CheckStatement(&file.Statements[i]); hit {
				findings = append(findings, Finding{
					Rule:     rule.Code,
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

func rulesForOptions(opts Options) ([]Rule, error) {
	rules := append(Rules(), cloneRules(opts.ExtraRules)...)
	declared, err := compileDeclaredRules(opts.RuleConfigs, rules, opts.Dialect)
	if err != nil {
		return nil, err
	}
	return append(rules, declared...), nil
}

func ruleSeverity(rule Rule, configs map[string]RuleConfig) Severity {
	config, ok := ruleConfigForCode(rule.Code, configs)
	if !ok || config.Severity == "" {
		return rule.Severity
	}
	return config.Severity
}

func ruleExcludedForFile(code string, file *File, configs map[string]RuleConfig) bool {
	config, ok := ruleConfigForCode(code, configs)
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

func ruleConfigForCode(code string, configs map[string]RuleConfig) (RuleConfig, bool) {
	if len(configs) == 0 {
		return RuleConfig{}, false
	}
	if config, ok := configs[code]; ok {
		return config, true
	}
	// An entry written under the Atlas spelling governs the Ptah rule that
	// reports it, so `rules: {PG301: {severity: warning}}` reaches DS103
	// (stokaro/ptah#1631).
	for _, atlasCode := range AtlasCodeFor(code) {
		if config, ok := configs[atlasCode]; ok {
			return config, true
		}
	}
	bestPrefix := ""
	var best RuleConfig
	for prefix, config := range configs {
		if strings.HasPrefix(code, prefix) && len(prefix) > len(bestPrefix) {
			bestPrefix = prefix
			best = config
		}
	}
	return best, bestPrefix != ""
}

func statementSuppressesRule(stmt *Statement, code string) bool {
	return suppressesRule(stmt.suppressedRules, code)
}

func fileSuppressesRule(file *File, code string) bool {
	return suppressesRule(file.suppressedRules, code)
}

func suppressesRule(entries []atlaslint.Target, code string) bool {
	return slices.ContainsFunc(entries, func(entry atlaslint.Target) bool {
		return entry.Matches(code)
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

// ruleDisabled reports whether code matches any disabled entry — exact code
// or family prefix ("DS" disables every DS rule).
//
// dialect scopes the Atlas alias expansion: a PostgreSQL Atlas code must not
// silence a generic Ptah rule while linting MySQL (stokaro/ptah#1631).
func ruleDisabled(code string, disabled []string, dialect string) bool {
	for _, entry := range expandAtlasCodeSelectorsForDialect(disabled, dialect) {
		entry = strings.TrimSpace(entry)
		if entry != "" && strings.HasPrefix(code, entry) {
			return true
		}
	}
	return false
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
// form while Direction follows the migrator, so if the two ever diverge
// again (as they did before #245, when the migrator's unescaped dot made
// 0000000001_cleanup.sql run as an up migration) lint keeps scanning
// whatever the migrator would execute and MF103 explains the ambiguity.
var strictNameRe = regexp.MustCompile(`^\d{10}_.+\.(up|down)\.sql$`)

// rawStatement is one raw SQL statement plus the line it starts on.
type rawStatement struct {
	text            string
	line            int
	start           int
	end             int
	suppressedRules []atlaslint.Target
}

// splitStatementsWithLines splits SQL into statements using the dialect-aware
// scanner (so semicolons inside strings, comments, executable comments and
// dollar-quoted bodies do not terminate statements) while tracking each
// statement's starting line.
func splitStatementsWithLines(
	raw string,
	mode scanMode,
	compatibility CompatibilityProfile,
) []rawStatement {
	var statements []rawStatement
	start := -1
	end := 0
	startLine := 0
	lastStatementEndLine := 0
	var pendingSuppressions []atlaslint.Target
	var activeSuppressions []atlaslint.Target
	for _, tok := range scanSQL(raw, mode) {
		switch tok.kind {
		case tokSemicolon:
			if start >= 0 {
				statements = append(statements, rawStatement{
					text:            raw[start:end],
					line:            startLine,
					start:           start,
					end:             end,
					suppressedRules: activeSuppressions,
				})
				start = -1
				activeSuppressions = nil
				lastStatementEndLine = tok.line
			}
		case tokWhitespace:
			// Never starts a statement; keep end untouched so trailing
			// comments/whitespace are not included in the statement text.
			//
			// A blank line detaches a pending directive from whatever comes
			// next: a statement-local nolint has to sit directly above its
			// statement. Measured against atlas community version v1.3.0 on
			// `-- atlas:nolint DS103`, a blank line, then a DROP COLUMN — it
			// reports DS103 and exits 1, while the same two lines with no
			// blank line between them exit 0. A directive on the file's first
			// line followed by a blank line is the file-wide header form,
			// handled by [parseAtlasFileNoLint] on the compatibility surface
			// only.
			if start < 0 && strings.Count(tok.text, "\n") > 1 {
				pendingSuppressions = nil
			}
		case tokComment:
			if start < 0 && tok.line != lastStatementEndLine {
				pendingSuppressions = append(
					pendingSuppressions,
					parseNoLintDirective(tok.text, compatibility)...,
				)
			}
		default:
			if start < 0 {
				start = tok.start
				startLine = tok.line
				activeSuppressions = slices.Clone(pendingSuppressions)
				pendingSuppressions = nil
			}
			end = tok.end
		}
	}
	if start >= 0 {
		statements = append(statements, rawStatement{
			text:            raw[start:end],
			line:            startLine,
			start:           start,
			end:             end,
			suppressedRules: activeSuppressions,
		})
	}
	return statements
}

func parseNoLintDirective(
	comment string,
	compatibility CompatibilityProfile,
) []atlaslint.Target {
	trimmed := strings.TrimSpace(comment)
	if !strings.HasPrefix(trimmed, "--") && !strings.HasPrefix(trimmed, "#") {
		return nil
	}
	if rules, ok := parseNoLintMarker(comment, "ptah:nolint", nativeNoLintTargets); ok {
		return rules
	}
	rules, _ := parseNoLintMarker(comment, "atlas:nolint", atlasNoLintTargets(compatibility))
	return rules
}

// atlasNoLintTargets resolves one `atlas:nolint` selector for the surface that
// is running. The selector vocabulary is Atlas's on both surfaces: analyzer
// names name rule families, and a code selector matches one code exactly rather
// than widening into a family the way `ptah:nolint DS` does. Only the code
// namespace is surface-specific — a selector names the code the running surface
// prints, which is the native code natively and the Atlas identity under the
// compatibility profile.
func atlasNoLintTargets(compatibility CompatibilityProfile) func(string) []atlaslint.Target {
	if compatibility == CompatibilityProfileAtlas {
		return atlaslint.NativeSuppressionTargets
	}
	return nativeAtlasNoLintTargets
}

func nativeAtlasNoLintTargets(entry string) []atlaslint.Target {
	if targets, ok := atlaslint.AnalyzerSuppressionTargets(entry); ok {
		return targets
	}
	return []atlaslint.Target{atlaslint.CodeTarget(entry)}
}

func parseAtlasFileNoLint(sql string) ([]atlaslint.Target, bool) {
	lines := strings.Split(sql, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(strings.TrimPrefix(line, "\uFEFF"))
		if trimmed == "" {
			continue
		}
		rules, ok := parseAtlasNoLintLine(trimmed)
		if !ok {
			return nil, false
		}
		for _, following := range lines[i+1:] {
			trimmedFollowing := strings.TrimSpace(following)
			if trimmedFollowing == "" {
				return rules, slices.ContainsFunc(rules, atlaslint.Target.MatchesEveryRule)
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

func parseAtlasNoLintLine(line string) ([]atlaslint.Target, bool) {
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
	return parseNoLintRules(rest, atlaslint.NativeSuppressionTargets), true
}

func parseNoLintMarker(
	comment,
	marker string,
	targets func(string) []atlaslint.Target,
) ([]atlaslint.Target, bool) {
	idx := strings.Index(strings.ToLower(comment), marker)
	if idx < 0 {
		return nil, false
	}
	rest := comment[idx+len(marker):]
	if rest != "" && !isNoLintSeparator(rune(rest[0])) {
		return nil, false
	}
	return parseNoLintRules(rest, targets), true
}

func parseNoLintRules(rest string, targets func(string) []atlaslint.Target) []atlaslint.Target {
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return []atlaslint.Target{atlaslint.FamilyTarget("")}
	}
	parts := strings.FieldsFunc(rest, isNoLintSeparator)
	rules := make([]atlaslint.Target, 0, len(parts))
	for _, part := range parts {
		part = strings.ToUpper(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		rules = append(rules, targets(part)...)
	}
	return rules
}

// nativeNoLintTargets resolves a selector against the native code namespace,
// where a code prefix names its family: `ptah:nolint DS` silences every
// data-safety rule and `ptah:nolint DS102` silences one.
func nativeNoLintTargets(entry string) []atlaslint.Target {
	return []atlaslint.Target{atlaslint.FamilyTarget(entry)}
}

func isNoLintSeparator(r rune) bool {
	return strings.ContainsRune(",;:/*)([]{}\"'` \t\r\n", r)
}

// canonicalize renders a statement in its display form: comments removed,
// every whitespace run collapsed to one space, everything except string
// literals and quoted identifiers uppercased.
func canonicalize(sql string, mode scanMode) string {
	var b strings.Builder
	pendingSpace := false
	for _, tok := range scanSQL(sql, mode) {
		switch tok.kind {
		case tokComment, tokWhitespace:
			// A comment separates tokens exactly like whitespace does
			// (DROP/*x*/TABLE must not canonicalize to DROPTABLE).
			pendingSpace = b.Len() > 0
		case tokString, tokQuotedIdent:
			if pendingSpace {
				b.WriteByte(' ')
				pendingSpace = false
			}
			b.WriteString(tok.text)
		default:
			if pendingSpace {
				b.WriteByte(' ')
				pendingSpace = false
			}
			b.WriteString(strings.ToUpper(tok.text))
		}
	}
	return b.String()
}

// tokenizeWords renders a statement as the word sequence rules scan (see
// Statement.Words). String literals and quoted identifiers keep their quotes
// and case, so quoted names never collide with SQL keywords in the scans.
func tokenizeWords(sql string, mode scanMode) []string {
	var words []string
	for _, tok := range scanSQL(sql, mode) {
		switch tok.kind {
		case tokComment, tokWhitespace:
			continue
		case tokString, tokQuotedIdent:
			words = append(words, tok.text)
		default:
			words = append(words, strings.ToUpper(tok.text))
		}
	}
	return words
}

func tokenizeSourceWords(sql string, mode scanMode) []string {
	var words []string
	for _, tok := range scanSQL(sql, mode) {
		switch tok.kind {
		case tokComment, tokWhitespace:
			continue
		default:
			words = append(words, tok.text)
		}
	}
	return words
}

// ruleRunsOnFile reports whether rule is enabled for this file under opts.
//
// It is shared with [baselineVersions] on purpose: the set of rules that run on
// a file and the set that may ask for that file's starting schema state have to
// be the same set, or a run reads a dev database for a rule it then skips, or
// skips a read for a rule it then runs (stokaro/ptah#1632).
func ruleRunsOnFile(rule Rule, file *File, opts Options) bool {
	return !ruleDisabled(rule.Code, opts.Disabled, opts.Dialect) &&
		!fileSuppressesRule(file, rule.Code) &&
		ruleAppliesToDialect(rule, opts.Dialect) &&
		!ruleExcludedForFile(rule.Code, file, opts.RuleConfigs)
}
