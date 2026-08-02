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
// Statement-level rules run on up migrations only: a down migration dropping
// what its up created is the expected shape, not a hazard. File-level form
// rules (naming, pairing) look at every *.sql file.
package lint

import (
	"path"
	"regexp"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/atlaslint"
	"go.5x5.cz/ptah/internal/lexer"
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
	// CheckStatement inspects one statement of an up migration and reports
	// whether the rule fires, with a specific message.
	CheckStatement func(stmt *Statement) (bool, string)
	// CheckFile inspects file-level form and returns full findings.
	CheckFile func(file *File) []Finding
}

// RuleConfig customizes one rule for a lint run.
type RuleConfig struct {
	// Severity overrides the rule's default severity when set.
	Severity Severity `yaml:"severity,omitempty"`
	// Exclude lists slash-separated path globs where this rule is skipped.
	Exclude []string `yaml:"exclude,omitempty"`
}

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

func fileNoTransactionDirective(sql string) bool {
	if value := migrator.ParseFileDirectives(sql)[migrator.DirectiveNoTransaction]; value == "true" {
		return true
	}
	return hasAtlasTxModeNoneDirective(sql)
}

func hasAtlasTxModeNoneDirective(sql string) bool {
	lexr := lexer.NewLexer(sql)
	for {
		tok := lexr.NextToken()
		if tok.Type == lexer.TokenEOF {
			break
		}
		if tok.Type != lexer.TokenComment {
			continue
		}
		comment, ok := strings.CutPrefix(tok.Value, "--")
		if !ok {
			continue
		}
		if !commentStartsLine(sql, tok.Start) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(comment), "atlas:txmode none") {
			return true
		}
	}
	return false
}

func commentStartsLine(sql string, pos int) bool {
	for i := pos - 1; i >= 0; i-- {
		switch sql[i] {
		case '\n':
			return true
		case ' ', '\t', '\r':
			continue
		default:
			return false
		}
	}
	return true
}

// runRules applies every enabled rule to one prepared file.
func runRules(file *File, opts Options, rules []Rule) []Finding {
	var findings []Finding
	for _, rule := range rules {
		if ruleDisabled(rule.Code, opts.Disabled) ||
			fileSuppressesRule(file, rule.Code) ||
			!ruleAppliesToDialect(rule, opts.Dialect) ||
			ruleExcludedForFile(rule.Code, file, opts.RuleConfigs) {
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
					Context: statementFindingContext(
						i,
						statementSubjects(
							rule.Code,
							file.Statements[i].Words,
							file.Statements[i].sourceWords,
						)...,
					),
				})
			}
		}
	}
	return findings
}

func rulesForOptions(opts Options) []Rule {
	rules := Rules()
	return append(rules, cloneRules(opts.ExtraRules)...)
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

func suppressesRule(entries []string, code string) bool {
	for _, entry := range entries {
		if entry == "*" || strings.HasPrefix(code, entry) {
			return true
		}
	}
	return false
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
	pattern = path.Clean(strings.TrimSpace(pattern))
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
func ruleDisabled(code string, disabled []string) bool {
	for _, entry := range disabled {
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
	suppressedRules []string
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
	var pendingSuppressions []string
	var activeSuppressions []string
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
				activeSuppressions = append([]string(nil), pendingSuppressions...)
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
) []string {
	trimmed := strings.TrimSpace(comment)
	if !strings.HasPrefix(trimmed, "--") && !strings.HasPrefix(trimmed, "#") {
		return nil
	}
	if rules, ok := parseNoLintMarker(comment, "ptah:nolint", nativeNoLintTargets); ok {
		return rules
	}
	targets := nativeNoLintTargets
	if compatibility == CompatibilityProfileAtlas {
		targets = atlaslint.NativeSuppressionTargets
	}
	rules, _ := parseNoLintMarker(comment, "atlas:nolint", targets)
	return rules
}

func parseAtlasFileNoLint(sql string) ([]string, bool) {
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
				return rules, suppressesRule(rules, "*")
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

func parseAtlasNoLintLine(line string) ([]string, bool) {
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
	comment string,
	marker string,
	targets func(string) []string,
) ([]string, bool) {
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

func parseNoLintRules(rest string, targets func(string) []string) []string {
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return []string{"*"}
	}
	parts := strings.FieldsFunc(rest, isNoLintSeparator)
	rules := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.ToUpper(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		rules = append(rules, targets(part)...)
	}
	return rules
}

func nativeNoLintTargets(entry string) []string {
	return []string{entry}
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
