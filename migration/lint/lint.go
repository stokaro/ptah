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
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/risk"
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

// Finding is one rule hit in one migration file.
type Finding struct {
	Rule     string   `json:"rule"`
	Title    string   `json:"title"`
	Severity Severity `json:"severity"`
	File     string   `json:"file"`
	// Line is the 1-based line of the offending statement's first token;
	// zero for file-level findings (naming, pairing).
	Line    int    `json:"line,omitempty"`
	Message string `json:"message"`
}

// Statement is one SQL statement of a migration file, in the forms rules
// consume.
type Statement struct {
	// SQL is the raw statement text as written (comments included).
	SQL string
	// Canonical is the comment-stripped, whitespace-collapsed, uppercased
	// display form. String literals keep their original case.
	Canonical string
	// Words is the token-word sequence the built-in rules scan: comments and
	// whitespace dropped, bare keywords/identifiers uppercased, punctuation
	// as its own entry, string literals and quoted identifiers kept verbatim
	// as single opaque words (so a literal containing "DROP COLUMN" or a
	// column named "type" can never impersonate a keyword).
	Words []string
	// Line is the 1-based line number of the statement's first token.
	Line int
	// SuppressedRules lists rule codes or families suppressed by a leading
	// ptah:nolint or atlas:nolint directive.
	SuppressedRules []string
}

// File is one migration file prepared for linting.
type File struct {
	// Path is the file path as it should appear in findings (reporting
	// prefix included).
	Path string
	// Name is the slash-separated path of the file relative to the linted
	// directory (bare file name for root-level files).
	Name string
	// Direction is the migration direction ("up"/"down") exactly as the
	// migrator's name parser classifies this file; empty when the migrator
	// cannot parse the name at all.
	Direction string
	// IsUp reports whether statement rules treat this as an up migration:
	// the migrator parses it as up, or its name carries the .up.sql suffix.
	IsUp bool
	// HasPair reports whether the matching counterpart file (down for up,
	// up for down) exists in the same directory.
	HasPair bool
	// WellFormedName reports whether the name matches the documented
	// NNNNNNNNNN_description.(up|down).sql convention, checked
	// independently of the migrator's parser as defense in depth (see the
	// strictNameRe comment).
	WellFormedName bool
	// NoTransaction reports whether file-scoped directives opt this migration
	// out of the migrator's transaction wrapper.
	NoTransaction bool
	// Statements holds the parsed statements of up migrations. Empty for
	// down migrations (statement rules do not run there).
	Statements []Statement
}

// Rule is one lint check. Exactly one of CheckStatement / CheckFile is set.
type Rule struct {
	// Code is the stable identifier (DS101, PG101, ...).
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
	// Versions restricts linting to parsed migration versions. Empty means
	// every migration file is linted.
	Versions []int64
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

// LintFS lints every *.sql file under fsys — recursively, because the
// migrator's FSMigrationProvider discovers migrations in subdirectories too —
// and returns the findings ordered by file, line and rule code. The .sql
// suffix is matched case-insensitively so that stray case variants (x.UP.SQL)
// at least earn a naming warning instead of vanishing.
func LintFS(fsys fs.FS, opts Options) ([]Finding, error) {
	if err := validateRules(rulesForOptions(opts)); err != nil {
		return nil, err
	}
	files, err := PrepareFS(fsys, opts)
	if err != nil {
		return nil, err
	}
	var findings []Finding
	for _, file := range files {
		findings = append(findings, runRules(file, opts)...)
	}

	sort.SliceStable(findings, func(i, j int) bool {
		if findings[i].File != findings[j].File {
			return findings[i].File < findings[j].File
		}
		if findings[i].Line != findings[j].Line {
			return findings[i].Line < findings[j].Line
		}
		return findings[i].Rule < findings[j].Rule
	})
	return findings, nil
}

// PrepareFS loads and tokenizes every migration file under fsys using the
// same dialect-aware scanner and naming rules as LintFS. Custom analyzer
// packages can call this when they need Ptah-prepared File and Statement
// values without reimplementing the scanner.
func PrepareFS(fsys fs.FS, opts Options) ([]*File, error) {
	var names []string
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() && strings.EqualFold(path.Ext(p), ".sql") {
			names = append(names, p)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list migration files: %w", err)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no *.sql migration files found")
	}
	sort.Strings(names)
	names = filterNamesByVersion(names, opts.Versions)
	names, err = filterAtlasTemplateSupportNames(fsys, names)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, nil
	}

	present := make(map[string]struct{}, len(names))
	for _, name := range names {
		present[name] = struct{}{}
	}

	// Directions present per version, so pairing matches the migrator, which
	// pairs an up and a down by their shared version prefix regardless of
	// description — not by an identical file-name stem.
	versionDirs := map[int64]map[string]bool{}
	for _, name := range names {
		if parsed, err := parseKnownMigrationName(path.Base(name)); err == nil {
			if versionDirs[parsed.Version] == nil {
				versionDirs[parsed.Version] = map[string]bool{}
			}
			versionDirs[parsed.Version][parsed.Direction] = true
		}
	}

	mode := modeForDialect(opts.Dialect)
	files := make([]*File, 0, len(names))
	for _, name := range names {
		file, err := prepareFile(fsys, name, present, versionDirs, opts.PathPrefix, mode, opts.AtlasTemplateData)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, nil
}

func filterNamesByVersion(names []string, versions []int64) []string {
	if len(versions) == 0 {
		return names
	}
	allowed := make(map[int64]struct{}, len(versions))
	for _, version := range versions {
		allowed[version] = struct{}{}
	}
	var filtered []string
	for _, name := range names {
		parsed, err := parseKnownMigrationName(path.Base(name))
		if err != nil {
			continue
		}
		if _, ok := allowed[parsed.Version]; ok {
			filtered = append(filtered, name)
		}
	}
	return filtered
}

func filterAtlasTemplateSupportNames(fsys fs.FS, names []string) ([]string, error) {
	hasAtlasTemplate, err := hasAtlasTemplateMigration(fsys, names)
	if err != nil || !hasAtlasTemplate {
		return names, err
	}

	filtered := make([]string, 0, len(names))
	for _, name := range names {
		if _, err := parseKnownMigrationName(path.Base(name)); err == nil {
			filtered = append(filtered, name)
			continue
		}
		support, err := isAtlasTemplateSupportFile(fsys, name)
		if err != nil {
			return nil, err
		}
		if !support {
			filtered = append(filtered, name)
		}
	}
	return filtered, nil
}

func hasAtlasTemplateMigration(fsys fs.FS, names []string) (bool, error) {
	for _, name := range names {
		parsed, err := parseKnownMigrationName(path.Base(name))
		if err != nil || parsed.Format != migrator.MigrationDirFormatAtlas {
			continue
		}
		raw, err := fs.ReadFile(fsys, name)
		if err != nil {
			return false, fmt.Errorf("failed to read %s: %w", name, err)
		}
		if migrator.LooksAtlasTemplateSQL(string(raw)) {
			return true, nil
		}
	}
	return false, nil
}

func isAtlasTemplateSupportFile(fsys fs.FS, name string) (bool, error) {
	raw, err := fs.ReadFile(fsys, name)
	if err != nil {
		return false, fmt.Errorf("failed to read %s: %w", name, err)
	}
	sql := string(raw)
	return migrator.LooksAtlasTemplateSQL(sql) && strings.Contains(sql, "define "), nil
}

// prepareFile loads one migration file into the forms rules consume.
func prepareFile(
	fsys fs.FS,
	name string,
	present map[string]struct{},
	versionDirs map[int64]map[string]bool,
	pathPrefix string,
	mode scanMode,
	atlasTemplateData any,
) (*File, error) {
	raw, err := fs.ReadFile(fsys, name)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", name, err)
	}

	base := path.Base(name)
	direction := ""
	var version int64
	hasVersion := false
	atlasFormat := false
	if parsed, parseErr := parseKnownMigrationName(base); parseErr == nil {
		direction = parsed.Direction
		version = parsed.Version
		hasVersion = true
		atlasFormat = parsed.Format == migrator.MigrationDirFormatAtlas
	}
	file := &File{
		Path:      path.Join(pathPrefix, name),
		Name:      name,
		Direction: direction,
		// The migrator executes whatever its parser classifies as up, so
		// lint must follow it; the suffix check keeps hazard scanning for
		// .up.sql files whose version prefix is malformed.
		IsUp:           direction == "up" || strings.HasSuffix(base, ".up.sql"),
		WellFormedName: strictNameRe.MatchString(base) || atlasFormat,
		NoTransaction:  fileNoTransactionDirective(string(raw)),
	}
	switch {
	case atlasFormat:
		file.HasPair = true
	case hasVersion:
		// Pair by version, matching the migrator: the counterpart is any
		// file of the same version in the opposite direction.
		counterpart := "down"
		if direction == "down" {
			counterpart = "up"
		}
		file.HasPair = versionDirs[version][counterpart]
	case file.IsUp:
		_, file.HasPair = present[strings.TrimSuffix(name, ".up.sql")+".down.sql"]
	case strings.HasSuffix(name, ".down.sql"):
		_, file.HasPair = present[strings.TrimSuffix(name, ".down.sql")+".up.sql"]
	}

	// Statement rules apply to up migrations only.
	if file.IsUp {
		sql := string(raw)
		if atlasFormat && migrator.LooksAtlasTemplateSQL(sql) {
			rendered, _, err := migrator.RenderAtlasTemplateSQL(fsys, name, atlasTemplateData)
			if err != nil {
				return nil, err
			}
			sql = rendered
		}
		for _, rawStmt := range splitStatementsWithLines(sql, mode) {
			file.Statements = append(file.Statements, Statement{
				SQL:             rawStmt.text,
				Canonical:       canonicalize(rawStmt.text, mode),
				Words:           tokenizeWords(rawStmt.text, mode),
				Line:            rawStmt.line,
				SuppressedRules: rawStmt.suppressedRules,
			})
		}
	}
	return file, nil
}

func parseKnownMigrationName(name string) (*migrator.MigrationFile, error) {
	if parsed, err := migrator.ParseMigrationFileName(name); err == nil {
		return parsed, nil
	}
	return migrator.ParseAtlasMigrationFileNameForAutoDetection(name)
}

func fileNoTransactionDirective(sql string) bool {
	if value := migrator.ParseFileDirectives(sql)[migrator.DirectiveNoTransaction]; value == "true" {
		return true
	}
	for line := range strings.Lines(sql) {
		if strings.EqualFold(strings.TrimSpace(line), "-- atlas:txmode none") {
			return true
		}
	}
	return false
}

// runRules applies every enabled rule to one prepared file.
func runRules(file *File, opts Options) []Finding {
	var findings []Finding
	for _, rule := range rulesForOptions(opts) {
		if ruleDisabled(rule.Code, opts.Disabled) || !ruleAppliesToDialect(rule, opts.Dialect) || ruleExcludedForFile(rule.Code, file, opts.RuleConfigs) {
			continue
		}
		severity := ruleSeverity(rule, opts.RuleConfigs)
		if rule.CheckFile != nil {
			for _, finding := range rule.CheckFile(file) {
				if fileFindingSuppressed(file, finding) {
					continue
				}
				finding.Severity = severity
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
				})
			}
		}
	}
	return findings
}

func rulesForOptions(opts Options) []Rule {
	rules := Rules()
	return append(rules, opts.ExtraRules...)
}

func ruleSeverity(rule Rule, configs map[string]RuleConfig) Severity {
	config, ok := configs[rule.Code]
	if !ok || config.Severity == "" {
		return rule.Severity
	}
	return config.Severity
}

func ruleExcludedForFile(code string, file *File, configs map[string]RuleConfig) bool {
	config, ok := configs[code]
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

func statementSuppressesRule(stmt *Statement, code string) bool {
	for _, entry := range stmt.SuppressedRules {
		if entry == "*" || strings.HasPrefix(code, entry) {
			return true
		}
	}
	return false
}

func fileFindingSuppressed(file *File, finding Finding) bool {
	if finding.Line == 0 {
		return false
	}
	for i := range file.Statements {
		stmt := &file.Statements[i]
		if stmt.Line == finding.Line && statementSuppressesRule(stmt, finding.Rule) {
			return true
		}
	}
	return false
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
	suppressedRules []string
}

// splitStatementsWithLines splits SQL into statements using the dialect-aware
// scanner (so semicolons inside strings, comments, executable comments and
// dollar-quoted bodies do not terminate statements) while tracking each
// statement's starting line.
func splitStatementsWithLines(raw string, mode scanMode) []rawStatement {
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
				statements = append(statements, rawStatement{text: raw[start:end], line: startLine, suppressedRules: activeSuppressions})
				start = -1
				activeSuppressions = nil
				lastStatementEndLine = tok.line
			}
		case tokWhitespace:
			// Never starts a statement; keep end untouched so trailing
			// comments/whitespace are not included in the statement text.
		case tokComment:
			if start < 0 && tok.line != lastStatementEndLine {
				pendingSuppressions = append(pendingSuppressions, parseNoLintDirective(tok.text)...)
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
		statements = append(statements, rawStatement{text: raw[start:end], line: startLine, suppressedRules: activeSuppressions})
	}
	return statements
}

func parseNoLintDirective(comment string) []string {
	trimmed := strings.TrimSpace(comment)
	if !strings.HasPrefix(trimmed, "--") && !strings.HasPrefix(trimmed, "#") {
		return nil
	}
	lower := strings.ToLower(comment)
	idx := strings.Index(lower, "ptah:nolint")
	markerLen := len("ptah:nolint")
	if idx < 0 {
		idx = strings.Index(lower, "atlas:nolint")
		markerLen = len("atlas:nolint")
	}
	if idx < 0 {
		return nil
	}
	rest := strings.TrimSpace(comment[idx+markerLen:])
	if rest == "" {
		return []string{"*"}
	}
	parts := strings.FieldsFunc(rest, isNoLintSeparator)
	rules := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.ToUpper(strings.TrimSpace(part))
		if part != "" {
			rules = append(rules, part)
		}
	}
	return rules
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
