// Package lint inspects migration files for production-unsafe patterns and
// emits rule-coded findings, in the spirit of sqlcheck and `atlas migrate
// lint` (issue #151).
//
// # Rule codes
//
//   - DS — data safety (dropping tables/columns, lossy type changes)
//   - MF — migration form (missing down file, empty migration, naming)
//   - BC — backwards compatibility (renames breaking deployed readers)
//   - PG — PostgreSQL-specific hazards
//   - MY — MySQL/MariaDB-specific hazards
//
// # How statements are matched
//
// Each *.up.sql file is split into statements by a dialect-aware scanner:
// string literals, quoted identifiers, comments (including the MySQL-family
// # line comments and /*!...*/ executable comments) and PostgreSQL
// dollar-quoted bodies never confuse the splitter, and Options.Dialect
// selects which of those syntaxes apply. Every statement is then checked in
// two complementary forms:
//
//   - Statement.Words — the token-word sequence the built-in rules scan,
//     anchored at ALTER TABLE clause boundaries; string literals and quoted
//     identifiers stay opaque single words, so data or a column named like a
//     keyword can neither trigger nor mask a rule;
//   - a best-effort AST (Statement.Node) parsed with core/parser for the
//     statements it supports, available to rules that want structural
//     precision.
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

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/parser"
	"github.com/stokaro/ptah/migration/migrator"
)

// Severity is the urgency of a finding.
type Severity string

const (
	// SeverityWarning marks patterns that deserve review before a prod
	// rollout but are not necessarily destructive.
	SeverityWarning Severity = "warning"
	// SeverityError marks patterns that destroy data or database objects.
	SeverityError Severity = "error"
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
	// Node is the best-effort parse of the statement into ptah's DDL AST;
	// nil when the statement is outside the parser's supported subset.
	// Word-scan rules must not rely on it.
	Node ast.Node
	// Line is the 1-based line number of the statement's first token.
	Line int
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
}

// LintFS lints every *.sql file under fsys — recursively, because the
// migrator's FSMigrationProvider discovers migrations in subdirectories too —
// and returns the findings ordered by file, line and rule code. The .sql
// suffix is matched case-insensitively so that stray case variants (x.UP.SQL)
// at least earn a naming warning instead of vanishing.
func LintFS(fsys fs.FS, opts Options) ([]Finding, error) {
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

	present := make(map[string]struct{}, len(names))
	for _, name := range names {
		present[name] = struct{}{}
	}

	// Directions present per version, so pairing matches the migrator, which
	// pairs an up and a down by their shared version prefix regardless of
	// description — not by an identical file-name stem.
	versionDirs := map[int]map[string]bool{}
	for _, name := range names {
		if parsed, err := migrator.ParseMigrationFileName(path.Base(name)); err == nil {
			if versionDirs[parsed.Version] == nil {
				versionDirs[parsed.Version] = map[string]bool{}
			}
			versionDirs[parsed.Version][parsed.Direction] = true
		}
	}

	mode := modeForDialect(opts.Dialect)
	var findings []Finding
	for _, name := range names {
		file, err := prepareFile(fsys, name, present, versionDirs, opts.PathPrefix, mode)
		if err != nil {
			return nil, err
		}
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

// prepareFile loads one migration file into the forms rules consume.
func prepareFile(fsys fs.FS, name string, present map[string]struct{}, versionDirs map[int]map[string]bool, pathPrefix string, mode scanMode) (*File, error) {
	raw, err := fs.ReadFile(fsys, name)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", name, err)
	}

	base := path.Base(name)
	direction := ""
	version := -1
	if parsed, parseErr := migrator.ParseMigrationFileName(base); parseErr == nil {
		direction = parsed.Direction
		version = parsed.Version
	}
	file := &File{
		Path:      path.Join(pathPrefix, name),
		Name:      name,
		Direction: direction,
		// The migrator executes whatever its parser classifies as up, so
		// lint must follow it; the suffix check keeps hazard scanning for
		// .up.sql files whose version prefix is malformed.
		IsUp:           direction == "up" || strings.HasSuffix(base, ".up.sql"),
		WellFormedName: strictNameRe.MatchString(base),
	}
	switch {
	case version >= 0:
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
		for _, rawStmt := range splitStatementsWithLines(string(raw), mode) {
			file.Statements = append(file.Statements, Statement{
				SQL:       rawStmt.text,
				Canonical: canonicalize(rawStmt.text, mode),
				Words:     tokenizeWords(rawStmt.text, mode),
				Node:      parseBestEffort(rawStmt.text),
				Line:      rawStmt.line,
			})
		}
	}
	return file, nil
}

// runRules applies every enabled rule to one prepared file.
func runRules(file *File, opts Options) []Finding {
	var findings []Finding
	for _, rule := range Rules() {
		if ruleDisabled(rule.Code, opts.Disabled) || !ruleAppliesToDialect(rule, opts.Dialect) {
			continue
		}
		if rule.CheckFile != nil {
			findings = append(findings, rule.CheckFile(file)...)
			continue
		}
		for i := range file.Statements {
			if hit, message := rule.CheckStatement(&file.Statements[i]); hit {
				findings = append(findings, Finding{
					Rule:     rule.Code,
					Title:    rule.Title,
					Severity: rule.Severity,
					File:     file.Path,
					Line:     file.Statements[i].Line,
					Message:  message,
				})
			}
		}
	}
	return findings
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
	text string
	line int
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
	for _, tok := range scanSQL(raw, mode) {
		switch tok.kind {
		case tokSemicolon:
			if start >= 0 {
				statements = append(statements, rawStatement{text: raw[start:end], line: startLine})
				start = -1
			}
		case tokWhitespace, tokComment:
			// Never starts a statement; keep end untouched so trailing
			// comments/whitespace are not included in the statement text.
		default:
			if start < 0 {
				start = tok.start
				startLine = tok.line
			}
			end = tok.end
		}
	}
	if start >= 0 {
		statements = append(statements, rawStatement{text: raw[start:end], line: startLine})
	}
	return statements
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

// parseBestEffort parses a statement into ptah's DDL AST when it falls inside
// the parser's supported subset, and returns nil otherwise.
func parseBestEffort(sql string) ast.Node {
	statements, err := parser.NewParser(sql).Parse()
	if err != nil || statements == nil || len(statements.Statements) != 1 {
		return nil
	}
	return statements.Statements[0]
}
