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
// Each *.up.sql file is split into statements with ptah's SQL lexer (so
// dollar-quoted bodies and comments never confuse the splitter) and every
// statement is checked in two complementary forms:
//
//   - a canonical text form — comments stripped, whitespace collapsed,
//     keywords uppercased — that pattern rules match against, which keeps the
//     rules robust for statements outside the DDL subset ptah's parser
//     understands (DROP TABLE, ALTER TYPE, ...);
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
	"slices"
	"sort"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/lexer"
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
	// Name is the bare file name inside the linted directory.
	Name string
	// IsUp reports whether this is an up migration (*.up.sql).
	IsUp bool
	// HasPair reports whether the matching counterpart file (down for up,
	// up for down) exists in the same directory.
	HasPair bool
	// WellFormedName reports whether the name matches the migrator's
	// NNNNNNNNNN_description.(up|down).sql convention.
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
	// Dialect gates dialect-specific rules: "postgres" enables PG rules,
	// "mysql"/"mariadb" enable MY rules. Empty runs every rule — maximum
	// visibility when the target is unknown.
	Dialect string
	// Disabled lists rule codes or code prefixes to skip: "DS101" disables
	// one rule, "DS" the whole data-safety family.
	Disabled []string
	// PathPrefix is prepended (with /) to file names in findings so they
	// point at real repository paths in CI annotations.
	PathPrefix string
}

// LintFS lints every *.sql file at the root of fsys and returns the findings
// ordered by file, line and rule code.
func LintFS(fsys fs.FS, opts Options) ([]Finding, error) {
	names, err := fs.Glob(fsys, "*.sql")
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

	var findings []Finding
	for _, name := range names {
		file, err := prepareFile(fsys, name, present, opts.PathPrefix)
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
func prepareFile(fsys fs.FS, name string, present map[string]struct{}, pathPrefix string) (*File, error) {
	raw, err := fs.ReadFile(fsys, name)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", name, err)
	}

	file := &File{
		Path:           path.Join(pathPrefix, name),
		Name:           name,
		IsUp:           strings.HasSuffix(name, ".up.sql"),
		WellFormedName: migrator.ValidateMigrationFileName(name),
	}
	switch {
	case file.IsUp:
		_, file.HasPair = present[strings.TrimSuffix(name, ".up.sql")+".down.sql"]
	case strings.HasSuffix(name, ".down.sql"):
		_, file.HasPair = present[strings.TrimSuffix(name, ".down.sql")+".up.sql"]
	}

	// Statement rules apply to up migrations only.
	if file.IsUp {
		for _, rawStmt := range splitStatementsWithLines(string(raw)) {
			file.Statements = append(file.Statements, Statement{
				SQL:       rawStmt.text,
				Canonical: canonicalize(rawStmt.text),
				Words:     tokenizeWords(rawStmt.text),
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

// rawStatement is one raw SQL statement plus the line it starts on.
type rawStatement struct {
	text string
	line int
}

// splitStatementsWithLines splits SQL into statements using the lexer (so
// semicolons inside strings, comments and dollar-quoted bodies do not
// terminate statements) while tracking each statement's starting line.
func splitStatementsWithLines(raw string) []rawStatement {
	lexr := lexer.NewLexer(raw)
	var statements []rawStatement
	start := -1
	end := 0
	for {
		tok := lexr.NextToken()
		if tok.Type == lexer.TokenEOF {
			break
		}
		switch tok.Type {
		case lexer.TokenSemicolon:
			if start >= 0 {
				statements = append(statements, rawStatement{
					text: raw[start:end],
					line: 1 + strings.Count(raw[:start], "\n"),
				})
				start = -1
			}
		case lexer.TokenWhitespace, lexer.TokenComment:
			// Never starts a statement; keep end untouched so trailing
			// comments/whitespace are not included in the statement text.
		default:
			if start < 0 {
				start = tok.Start
			}
			end = tok.End
		}
	}
	if start >= 0 {
		statements = append(statements, rawStatement{
			text: raw[start:end],
			line: 1 + strings.Count(raw[:start], "\n"),
		})
	}
	return statements
}

// canonicalize renders a statement in the form pattern rules match against:
// comments removed, every whitespace run collapsed to one space, everything
// except string literals uppercased.
func canonicalize(sql string) string {
	lexr := lexer.NewLexer(sql)
	var b strings.Builder
	pendingSpace := false
	for {
		tok := lexr.NextToken()
		if tok.Type == lexer.TokenEOF {
			break
		}
		switch tok.Type {
		case lexer.TokenComment, lexer.TokenWhitespace:
			// A comment separates tokens exactly like whitespace does
			// (DROP/*x*/TABLE must not canonicalize to DROPTABLE).
			pendingSpace = b.Len() > 0
		case lexer.TokenString:
			if pendingSpace {
				b.WriteByte(' ')
				pendingSpace = false
			}
			b.WriteString(tok.Value)
		default:
			if pendingSpace {
				b.WriteByte(' ')
				pendingSpace = false
			}
			b.WriteString(strings.ToUpper(tok.Value))
		}
	}
	return b.String()
}

// tokenizeWords renders a statement as the word sequence rules scan (see
// Statement.Words). Double-quoted identifiers arrive from the lexer as string
// tokens and backtick-quoted ones as identifiers; both keep their quotes and
// case, so quoted names never collide with SQL keywords in the scans.
func tokenizeWords(sql string) []string {
	lexr := lexer.NewLexer(sql)
	var words []string
	for {
		tok := lexr.NextToken()
		if tok.Type == lexer.TokenEOF {
			break
		}
		switch tok.Type {
		case lexer.TokenComment, lexer.TokenWhitespace:
			continue
		case lexer.TokenString:
			words = append(words, tok.Value)
		default:
			if strings.HasPrefix(tok.Value, "`") || strings.HasPrefix(tok.Value, `"`) {
				words = append(words, tok.Value)
				continue
			}
			words = append(words, strings.ToUpper(tok.Value))
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
