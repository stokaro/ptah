// Package sqllint provides a parser-backed linter for standalone SQL files.
package sqllint

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/lexer"
	"github.com/stokaro/ptah/core/parser"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/sqlutil"
)

const (
	RuleParseError             = "SQL001"
	RuleUnsupportedStatement   = "SQL002"
	RuleTableWithoutPrimaryKey = "DDL001"
	RuleUnsupportedCapability  = "CAP001"
)

type Severity string

const (
	SeverityInfo    Severity = "info"
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"
)

type Source struct {
	Name string
	SQL  string
}

type Options struct {
	Dialect       string
	Version       string
	Capabilities  capability.Capabilities
	Rules         []Rule
	DisabledRules []string
}

type Finding struct {
	Rule         string   `json:"rule"`
	Title        string   `json:"title"`
	Severity     Severity `json:"severity"`
	File         string   `json:"file,omitempty"`
	Line         int      `json:"line,omitempty"`
	Column       int      `json:"column,omitempty"`
	Dialect      string   `json:"dialect,omitempty"`
	Message      string   `json:"message"`
	Rationale    string   `json:"rationale,omitempty"`
	SuggestedFix string   `json:"suggested_fix,omitempty"`
}

type Rule interface {
	ID() string
	CheckStatement(ctx Context, stmt ast.Node) []Finding
}

type Context struct {
	Source       Source
	Dialect      string
	Version      string
	Capabilities capability.Capabilities
	statement    sourceStatement
}

func DefaultRules() []Rule {
	return []Rule{
		unsupportedRoutineRule{},
		tablePrimaryKeyRule{},
		createIndexCapabilityRule{},
	}
}

func LintSource(source Source, opts Options) ([]Finding, error) {
	var findings []Finding
	for _, statement := range splitSourceStatements(source, opts.Dialect) {
		keyword, keywordOffset := firstKeyword(statement.sql)
		if keyword == "" {
			continue
		}
		if !isSupportedTopLevelKeyword(keyword) {
			findings = append(findings, unsupportedStatementFinding(source, statement, opts, keyword, keywordOffset))
			continue
		}
		statementFindings, err := lintParsedStatement(source, statement, opts)
		if err != nil {
			return nil, err
		}
		findings = append(findings, statementFindings...)
	}
	return findings, nil
}

func effectiveCapabilities(opts Options) capability.Capabilities {
	if opts.Capabilities != nil {
		return opts.Capabilities
	}
	if opts.Version != "" {
		return capability.ForServerVersion(opts.Dialect, opts.Version)
	}
	return capability.ForDialect(opts.Dialect)
}

type sourceStatement struct {
	sql    string
	offset int
}

func splitSourceStatements(source Source, dialect string) []sourceStatement {
	statements := sqlutil.SplitSQLStatementsForDialect(source.SQL, dialect)
	out := make([]sourceStatement, 0, len(statements))
	cursor := 0
	for _, statement := range statements {
		offset := cursor
		if idx := strings.Index(source.SQL[cursor:], statement); idx >= 0 {
			offset = cursor + idx
		}
		out = append(out, sourceStatement{sql: statement, offset: offset})
		cursor = min(offset+len(statement), len(source.SQL))
	}
	return out
}

func unsupportedStatementFinding(source Source, statement sourceStatement, opts Options, keyword string, keywordOffset int) Finding {
	line, column := lineColumn(source.SQL, statement.offset+keywordOffset)
	return Finding{
		Rule:      RuleUnsupportedStatement,
		Title:     "Unsupported SQL statement",
		Severity:  SeverityError,
		File:      source.Name,
		Line:      line,
		Column:    column,
		Dialect:   opts.Dialect,
		Message:   fmt.Sprintf("ptah sql lint does not lint %s statements yet", keyword),
		Rationale: "Unsupported SQL is reported explicitly so the file cannot be mistaken for a clean lint result.",
	}
}

func lintParsedStatement(source Source, statement sourceStatement, opts Options) ([]Finding, error) {
	caps := effectiveCapabilities(opts)
	stmtList, err := parser.NewParser(statementParserSQL(statement.sql), parserOptions(opts, caps)...).Parse()
	if err != nil {
		return []Finding{parseErrorFinding(source, statement, opts, err)}, nil
	}

	ctx := Context{
		Source:       source,
		statement:    statement,
		Dialect:      opts.Dialect,
		Version:      opts.Version,
		Capabilities: caps,
	}
	findings := make([]Finding, 0)
	for _, stmt := range stmtList.Statements {
		for _, rule := range effectiveRules(opts) {
			if ruleDisabled(rule.ID(), opts.DisabledRules) {
				continue
			}
			findings = append(findings, rule.CheckStatement(ctx, stmt)...)
		}
	}
	return findings, nil
}

func parserOptions(opts Options, caps capability.Capabilities) []parser.Option {
	var parseOpts []parser.Option
	if opts.Dialect != "" {
		parseOpts = append(parseOpts, parser.WithDialect(opts.Dialect))
	}
	if caps != nil {
		parseOpts = append(parseOpts, parser.WithCapabilities(caps))
	}
	return parseOpts
}

func statementParserSQL(sql string) string {
	if strings.HasSuffix(strings.TrimSpace(sql), ";") {
		return sql
	}
	return sql + ";"
}

func effectiveRules(opts Options) []Rule {
	if opts.Rules != nil {
		return opts.Rules
	}
	return DefaultRules()
}

func ruleDisabled(ruleID string, disabled []string) bool {
	for _, item := range disabled {
		item = strings.ToUpper(strings.TrimSpace(item))
		if item == "" {
			continue
		}
		if strings.EqualFold(item, ruleID) || strings.HasPrefix(ruleID, item) {
			return true
		}
	}
	return false
}

func firstKeyword(statement string) (keyword string, offset int) {
	l := lexer.NewLexer(statement)
	for {
		token := l.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return "", 0
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		case lexer.TokenIdentifier:
			return strings.ToUpper(token.Value), token.Start
		default:
			return "", token.Start
		}
	}
}

func isSupportedTopLevelKeyword(keyword string) bool {
	switch keyword {
	case "ALTER", "COMMENT", "CREATE", "DO", "DROP", "GO":
		return true
	default:
		return false
	}
}

func parseErrorFinding(source Source, statement sourceStatement, opts Options, err error) Finding {
	line, column := lineColumn(source.SQL, statement.offset+parseErrorPosition(err))
	return Finding{
		Rule:      parseErrorRule(err),
		Title:     parseErrorTitle(err),
		Severity:  SeverityError,
		File:      source.Name,
		Line:      line,
		Column:    column,
		Dialect:   opts.Dialect,
		Message:   err.Error(),
		Rationale: "The SQL parser could not build an AST, so no rule can safely treat this statement as clean.",
	}
}

func parseErrorRule(err error) string {
	if strings.Contains(strings.ToLower(err.Error()), "unsupported") {
		return RuleUnsupportedStatement
	}
	return RuleParseError
}

func parseErrorTitle(err error) string {
	if parseErrorRule(err) == RuleUnsupportedStatement {
		return "Unsupported SQL statement"
	}
	return "SQL parse error"
}

func parseErrorPosition(err error) int {
	message := err.Error()
	idx := strings.LastIndex(message, "position ")
	if idx == -1 {
		return 0
	}
	start := idx + len("position ")
	end := start
	for end < len(message) && message[end] >= '0' && message[end] <= '9' {
		end++
	}
	pos, convErr := strconv.Atoi(message[start:end])
	if convErr != nil {
		return 0
	}
	return pos
}

type tablePrimaryKeyRule struct{}

func (tablePrimaryKeyRule) ID() string {
	return RuleTableWithoutPrimaryKey
}

func (tablePrimaryKeyRule) CheckStatement(ctx Context, stmt ast.Node) []Finding {
	table, ok := stmt.(*ast.CreateTableNode)
	if !ok {
		return nil
	}
	if table.SelectBody != "" || createTableHasPrimaryKey(table) {
		return nil
	}
	line, column := ctx.LineColumn(table.Name)
	return []Finding{{
		Rule:         RuleTableWithoutPrimaryKey,
		Title:        "Table has no primary key",
		Severity:     SeverityWarning,
		File:         ctx.Source.Name,
		Line:         line,
		Column:       column,
		Dialect:      ctx.Dialect,
		Message:      fmt.Sprintf("table %q has no primary key", table.Name),
		Rationale:    "Tables without primary keys are harder to reference, replicate, and migrate safely.",
		SuggestedFix: "Add a column-level PRIMARY KEY or a table-level PRIMARY KEY constraint.",
	}}
}

func createTableHasPrimaryKey(table *ast.CreateTableNode) bool {
	for _, column := range table.Columns {
		if column.Primary {
			return true
		}
	}
	for _, constraint := range table.Constraints {
		if constraint.Type == ast.PrimaryKeyConstraint {
			return true
		}
	}
	return false
}

type unsupportedRoutineRule struct{}

func (unsupportedRoutineRule) ID() string {
	return RuleUnsupportedStatement
}

func (unsupportedRoutineRule) CheckStatement(ctx Context, stmt ast.Node) []Finding {
	switch node := stmt.(type) {
	case *ast.RawSQLNode:
		keyword, keywordOffset := firstKeyword(node.SQL)
		if keyword == "" {
			keyword = "raw SQL"
		}
		return []Finding{ctx.unsupportedModeledSQLFinding(keyword, keywordOffset)}
	case *ast.MySQLRoutineNode:
		return []Finding{ctx.unsupportedModeledSQLFinding("CREATE "+strings.ToUpper(string(node.Kind)), 0)}
	case *ast.OpaqueRoutineNode:
		return []Finding{ctx.unsupportedModeledSQLFinding("CREATE "+strings.ToUpper(string(node.Kind)), 0)}
	case *ast.PostgresDoBlockNode:
		return []Finding{ctx.unsupportedModeledSQLFinding("DO", 0)}
	case *ast.PostgresRoutineNode:
		return []Finding{ctx.unsupportedModeledSQLFinding("CREATE "+strings.ToUpper(string(node.Kind)), 0)}
	case *ast.SQLServerRoutineNode:
		return []Finding{ctx.unsupportedModeledSQLFinding("CREATE "+strings.ToUpper(string(node.Kind)), 0)}
	case *ast.CreateFunctionNode:
		return []Finding{ctx.unsupportedModeledSQLFinding("CREATE FUNCTION", 0)}
	case *ast.CreateTriggerNode:
		return []Finding{ctx.unsupportedModeledSQLFinding("CREATE TRIGGER", 0)}
	default:
		return nil
	}
}

func (c Context) unsupportedModeledSQLFinding(label string, offset int) Finding {
	if offset < 0 {
		offset = 0
	}
	line, column := lineColumn(c.Source.SQL, c.statement.offset+offset)
	return Finding{
		Rule:      RuleUnsupportedStatement,
		Title:     "Unsupported SQL statement",
		Severity:  SeverityError,
		File:      c.Source.Name,
		Line:      line,
		Column:    column,
		Dialect:   c.Dialect,
		Message:   fmt.Sprintf("ptah sql lint does not model %s statements yet", label),
		Rationale: "This statement uses a routine or raw SQL sub-language that the SQL linter does not analyze yet.",
	}
}

type createIndexCapabilityRule struct{}

func (createIndexCapabilityRule) ID() string {
	return RuleUnsupportedCapability
}

func (createIndexCapabilityRule) CheckStatement(ctx Context, stmt ast.Node) []Finding {
	index, ok := stmt.(*ast.IndexNode)
	if !ok {
		return nil
	}
	if !index.Concurrently || ctx.Capabilities.Has(capability.CreateIndexConcurrently) {
		return nil
	}
	line, column := ctx.LineColumn(index.Name)
	return []Finding{{
		Rule:      RuleUnsupportedCapability,
		Title:     "Statement requires unsupported capability",
		Severity:  SeverityError,
		File:      ctx.Source.Name,
		Line:      line,
		Column:    column,
		Dialect:   ctx.Dialect,
		Message:   "CREATE INDEX CONCURRENTLY requires the create_index_concurrently capability",
		Rationale: "Capability-aware lint rules catch SQL that is valid for one PostgreSQL-family target but not for another.",
	}}
}

func (c Context) LineColumn(needle string) (line int, column int) {
	pos := c.statement.offset
	if idx := strings.Index(c.statement.sql, needle); idx >= 0 {
		pos += idx
	}
	return lineColumn(c.Source.SQL, pos)
}

func lineColumn(input string, pos int) (line int, column int) {
	if pos < 0 {
		pos = 0
	}
	line = 1
	column = 1
	for i, r := range input {
		if i >= pos {
			break
		}
		if r == '\n' {
			line++
			column = 1
			continue
		}
		column++
	}
	return line, column
}
