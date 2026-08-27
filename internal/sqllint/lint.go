// Package sqllint provides a parser-backed linter for standalone SQL files.
package sqllint

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/internal/lexer"
	"go.5x5.cz/ptah/internal/parser"
	"go.5x5.cz/ptah/internal/servertarget"
)

const (
	RuleParseError             = "SQL001"
	RuleUnsupportedStatement   = "SQL002"
	RuleTableWithoutPrimaryKey = "DDL001"
	RuleUnsupportedCapability  = "CAP001"
	// RuleDynamicSQL marks a routine body that builds SQL at run time, which is
	// where static analysis of that routine stops.
	RuleDynamicSQL = "SQL003"
	// RuleStatementsNotAnalyzed names the statement kinds a file carried that
	// no rule examined.
	//
	// It is not [RuleUnsupportedStatement], and the difference is the whole
	// reason it exists. SQL002 says the PARSER does not model a statement, at
	// error severity; the kinds this reports are modeled exactly as intended --
	// `CREATE POLICY` has a test asserting it lints clean -- and simply have no
	// rule looking at them. Reporting them as unsupported would make SQL002's
	// own message untrue.
	RuleStatementsNotAnalyzed = "SQL004"
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
		dynamicSQLRule{},
	}
}

func LintSource(source Source, opts Options) ([]Finding, error) {
	caps, err := effectiveCapabilities(opts)
	if err != nil {
		return nil, err
	}

	var findings []Finding
	var unanalyzedKinds []string
	for _, statement := range splitSourceStatements(source, opts.Dialect) {
		keyword, keywordOffset := firstKeyword(statement.sql)
		if keyword == "" {
			continue
		}
		if !isSupportedTopLevelKeyword(keyword) {
			findings = append(findings, unsupportedStatementFinding(source, statement, opts, keyword, keywordOffset))
			continue
		}
		statementFindings, unanalyzed, err := lintParsedStatement(source, statement, opts, caps)
		if err != nil {
			return nil, err
		}
		findings = append(findings, statementFindings...)
		unanalyzedKinds = appendUnanalyzed(unanalyzedKinds, unanalyzed)
	}
	if len(unanalyzedKinds) > 0 {
		findings = append(findings, statementsNotAnalyzedFinding(source, opts, unanalyzedKinds))
	}
	return keepEnabled(findings, opts.DisabledRules), nil
}

// appendUnanalyzed adds kinds not already recorded, keeping first-seen order so
// the finding reads in the order the file does.
func appendUnanalyzed(seen, kinds []string) []string {
	for _, kind := range kinds {
		if !slices.Contains(seen, kind) {
			seen = append(seen, kind)
		}
	}
	return seen
}

// statementsNotAnalyzedFinding is the one line a file gets when it carried
// statements no rule examined.
//
// It is ONE finding rather than one per statement, and that is a decision. A
// migration is mostly statements this linter has no rule for, so per-statement
// findings would outnumber the real ones several times over and bury them --
// the failure mode of a rule meant to make incompleteness visible is being
// ignored.
//
// Info severity, for the same reason [RuleDynamicSQL] is: nothing is wrong with
// the file. What is incomplete is the analysis, and saying so is the point --
// before this, a file of views, policies and ALTER TABLEs linted clean and
// exited 0, which reads as "checked and fine" rather than "not checked"
// (stokaro/ptah#1270).
func statementsNotAnalyzedFinding(source Source, opts Options, kinds []string) Finding {
	return Finding{
		Rule:     RuleStatementsNotAnalyzed,
		Title:    "Statements no rule analyzed",
		Severity: SeverityInfo,
		File:     source.Name,
		Line:     1,
		Column:   1,
		Dialect:  opts.Dialect,
		Message:  fmt.Sprintf("no rule examined %s in this file", strings.Join(kinds, ", ")),
		Rationale: "A statement kind no rule looks at is reported so a clean result " +
			"means the file was analyzed rather than skipped.",
	}
}

// keepEnabled drops the findings a --disable selector covers.
//
// The per-rule check inside [lintParsedStatement] cannot answer for the whole
// command: SQL001 and SQL002 are produced by the parse path, which runs before
// any rule object exists, so a selector naming either was accepted and silently
// ignored -- `ptah sql lint --disable SQL001` still reported SQL001 and still
// exited 1. Filtering once, where every finding this function returns passes
// through, is what makes the flag mean the same thing for every identifier the
// linter can report.
func keepEnabled(findings []Finding, disabled []string) []Finding {
	if len(disabled) == 0 {
		return findings
	}
	kept := make([]Finding, 0, len(findings))
	for _, finding := range findings {
		if ruleDisabled(finding.Rule, disabled) {
			continue
		}
		kept = append(kept, finding)
	}
	return kept
}

// effectiveCapabilities resolves once per source rather than once per
// statement, and refuses a version string that names no server instead of
// planning with the dialect default under that version's name.
func effectiveCapabilities(opts Options) (capability.Capabilities, error) {
	if opts.Capabilities != nil {
		return opts.Capabilities, nil
	}
	target, err := servertarget.Resolve(opts.Dialect, opts.Version)
	if err != nil {
		return nil, err
	}
	return target.Capabilities, nil
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

func lintParsedStatement(
	source Source,
	statement sourceStatement,
	opts Options,
	caps capability.Capabilities,
) ([]Finding, []string, error) {
	stmtList, err := parser.NewParser(statementParserSQL(statement.sql), parserOptions(opts, caps)...).Parse()
	if err != nil {
		return []Finding{parseErrorFinding(source, statement, opts, err)}, nil, nil
	}

	ctx := Context{
		Source:       source,
		statement:    statement,
		Dialect:      opts.Dialect,
		Version:      opts.Version,
		Capabilities: caps,
	}
	findings := make([]Finding, 0)
	var unanalyzed []string
	for _, stmt := range stmtList.Statements {
		for _, rule := range effectiveRules(opts) {
			if ruleDisabled(rule.ID(), opts.DisabledRules) {
				continue
			}
			findings = append(findings, rule.CheckStatement(ctx, stmt)...)
		}
		if label, ok := unanalyzedStatementLabel(ctx, stmt); ok {
			unanalyzed = append(unanalyzed, label)
		}
	}
	return findings, unanalyzed, nil
}

// unanalyzedStatementLabel names a statement no rule looks at, and reports
// whether there is one.
//
// It asks the classification questions rather than watching for findings, and
// deliberately ignores --disable. A statement's kind is what decides whether
// this linter has anything to say about it; silencing the rule that says so
// must not manufacture a different complaint about the same statement.
func unanalyzedStatementLabel(ctx Context, stmt ast.Node) (string, bool) {
	if _, analyzed := analyzedStatementKinds[reflect.TypeOf(stmt)]; analyzed {
		return "", false
	}
	// A kind the unsupported rule already names is reported by SQL002, whose
	// message is about the parser rather than about analysis.
	if len((unsupportedRoutineRule{}).CheckStatement(ctx, stmt)) > 0 {
		return "", false
	}
	return statementLabel(stmt), true
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

// analyzedStatementKinds are the statement kinds a rule in this package
// actually examines. It is the ONLY way a statement reaches the end of a lint
// run without being reported, and the direction is the whole point: a node kind
// added to core/ast joins the reported set by default rather than the silent
// one.
//
// It was the other way around, behind a `default: return nil`, and the cost was
// measured rather than argued. On postgres, `ptah sql lint` answered
// CREATE VIEW, CREATE MATERIALIZED VIEW, CREATE POLICY, CREATE SEQUENCE,
// CREATE TYPE, ALTER TABLE and DROP TABLE with no findings and exit 0 -- seven
// statement kinds reported as a clean file when no rule had looked at them.
// #1270 asks for the opposite in those words: "No supported code object is
// silently skipped and reported as clean when analysis was incomplete."
var analyzedStatementKinds = map[reflect.Type]struct{}{
	// tablePrimaryKeyRule.
	reflect.TypeFor[*ast.CreateTableNode](): {},
	// createIndexCapabilityRule.
	reflect.TypeFor[*ast.IndexNode](): {},
}

// statementLabel names a statement kind the way SQL spells it, from the node
// type core/ast gives it.
//
// It is derived rather than mapped because a hand-written map is the thing it
// would exist to guard: a node kind added without a map entry would report an
// empty label, which reads as a finding about nothing. Consecutive capitals
// stay together, so AlterTableEnableRLSNode is ALTER TABLE ENABLE RLS and not
// ALTER TABLE ENABLE R L S.
// statementLabelOverrides name the statement kinds whose node type is not what
// SQL calls them. They are the exceptions the derivation cannot see: a
// `CREATE TYPE ... AS ENUM` parses to an EnumNode, and reporting that Ptah
// "does not model ENUM statements" names a keyword no author wrote.
var statementLabelOverrides = map[string]string{
	"EnumNode":      "CREATE TYPE",
	"ExtensionNode": "CREATE EXTENSION",
}

func statementLabel(stmt ast.Node) string {
	name := reflect.TypeOf(stmt).String()
	if index := strings.LastIndex(name, "."); index >= 0 {
		name = name[index+1:]
	}
	return statementLabelForName(name)
}

// statementLabelForName is [statementLabel] over the type's bare name, so the
// guard that reads core/ast from disk can ask about a node type without
// constructing one.
func statementLabelForName(name string) string {
	if override, ok := statementLabelOverrides[name]; ok {
		return override
	}
	name = strings.TrimSuffix(name, "Node")
	if name == "" {
		return "this statement"
	}

	var words []string
	var current []rune
	runes := []rune(name)
	for i, r := range runes {
		startsWord := i > 0 && r >= 'A' && r <= 'Z' &&
			(runes[i-1] < 'A' || runes[i-1] > 'Z' ||
				(i+1 < len(runes) && runes[i+1] >= 'a' && runes[i+1] <= 'z'))
		if startsWord && len(current) > 0 {
			words = append(words, string(current))
			current = current[:0]
		}
		current = append(current, r)
	}
	if len(current) > 0 {
		words = append(words, string(current))
	}
	return strings.ToUpper(strings.Join(words, " "))
}

// postgresRoutineStatements is the parsed body of whichever node a PostgreSQL
// routine arrived as, with a label naming it for the finding.
//
// There are two, and the difference is not about what was parsed. A PROCEDURE
// becomes a PostgresRoutineNode carrying its body; a FUNCTION becomes a
// CreateFunctionNode, and `Parser.attachPostgresFunctionBody` fills its
// RoutineBody with the same parsed statements. Reading only the first node type
// left every function unexamined while looking like a parser limitation.
func postgresRoutineStatements(stmt ast.Node) (label string, statements []ast.PostgresRoutineStatement) {
	switch node := stmt.(type) {
	case *ast.PostgresRoutineNode:
		return fmt.Sprintf("%s %s", node.Kind, node.Name), node.Body.Statements
	case *ast.CreateFunctionNode:
		if node.RoutineBody == nil {
			return "", nil
		}
		kind := node.Kind
		if kind == "" {
			kind = "function"
		}
		return fmt.Sprintf("%s %s", kind, node.Name), node.RoutineBody.Statements
	}
	return "", nil
}

// dynamicSQLRule reports where a routine builds SQL at run time.
//
// This is the first thing in Ptah that READS a parsed routine body. The parser
// has been producing `ast.PostgresRoutineBody.Statements` -- a statement list
// with a typed Kind -- and nothing consumed it, which is what made every
// schema-aware rule in stokaro/ptah#1270 look unreachable. It is not: PL/pgSQL's
// EXECUTE is already its own Kind, so "analysis stops here" is decidable from
// what the parser hands over, with no new parsing at all.
//
// It is the boundary #1270 asks to surface in its own words -- "does dynamic
// SQL introduce an analysis boundary that should be surfaced?" -- and it is
// INFO rather than a warning because a routine that composes SQL is doing
// something legitimate. What the finding reports is the limit of what any later
// rule can claim about that routine, not a defect in it.
//
// One finding per EXECUTE rather than one per routine: each is a separate place
// the analysis stops, and each gets its own position.
//
// PostgreSQL only, because it is the only frontend with an execute Kind --
// MySQL and SQL Server routine statements have no such classification, so their
// dynamic SQL is indistinguishable from any other raw statement here.
type dynamicSQLRule struct{}

func (dynamicSQLRule) ID() string {
	return RuleDynamicSQL
}

func (dynamicSQLRule) CheckStatement(ctx Context, stmt ast.Node) []Finding {
	label, statements := postgresRoutineStatements(stmt)
	if len(statements) == 0 {
		return nil
	}

	var findings []Finding
	for _, statement := range statements {
		if statement.Kind != ast.PostgresRoutineStatementExecute {
			continue
		}
		// The statement carries no offset of its own -- it is a Kind and its
		// raw text -- so the text is what locates it. A body running the same
		// EXECUTE twice reports the first position twice, which is a worse
		// position rather than a wrong claim.
		line, column := ctx.LineColumn(statement.SQL)
		findings = append(findings, Finding{
			Rule:      RuleDynamicSQL,
			Title:     "Dynamic SQL limits static analysis",
			Severity:  SeverityInfo,
			File:      ctx.Source.Name,
			Line:      line,
			Column:    column,
			Dialect:   ctx.Dialect,
			Message:   fmt.Sprintf("%s builds SQL at run time with EXECUTE", label),
			Rationale: "The statement text is not known until the routine runs, so nothing here can resolve what it reads or writes.",
		})
	}
	return findings
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
		Rule:     RuleUnsupportedCapability,
		Title:    "Statement requires unsupported capability",
		Severity: SeverityError,
		File:     ctx.Source.Name,
		Line:     line,
		Column:   column,
		Dialect:  ctx.Dialect,
		// The key is read off the constant the gate above tested. A retyped
		// literal is a second spelling of the same identifier, free to drift
		// from the one Capabilities.Has answers for, and the finding would
		// then name a capability no preset carries.
		Message:   fmt.Sprintf("CREATE INDEX CONCURRENTLY requires target capability %s, unavailable on this target", capability.CreateIndexConcurrently),
		Rationale: "Capability-aware lint rules catch SQL that is valid for one PostgreSQL-family target but not for another.",
	}}
}

func (c Context) LineColumn(needle string) (line, column int) {
	pos := c.statement.offset
	if idx := strings.Index(c.statement.sql, needle); idx >= 0 {
		pos += idx
	}
	return lineColumn(c.Source.SQL, pos)
}

func lineColumn(input string, pos int) (line, column int) {
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
