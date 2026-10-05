package sqllint

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/yqlddl"
)

// lintYQL lints a YDB source.
//
// The rules above read a statement through internal/parser, which has no YQL
// grammar (stokaro/ptah#4015), and YQL reads a double-quoted "x" as a string
// where those rules read a name. So a YDB source is read through
// internal/yqlddl instead, which `ptah migrations lint` reads YQL through too,
// and judged by the rules that reading can answer:
//
//   - DDL001, because YDB refuses a table without a primary key. Measured on
//     26.2.1.14 and 25.1.4.7: `Primary key is required for ydb tables.`
//   - CAP001, for the capabilities [yqlddl.Statement.Requirements] says a
//     CREATE TABLE or an ALTER TABLE needs, against the target's capability
//     set.
//
// SQL002 reports a statement this linter does not lint, as it does on every
// dialect, and SQL004 the CREATE, ALTER and DROP kinds no rule examined. No
// statement reaches the parser, so SQL001 and the parser-backed rules --
// DDL002 and SQL003 -- report nothing here.
func lintYQL(source Source, opts Options, caps capability.Capabilities) []Finding {
	var findings []Finding
	var unanalyzed []string
	for _, statement := range splitSourceStatements(source, opts.Dialect) {
		keyword, offset := yqlFirstKeyword(statement.sql)
		switch keyword {
		case "":
			continue
		case "CREATE", "ALTER", "DROP":
		default:
			findings = append(findings, unsupportedStatementFinding(source, statement, opts, yqlStatementLabel(keyword), offset))
			continue
		}
		read := yqlddl.Read(statement.sql)
		at := statement.offset + offset
		switch read.Kind {
		case yqlddl.CreateTable:
			if !read.PrimaryKey {
				findings = append(findings, yqlMissingKeyFinding(source, opts, read, at))
			}
			findings = append(findings, yqlCapabilityFindings(source, opts, caps, read, at)...)
		case yqlddl.AlterTable:
			findings = append(findings, yqlCapabilityFindings(source, opts, caps, read, at)...)
		default:
			unanalyzed = appendUnanalyzed(unanalyzed, []string{yqlKindLabel(statement.sql)})
		}
	}
	if len(unanalyzed) > 0 {
		findings = append(findings, statementsNotAnalyzedFinding(source, opts, unanalyzed))
	}
	return findings
}

// yqlFirstKeyword returns a statement's first word in upper case and its
// offset, read by the YQL lexer, or the empty string for a statement with no
// word at its head.
func yqlFirstKeyword(statement string) (string, int) {
	lexr := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.YDB))
	for {
		token := lexr.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return "", 0
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenUnknown:
			continue
		case lexer.TokenIdentifier:
			return strings.ToUpper(token.Value), token.Start
		default:
			return "", token.Start
		}
	}
}

// yqlStatementLabel names a statement SQL002 reports. A statement that opens
// with a named expression assigns it, and is named for what it is rather than
// for the name.
func yqlStatementLabel(keyword string) string {
	if strings.HasPrefix(keyword, "$") {
		return "named expression"
	}
	return keyword
}

// yqlKindLabel names a CREATE, ALTER or DROP statement by its first two words,
// for SQL004.
func yqlKindLabel(statement string) string {
	lexr := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.YDB))
	var words []string
	for len(words) < 2 {
		token := lexr.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return strings.Join(words, " ")
		case lexer.TokenIdentifier:
			words = append(words, strings.ToUpper(token.Value))
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenUnknown:
		default:
			return strings.Join(words, " ")
		}
	}
	return strings.Join(words, " ")
}

func yqlMissingKeyFinding(source Source, opts Options, read yqlddl.Statement, at int) Finding {
	line, column := lineColumn(source.SQL, at)
	return Finding{
		Rule:         RuleTableWithoutPrimaryKey,
		Title:        "Table has no primary key",
		Severity:     SeverityWarning,
		File:         source.Name,
		Line:         line,
		Column:       column,
		Dialect:      opts.Dialect,
		Message:      fmt.Sprintf("table %q has no primary key, which YDB refuses (Primary key is required for ydb tables)", read.Name),
		Rationale:    "YDB stores every table by its primary key and refuses a CREATE TABLE that declares none.",
		SuggestedFix: "Add a table-level PRIMARY KEY (...) clause; YQL has no column-level PRIMARY KEY.",
	}
}

func yqlCapabilityFindings(source Source, opts Options, caps capability.Capabilities, read yqlddl.Statement, at int) []Finding {
	var findings []Finding
	for _, requirement := range read.Requirements() {
		if caps.Has(requirement.Capability) {
			continue
		}
		line, column := lineColumn(source.SQL, at)
		findings = append(findings, Finding{
			Rule:     RuleUnsupportedCapability,
			Title:    "Statement requires unsupported capability",
			Severity: SeverityError,
			File:     source.Name,
			Line:     line,
			Column:   column,
			Dialect:  opts.Dialect,
			Message: fmt.Sprintf("%s requires target capability %s, unavailable on this target",
				yqlRequirementSubject(read, requirement), requirement.Capability),
			Rationale: "Capability-aware lint rules catch SQL that one YDB release line or cluster accepts and another refuses.",
		})
	}
	return findings
}

// yqlRequirementSubject names the index or the action a requirement belongs
// to, for a message.
func yqlRequirementSubject(read yqlddl.Statement, requirement yqlddl.Requirement) string {
	if requirement.Inline {
		return fmt.Sprintf("INDEX %s, a vector index of table %s,", read.Indexes[requirement.Action].Name, read.Name)
	}
	action := read.Actions[requirement.Action]
	switch {
	case action.Kind == yqlddl.AddIndex && action.Index.Vector():
		return fmt.Sprintf("ADD INDEX %s, a vector index added to table %s,", action.Index.Name, read.Name)
	case action.Kind == yqlddl.AddIndex:
		return fmt.Sprintf("ADD INDEX %s, a unique index added to the existing table %s,", action.Index.Name, read.Name)
	case action.Kind == yqlddl.AddColumn:
		return fmt.Sprintf("ADD COLUMN %s with a default on table %s", action.Column.Name, read.Name)
	default:
		return "ALTER TABLE " + read.Name
	}
}
