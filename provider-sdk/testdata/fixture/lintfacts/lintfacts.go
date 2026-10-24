// Package lintfacts is the fixture's migration lint with the statement facts
// of contract 25.1: what each statement is, a table created without a primary
// key, and the capability a parameterized DECIMAL needs. It splits as the
// fixture's contract 25.0 lint does and adds the facts only for a host that
// lists lint.statement-facts, which is how a provider stays correct against an
// older host.
package lintfacts

import (
	"context"
	"slices"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// DecimalCapability is the capability key a column of type DECIMAL(p,s)
// needs, the key Ptah's YDB release lines answer.
const DecimalCapability = "parameterized_decimal"

// Linter judges statement facts over the fixture's split.
type Linter struct{ backend.Linter }

// statementFacts are the facts Linter judges beside the fixture's own.
var statementFacts = []protocol.LintFact{
	protocol.LintFact_LINT_FACT_STATEMENT_KIND, protocol.LintFact_LINT_FACT_MISSING_PRIMARY_KEY, protocol.LintFact_LINT_FACT_REQUIRED_CAPABILITY,
}

// SplitLintStatements splits as the fixture does and, for a host that lists
// lint.statement-facts, judges every statement's facts.
func (l Linter) SplitLintStatements(ctx context.Context, request *protocol.SplitLintRequest) (*protocol.SplitLint, error) {
	result, err := l.Linter.SplitLintStatements(ctx, request)
	if err != nil || !slices.Contains(provider.HostFeatures(ctx), provider.LintStatementFactsFeature) {
		return result, err
	}
	result.Judged = append(result.Judged, statementFacts...)
	for _, file := range result.Files {
		for _, statement := range file.Statements {
			statement.Effects = append(statement.Effects, Facts(statement.Words, statement.SourceWords)...)
		}
	}
	return result, nil
}

// Facts are the statement facts of one statement, read from its words: the
// split's uppercased words and the words as written.
func Facts(words, written []string) []*protocol.LintEffect {
	head := strings.Join(words[:min(len(words), 2)], " ")
	switch {
	case len(words) == 0:
		return []*protocol.LintEffect{kind(protocol.LintStatementClass_LINT_STATEMENT_CLASS_OTHER, "empty statement", "")}
	case head == "CREATE TABLE" && len(words) > 2:
		table := written[2]
		effects := []*protocol.LintEffect{kind(protocol.LintStatementClass_LINT_STATEMENT_CLASS_CREATE_TABLE, head, table)}
		if !hasPrimaryKey(words) {
			effects = append(effects, &protocol.LintEffect{Fact: protocol.LintFact_LINT_FACT_MISSING_PRIMARY_KEY, Table: table})
		}
		return append(effects, decimals(words, written, table)...)
	case head == "ALTER TABLE" && len(words) > 2:
		table := written[2]
		effects := []*protocol.LintEffect{kind(protocol.LintStatementClass_LINT_STATEMENT_CLASS_ALTER_TABLE, head, table)}
		return append(effects, decimals(words, written, table)...)
	case slices.Contains([]string{"CREATE", "ALTER", "DROP"}, words[0]) && len(words) > 1:
		return []*protocol.LintEffect{kind(protocol.LintStatementClass_LINT_STATEMENT_CLASS_SCHEMA, head, object(written))}
	default:
		return []*protocol.LintEffect{kind(protocol.LintStatementClass_LINT_STATEMENT_CLASS_OTHER, words[0], "")}
	}
}

func kind(class protocol.LintStatementClass, name, target string) *protocol.LintEffect {
	return &protocol.LintEffect{Fact: protocol.LintFact_LINT_FACT_STATEMENT_KIND, StatementClass: class, Kind: name, Object: target}
}

// object is the name a schema statement acts on, the word after its kind.
func object(written []string) string {
	if len(written) > 2 {
		return written[2]
	}
	return ""
}

func hasPrimaryKey(words []string) bool {
	for i := range len(words) - 1 {
		if words[i] == "PRIMARY" && words[i+1] == "KEY" {
			return true
		}
	}
	return false
}

// decimals are the requirements of every column whose type is DECIMAL with
// a precision, named by the word before the type.
func decimals(words, written []string, table string) []*protocol.LintEffect {
	var effects []*protocol.LintEffect
	for i := 1; i+1 < len(words); i++ {
		if words[i] == "DECIMAL" && words[i+1] == "(" {
			effects = append(effects, &protocol.LintEffect{Fact: protocol.LintFact_LINT_FACT_REQUIRED_CAPABILITY, Capability: DecimalCapability,
				Kind: "COLUMN", Object: written[i-1], Table: table})
		}
	}
	return effects
}
