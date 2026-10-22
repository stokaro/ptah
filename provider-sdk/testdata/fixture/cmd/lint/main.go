// Package main exercises provider-owned migration lint through an independent process.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-lint"
	description.Features = append(description.Features, provider.LintFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.LintFeature)
	engine.LintRules = []*protocol.LintRule{{Code: "FX101", Title: "DROP TABLE loses rows", Severity: protocol.LintRule_ERROR}}
	owner := &linter{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, Linter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type linter struct {
	*backend.Provider
}

// split cuts text after every semicolon, the whole fixture dialect.
func split(text string) []*protocol.LintStatement {
	var statements []*protocol.LintStatement
	start := 0
	for start < len(text) {
		end := strings.IndexByte(text[start:], ';')
		if end < 0 {
			break
		}
		end += start + 1
		first := start + len(text[start:]) - len(strings.TrimLeft(text[start:], " \n"))
		statements = append(statements, &protocol.LintStatement{Start: uint32(first), End: uint32(end), Line: uint32(1 + strings.Count(text[:first], "\n")), Sql: text[first:end]})
		start = end
	}
	return statements
}

func dropsTable(statement *protocol.LintStatement) bool {
	return strings.HasPrefix(strings.ToUpper(statement.Sql), "DROP TABLE")
}

func (*linter) SplitLintStatements(ctx context.Context, request *protocol.SplitLintRequest) (*protocol.SplitLint, error) {
	result := &protocol.SplitLint{Judged: []protocol.LintFact{protocol.LintFact_LINT_FACT_STATEFUL_DROP}}
	for _, source := range request.Sources {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		file := &protocol.LintFile{Name: source.Name, Version: source.Version, Up: source.Up, Down: source.Down, Statements: split(source.Sql)}
		for _, statement := range file.Statements {
			if dropsTable(statement) {
				statement.Effects = []*protocol.LintEffect{{Fact: protocol.LintFact_LINT_FACT_STATEFUL_DROP, Kind: "table"}}
			}
		}
		result.Files = append(result.Files, file)
	}
	return result, nil
}

func (*linter) AnnotateLintStatements(ctx context.Context, request *protocol.AnnotateLintRequest) (*protocol.AnnotatedLint, error) {
	result := &protocol.AnnotatedLint{}
	for _, file := range request.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		annotated := &protocol.LintAnnotations{}
		for _, statement := range file.Statements {
			annotation := &protocol.LintAnnotation{}
			if dropsTable(statement) {
				annotation.Effects = []*protocol.LintEffect{{Fact: protocol.LintFact_LINT_FACT_STATEFUL_DROP, Kind: "table"}}
			}
			annotated.Statements = append(annotated.Statements, annotation)
		}
		result.Files = append(result.Files, annotated)
	}
	return result, nil
}

func (*linter) CheckLintMigrations(ctx context.Context, request *protocol.CheckLintRequest) (*protocol.CheckedLint, error) {
	result := &protocol.CheckedLint{Complete: true}
	for i, file := range request.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for j, statement := range file.Statements {
			if dropsTable(statement) {
				result.Findings = append(result.Findings, &protocol.LintFinding{Code: "FX101", File: int32(i), Statement: int32(j), Message: "DROP TABLE removes every row"})
			}
		}
	}
	return result, nil
}
