package backend

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// LintRules are the rules the fixture's migration lint publishes. FX101 judges
// statements; FX102 judges the target a check names, so a test reads back what
// the host sent.
func LintRules() []*protocol.LintRule {
	return []*protocol.LintRule{
		{Code: "FX101", Title: "DROP TABLE loses rows", Severity: protocol.LintRule_ERROR},
		{Code: "FX102", Title: "target the check names", Severity: protocol.LintRule_INFO},
	}
}

// Linter is the fixture's migration lint: a statement ends at a semicolon and
// carries its words, a DROP TABLE loses rows, and FX102 reports the server
// version and the capabilities a check carries.
type Linter struct{}

// splitLint cuts text after every semicolon, the whole fixture dialect.
func splitLint(text string) []*protocol.LintStatement {
	var statements []*protocol.LintStatement
	start := 0
	for start < len(text) {
		end := strings.IndexByte(text[start:], ';')
		if end < 0 {
			break
		}
		end += start + 1
		first := start + len(text[start:]) - len(strings.TrimLeft(text[start:], " \n"))
		words := lintWords(text[first:end])
		statements = append(statements, &protocol.LintStatement{
			Start:       lintOffset(first),
			End:         lintOffset(end),
			Line:        lintOffset(1 + strings.Count(text[:first], "\n")),
			Sql:         text[first:end],
			Words:       upper(words),
			SourceWords: words,
		})
		start = end
	}
	return statements
}

// lintWords tokenizes a statement the way the host's rules read it: words,
// and every punctuation character but the closing semicolon as an entry of
// its own. The fixture dialect has no comment and no quoting.
func lintWords(statement string) []string {
	var words []string
	word := strings.Builder{}
	flush := func() {
		if word.Len() > 0 {
			words = append(words, word.String())
			word.Reset()
		}
	}
	for _, char := range strings.TrimSuffix(statement, ";") {
		switch {
		case char == '_' || char >= '0' && char <= '9' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z':
			word.WriteRune(char)
		case char == ' ' || char == '\t' || char == '\n' || char == '\r':
			flush()
		default:
			flush()
			words = append(words, string(char))
		}
	}
	flush()
	return words
}

func upper(words []string) []string {
	result := make([]string, len(words))
	for i, word := range words {
		result[i] = strings.ToUpper(word)
	}
	return result
}

func dropsTable(statement *protocol.LintStatement) bool {
	return strings.HasPrefix(strings.ToUpper(statement.Sql), "DROP TABLE")
}

// SplitLintStatements splits each source after every semicolon and marks a
// DROP TABLE as a drop that loses state.
func (Linter) SplitLintStatements(ctx context.Context, request *protocol.SplitLintRequest) (*protocol.SplitLint, error) {
	result := &protocol.SplitLint{Judged: []protocol.LintFact{protocol.LintFact_LINT_FACT_STATEFUL_DROP}}
	for _, source := range request.Sources {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		file := &protocol.LintFile{Name: source.Name, Version: source.Version, Up: source.Up, Down: source.Down, Statements: splitLint(source.Sql)}
		for _, statement := range file.Statements {
			if dropsTable(statement) {
				statement.Effects = []*protocol.LintEffect{{Fact: protocol.LintFact_LINT_FACT_STATEFUL_DROP, Kind: "table"}}
			}
		}
		result.Files = append(result.Files, file)
	}
	return result, nil
}

// AnnotateLintStatements marks a DROP TABLE the host split as a drop that
// loses state.
func (Linter) AnnotateLintStatements(ctx context.Context, request *protocol.AnnotateLintRequest) (*protocol.AnnotatedLint, error) {
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

// CheckLintMigrations reports FX101 on every DROP TABLE and FX102 once per
// run, on the first file, each only where the request enables it.
func (Linter) CheckLintMigrations(ctx context.Context, request *protocol.CheckLintRequest) (*protocol.CheckedLint, error) {
	result := &protocol.CheckedLint{Complete: true}
	if slices.Contains(request.Codes, "FX102") && len(request.Files) > 0 {
		result.Findings = append(result.Findings, &protocol.LintFinding{Code: "FX102", File: 0, Statement: -1, Message: checkedTarget(request)})
	}
	for i, file := range request.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for j, statement := range file.Statements {
			if slices.Contains(request.Codes, "FX101") && dropsTable(statement) {
				result.Findings = append(result.Findings, &protocol.LintFinding{Code: "FX101", File: lintPosition(i), Statement: lintPosition(j), Message: "DROP TABLE removes every row"})
			}
		}
	}
	return result, nil
}

// checkedTarget spells the target a check names: the engine, the server
// version, and every capability with its answer, in key order.
func checkedTarget(request *protocol.CheckLintRequest) string {
	keys := make([]string, 0, len(request.Capabilities))
	for key := range request.Capabilities {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%t", key, request.Capabilities[key]))
	}
	return fmt.Sprintf("checked against %s server %q with %d capabilities: %s", request.Engine, request.ServerVersion, len(keys), strings.Join(pairs, " "))
}

// lintOffset converts an offset or count that is never negative and never above
// the size of the request.
func lintOffset(n int) uint32 {
	return uint32(min(max(int64(n), 0), math.MaxUint32)) // #nosec G115 -- clamped into range on this line
}

// lintPosition converts the position of a file or a statement in a request.
func lintPosition(n int) int32 {
	return int32(min(max(int64(n), 0), math.MaxInt32)) // #nosec G115 -- clamped into range on this line
}
