package backend

import (
	"context"
	"fmt"
	"math"
	"strings"
	"unicode"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// AnalyzeSQL tokenizes the fixture's deliberately narrow SQL grammar. It
// refuses comments, expressions, and native statements; it never guesses that
// unknown SQL is a harmless common operation.
func (*Provider) AnalyzeSQL(ctx context.Context, request *protocol.AnalyzeSQLRequest) (*protocol.SQLAnalysis, error) {
	if request.GetEngine() != "fixture" {
		return nil, fmt.Errorf("unsupported fixture engine")
	}
	result := &protocol.SQLAnalysis{Complete: true}
	for _, source := range request.GetSources() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		file, err := analyzeSource(source)
		if err != nil {
			return nil, err
		}
		result.Files = append(result.Files, file)
	}
	return result, nil
}

func analyzeSource(source string) (*protocol.SQLFile, error) {
	file := &protocol.SQLFile{}
	start := -1
	var words, original []string
	for i := 0; i < len(source); {
		if isSpace(source[i]) {
			i++
			continue
		}
		if start < 0 {
			start = i
		}
		if source[i] == ';' {
			statement, err := analyzedStatement(source, start, i+1, words, original)
			if err != nil {
				return nil, err
			}
			file.Statements = append(file.Statements, statement)
			start = -1
			words = nil
			original = nil
			i++
			continue
		}
		end, err := tokenEnd(source, i)
		if err != nil {
			return nil, err
		}
		word := source[i:end]
		original = append(original, word)
		if word[0] != '"' {
			word = strings.ToUpper(word)
		}
		words = append(words, word)
		i = end
	}
	if start >= 0 {
		statement, err := analyzedStatement(source, start, len(source), words, original)
		if err != nil {
			return nil, err
		}
		file.Statements = append(file.Statements, statement)
	}
	return file, nil
}

func isSpace(b byte) bool     { return b == ' ' || b == '\t' || b == '\r' || b == '\n' }
func quoted(word string) bool { return len(word) >= 2 && word[0] == '"' && word[len(word)-1] == '"' }

func analyzedStatement(source string, start, end int, words, original []string) (*protocol.SQLStatement, error) {
	if !commonStatement(words) {
		return nil, fmt.Errorf("unsupported fixture SQL grammar")
	}
	line := 1 + strings.Count(source[:start], "\n")
	if start < 0 || start > math.MaxUint32 || end < 0 || end > math.MaxUint32 || line < 0 || line > math.MaxUint32 {
		return nil, fmt.Errorf("fixture SQL positions exceed the protocol limit")
	}
	return &protocol.SQLStatement{Start: uint32(start), End: uint32(end), Line: uint32(line), Words: words, SourceWords: original, Canonical: strings.Join(words, " "), Safety: protocol.SQLStatement_COMMON_DDL}, nil
}

func commonStatement(words []string) bool {
	if len(words) < 3 || words[1] != "TABLE" || !quoted(words[2]) {
		return false
	}
	switch words[0] {
	case "DROP":
		return len(words) == 3
	case "ALTER":
		if len(words) >= 6 && words[3] == "ADD" && words[4] == "COLUMN" {
			return commonColumnWords(words[5:])
		}
		return len(words) == 6 && words[3] == "DROP" && words[4] == "COLUMN" && quoted(words[5])
	case "CREATE":
		if len(words) < 7 || words[3] != "(" || words[len(words)-1] != ")" {
			return false
		}
		columns := words[4 : len(words)-1]
		start := 0
		for i, word := range columns {
			if word == "," {
				if !commonColumnWords(columns[start:i]) {
					return false
				}
				start = i + 1
			}
		}
		return commonColumnWords(columns[start:])
	default:
		return false
	}
}

func commonColumnWords(words []string) bool {
	if len(words) < 2 || !quoted(words[0]) {
		return false
	}
	switch words[1] {
	case "INTEGER", "TEXT", "REAL", "BLOB":
	default:
		return false
	}
	rest := words[2:]
	if len(rest) >= 2 && rest[0] == "NOT" && rest[1] == "NULL" {
		rest = rest[2:]
	}
	if len(rest) >= 2 && rest[0] == "PRIMARY" && rest[1] == "KEY" {
		rest = rest[2:]
	}
	return len(rest) == 0
}

func tokenEnd(source string, i int) (int, error) {
	end := i + 1
	switch {
	case source[i] == '"':
		closed := false
		for end < len(source) {
			if source[end] == '"' {
				end++
				if end < len(source) && source[end] == '"' {
					end++
					continue
				}
				closed = true
				break
			}
			end++
		}
		if !closed {
			return 0, fmt.Errorf("unclosed fixture identifier")
		}
	case strings.ContainsRune("(),", rune(source[i])):
	case source[i] >= 'A' && source[i] <= 'Z' || source[i] >= 'a' && source[i] <= 'z':
		for end < len(source) && unicode.IsLetter(rune(source[end])) {
			end++
		}
	default:
		return 0, fmt.Errorf("unsupported fixture SQL token")
	}
	return end, nil
}
