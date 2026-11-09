package backend

import (
	"context"
	"math"
	"strings"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// AnalyzeSQL splits each source into statements and says what each one is.
// Ptah reads SQL through it wherever it judges a statement of the engine,
// such as the data-safety check before a plan runs. Every byte of a source is
// part of a statement or white space between statements, and a statement the
// engine does not know refuses the whole request.
func (*Provider) AnalyzeSQL(ctx context.Context, request *protocol.AnalyzeSQLRequest) (*protocol.SQLAnalysis, error) {
	if request.GetEngine() != Engine {
		return nil, refuse("the provider analyzes the SQL of the tutorial engine only")
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

// analyzeSource splits one source at each semicolon outside a quoted name.
func analyzeSource(source string) (*protocol.SQLFile, error) {
	if uint64(len(source)) > math.MaxUint32 {
		return nil, refuse("a SQL source is larger than the protocol can address")
	}
	file := &protocol.SQLFile{}
	start := -1
	var words []string
	for position := 0; position < len(source); {
		character := source[position]
		if strings.IndexByte(" \t\r\n", character) >= 0 {
			position++
			continue
		}
		if start < 0 {
			start = position
		}
		if character == ';' {
			statement, err := analyzedStatement(source, start, position+1, words)
			if err != nil {
				return nil, err
			}
			file.Statements = append(file.Statements, statement)
			start, words = -1, nil
			position++
			continue
		}
		end, err := tokenEnd(source, position)
		if err != nil {
			return nil, err
		}
		words = append(words, source[position:end])
		position = end
	}
	if start >= 0 {
		statement, err := analyzedStatement(source, start, len(source), words)
		if err != nil {
			return nil, err
		}
		file.Statements = append(file.Statements, statement)
	}
	return file, nil
}

// tokenEnd returns where the token that starts at position ends: a quoted
// name, one punctuation character or a word.
func tokenEnd(source string, position int) (int, error) {
	character := source[position]
	switch {
	case character == '"':
		for end := position + 1; end < len(source); end++ {
			if source[end] != '"' {
				continue
			}
			if end+1 < len(source) && source[end+1] == '"' {
				end++
				continue
			}
			return end + 1, nil
		}
		return 0, refuse("a quoted name is not closed")
	case strings.IndexByte("(),", character) >= 0:
		return position + 1, nil
	case isLetter(character):
		end := position + 1
		for end < len(source) && (isLetter(source[end]) || source[end] == '_' || source[end] >= '0' && source[end] <= '9') {
			end++
		}
		return end, nil
	default:
		return 0, refuse("the tutorial engine's SQL has no comments, literals or operators")
	}
}

func isLetter(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
}

// analyzedStatement describes the statement that spans source[start:end].
// The engine knows the four statements Render writes. Each is common DDL:
// Ptah judges what a common statement does to the schema, and a DROP is
// destructive in its judgment, not in the provider's.
func analyzedStatement(source string, start, end int, words []string) (*protocol.SQLStatement, error) {
	canonical := make([]string, len(words))
	for i, word := range words {
		canonical[i] = word
		if !strings.HasPrefix(word, `"`) {
			canonical[i] = strings.ToUpper(word)
		}
	}
	if !knownStatement(canonical) {
		return nil, refuse("the tutorial engine runs CREATE TABLE, DROP TABLE and ALTER TABLE ADD COLUMN or DROP COLUMN only")
	}
	line := 1 + strings.Count(source[:start], "\n")
	return &protocol.SQLStatement{
		Start:       uint32(start), // #nosec G115 -- analyzeSource refuses a source longer than math.MaxUint32
		End:         uint32(end),   // #nosec G115 -- analyzeSource refuses a source longer than math.MaxUint32
		Line:        uint32(line),  // #nosec G115 -- a line number is at most the source length
		Words:       canonical,
		SourceWords: words,
		Canonical:   strings.Join(canonical, " "),
		Safety:      protocol.SQLStatement_COMMON_DDL,
	}, nil
}

// knownStatement reports whether the words start one of the statements the
// engine runs.
func knownStatement(words []string) bool {
	if len(words) < 3 || words[1] != "TABLE" || !strings.HasPrefix(words[2], `"`) {
		return false
	}
	switch words[0] {
	case "CREATE", "DROP":
		return true
	case "ALTER":
		return len(words) >= 6 && (words[3] == "ADD" || words[3] == "DROP") && words[4] == "COLUMN"
	default:
		return false
	}
}
