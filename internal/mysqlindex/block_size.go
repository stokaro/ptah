package mysqlindex

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// KeepsBlockSize reports whether this table retains an index block-size hint.
// MariaDB prints it on every table; MySQL drops it outside compressed tables.
// The reader and comparator use the same rule so an ignored hint cannot
// cause a rebuild on every apply (stokaro/ptah#3962).
func KeepsBlockSize(dialect, rowFormat string) bool {
	return platform.NormalizeDialect(dialect) == platform.MariaDB || strings.EqualFold(rowFormat, "Compressed")
}

// BlockSizes reads the index KEY_BLOCK_SIZE options from SHOW CREATE TABLE.
// It scans table elements rather than parsing the whole statement: a catalog
// table can contain unrelated clauses that the desired-schema reader refuses.
// Strings, quoted identifiers, nested expressions, and table options cannot
// contribute an index option. The primary key is returned as PRIMARY.
func BlockSizes(ddl string) (map[string]uint64, error) {
	scan := lexer.NewLexerWithOptions(ddl, dialectlexer.Options(platform.MySQL))
	sizes := make(map[string]uint64)
	var element []lexer.Token
	depth := 0
	for {
		tok := scan.NextToken()
		if tok.Type == lexer.TokenEOF {
			break
		}
		if tok.Type == lexer.TokenWhitespace || tok.Type == lexer.TokenComment {
			continue
		}
		switch {
		case tok.MatchOperatorValue("("):
			depth++
			if depth == 1 {
				continue
			}
		case tok.MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return sizes, recordBlockSize(sizes, element)
			}
		case tok.MatchOperatorValue(",") && depth == 1:
			if err := recordBlockSize(sizes, element); err != nil {
				return nil, err
			}
			element = nil
			continue
		}
		if depth > 0 {
			element = append(element, tok)
		}
	}
	return nil, fmt.Errorf("SHOW CREATE TABLE has no complete table body")
}

func recordBlockSize(sizes map[string]uint64, tokens []lexer.Token) error {
	name, rest := indexOptions(tokens)
	if name == "" {
		return nil
	}
	for i, tok := range rest {
		if !tok.MatchIdentifierValue("KEY_BLOCK_SIZE") {
			continue
		}
		value := rest[i+1:]
		if len(value) > 0 && value[0].MatchOperatorValue("=") {
			value = value[1:]
		}
		if len(value) == 0 {
			return fmt.Errorf("index %q: missing KEY_BLOCK_SIZE value", name)
		}
		size, err := strconv.ParseUint(value[0].Value, 10, 64)
		if err != nil {
			return fmt.Errorf("index %q: invalid KEY_BLOCK_SIZE: %w", name, err)
		}
		sizes[name] = size
	}
	return nil
}

// indexOptions returns only tokens following the outer key-parts parentheses.
func indexOptions(tokens []lexer.Token) (string, []lexer.Token) {
	if len(tokens) < 3 {
		return "", nil
	}
	first := strings.ToUpper(tokens[0].Value)
	if first == "UNIQUE" || first == "FULLTEXT" || first == "SPATIAL" {
		tokens = tokens[1:]
	}
	name := ""
	switch strings.ToUpper(tokens[0].Value) {
	case "PRIMARY":
		name = "PRIMARY"
	case "KEY", "INDEX":
		name = strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(tokens[1].Value, "`"), "`"), "``", "`")
	default:
		return "", nil
	}
	depth := 0
	for i, tok := range tokens {
		if tok.MatchOperatorValue("(") {
			depth++
		}
		if tok.MatchOperatorValue(")") {
			depth--
			if depth == 0 {
				return name, tokens[i+1:]
			}
		}
	}
	return "", nil
}

// ValidateBlockSize refuses values the server truncates instead of preserving.
// Measured on MariaDB 11.8.9 and MySQL 8.4.11: 65536 becomes 0 on MariaDB;
// 4294967296 becomes 0 on MySQL. Comparing either would rebuild indefinitely.
// An unspecified dialect permits the larger MySQL range; rendering resolves it.
func ValidateBlockSize(dialect string, size uint64) error {
	limit := uint64(4294967295)
	if platform.NormalizeDialect(dialect) == platform.MariaDB {
		limit = 65535
	}
	if size > limit {
		return fmt.Errorf("KEY_BLOCK_SIZE %d exceeds the %s limit %d; the server would truncate it", size, dialect, limit)
	}
	return nil
}
