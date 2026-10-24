package provider

import (
	"fmt"
	"slices"
	"strings"
	"unicode"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// maxQuerySpelling bounds a refusal sentence or a spelling the query builder
// writes, such as an ORDER BY term.
const maxQuerySpelling = 512

// ValidateQueryDialect checks the shape of an engine's query-builder
// declaration: a placeholder style, one line of text in every spelling and
// refusal, keywords and unsupported joins that are words and not repeated,
// and capability names a host can read. A nil declaration is valid and
// declares nothing. The SDK does not own the vocabulary: the host refuses a
// placeholder, quote style, join or upsert form it does not know, rather than
// render with a substitute.
func ValidateQueryDialect(dialect *protocol.QueryDialect) error {
	if dialect == nil {
		return nil
	}
	quoting, query := dialect.GetQuoting(), dialect.GetQuery()
	for _, text := range []string{dialect.GetPlaceholder(), quoting.GetStyle(), quoting.GetBare(), query.GetUpsert()} {
		if text != "" && !queryWord(text) {
			return fmt.Errorf("%w: a query dialect value is not a word", ErrProtocol)
		}
	}
	if dialect.GetPlaceholder() == "" {
		return fmt.Errorf("%w: a query dialect needs a placeholder style", ErrProtocol)
	}
	for _, text := range []string{query.GetFetchOrderBy(), query.GetOffsetOnlyLimit(), query.GetUpsertRefusal(), query.GetUpdateRefusal()} {
		if text != "" && (!comparisonReason(text) || len(text) > maxQuerySpelling) {
			return fmt.Errorf("%w: a query dialect spelling is not one line of text", ErrProtocol)
		}
	}
	for _, words := range [][]string{quoting.GetKeywords(), query.GetUnsupportedJoins()} {
		for i, word := range words {
			if !queryWord(word) || slices.Contains(words[:i], word) {
				return fmt.Errorf("%w: a query dialect list holds an empty, spaced, or repeated word", ErrProtocol)
			}
		}
	}
	for name := range dialect.GetDefaultCapabilities() {
		if !comparisonReason(name) {
			return fmt.Errorf("%w: invalid query dialect capability name", ErrProtocol)
		}
	}
	return nil
}

// queryWord reports text with no space or control character in it.
func queryWord(text string) bool {
	return text != "" && comparisonReason(text) && !strings.ContainsFunc(text, unicode.IsSpace)
}
