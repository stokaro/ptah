package provider

import (
	"context"
	"fmt"
	"slices"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ScanSQLFeature enables ScanSQL, by which the owner returns the tokens of one
// text of its engine's SQL. The provider and each engine that scans list it.
// A host that does not know the RPC never calls it, so it is not a two-way
// feature. Contract 25.26.
const ScanSQLFeature = "sql.scan"

// SQLScanner scans SQL text offline.
type SQLScanner interface {
	// ScanSQL reads the request's text alone and returns its tokens in order.
	// The tokens cover the text with no gap and no overlap. A STRING carries
	// the text it stands for and an IDENTIFIER the name it stands for, where
	// the scanner reads one. It opens nothing and runs nothing.
	ScanSQL(context.Context, *protocol.ScanSQLRequest) (*protocol.SQLScan, error)
}

func sqlScanError(reason string) error {
	return fmt.Errorf("%w: SQL scan: %s", ErrProtocol, reason)
}

// ValidateScanSQLRequest refuses a request for an engine the provider does
// not own or that does not list ScanSQLFeature, and one the description does
// not list the feature for. An empty text is valid; its scan has no token.
func ValidateScanSQLRequest(description *protocol.Description, request *protocol.ScanSQLRequest) error {
	if request == nil {
		return sqlScanError("the request is missing")
	}
	if !slices.Contains(description.GetFeatures(), ScanSQLFeature) {
		return sqlScanError("the provider does not scan SQL")
	}
	index := slices.IndexFunc(description.GetEngines(), func(engine *protocol.Engine) bool { return engine.GetId() == request.GetEngine() })
	if request.GetEngine() == "" || index < 0 {
		return sqlScanError("the request names no engine this provider owns")
	}
	if !slices.Contains(description.GetEngines()[index].GetFeatures(), ScanSQLFeature) {
		return sqlScanError("the engine does not scan SQL")
	}
	if !utf8.ValidString(request.GetText()) {
		return sqlScanError("the text is not UTF-8")
	}
	return nil
}

// ValidateSQLScan checks a reply against its request. Each token has a known
// kind and is not empty, ends on a UTF-8 boundary of the text, and starts
// where the one before it ends; the last ends at the end of the text. A value
// appears only on a STRING or an IDENTIFIER.
func ValidateSQLScan(description *protocol.Description, request *protocol.ScanSQLRequest, result *protocol.SQLScan) error {
	if err := ValidateScanSQLRequest(description, request); err != nil {
		return err
	}
	if result == nil {
		return sqlScanError("the reply is missing")
	}
	text := request.GetText()
	start := 0
	for _, token := range result.GetTokens() {
		if err := validateSQLToken(text, start, token); err != nil {
			return err
		}
		start = int(token.GetEnd())
	}
	if start != len(text) {
		return sqlScanError("the tokens do not reach the end of the text")
	}
	return nil
}

func validateSQLToken(text string, start int, token *protocol.SQLToken) error {
	switch token.GetKind() {
	case protocol.SQLToken_UNKNOWN, protocol.SQLToken_COMMENT, protocol.SQLToken_SEMICOLON, protocol.SQLToken_WHITESPACE, protocol.SQLToken_OPERATOR:
		if token.Value != nil {
			return sqlScanError("a value is on a token that is neither a string nor an identifier")
		}
	case protocol.SQLToken_STRING, protocol.SQLToken_IDENTIFIER:
	default:
		return sqlScanError("a token has no known kind")
	}
	end := int(token.GetEnd())
	if end <= start {
		return sqlScanError("a token is empty or ends before it starts")
	}
	if end > len(text) {
		return sqlScanError("a token ends past the end of the text")
	}
	if end < len(text) && !utf8.RuneStart(text[end]) {
		return sqlScanError("a token ends inside a UTF-8 sequence")
	}
	return nil
}

// validateSQLScanEngine checks that an engine that lists ScanSQLFeature has
// the provider's.
func validateSQLScanEngine(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(engine.GetFeatures(), ScanSQLFeature) && !slices.Contains(description.GetFeatures(), ScanSQLFeature) {
		return fmt.Errorf("%w: engine %s requires the provider's", ErrProtocol, ScanSQLFeature)
	}
	return nil
}
