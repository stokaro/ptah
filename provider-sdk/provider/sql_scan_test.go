package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// scanText is a statement with a quoted name, a string and a two-byte
// character inside the string.
const scanText = "SELECT `a b` FROM t WHERE x = 'é';"

// scanDescription lists SQL scanning for the provider and its engine.
func scanDescription() *protocol.Description {
	d := description()
	d.Features = append(d.Features, provider.ScanSQLFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ScanSQLFeature)
	return d
}

func scanRequest(text string) *protocol.ScanSQLRequest {
	return &protocol.ScanSQLRequest{Engine: description().Engines[0].Id, Text: text, Head: true}
}

func token(kind protocol.SQLToken_Kind, end uint32) *protocol.SQLToken {
	return &protocol.SQLToken{Kind: kind, End: end}
}

func valued(kind protocol.SQLToken_Kind, end uint32, value string) *protocol.SQLToken {
	return &protocol.SQLToken{Kind: kind, End: end, Value: &value}
}

// scanTokens is every token of scanText. The string ends after the two
// bytes of é.
func scanTokens() []*protocol.SQLToken {
	return []*protocol.SQLToken{
		valued(protocol.SQLToken_IDENTIFIER, 6, "SELECT"), token(protocol.SQLToken_WHITESPACE, 7),
		valued(protocol.SQLToken_IDENTIFIER, 12, "a b"), token(protocol.SQLToken_WHITESPACE, 13),
		token(protocol.SQLToken_IDENTIFIER, 17), token(protocol.SQLToken_WHITESPACE, 18),
		token(protocol.SQLToken_IDENTIFIER, 19), token(protocol.SQLToken_WHITESPACE, 20),
		token(protocol.SQLToken_IDENTIFIER, 25), token(protocol.SQLToken_WHITESPACE, 26),
		token(protocol.SQLToken_IDENTIFIER, 27), token(protocol.SQLToken_WHITESPACE, 28),
		token(protocol.SQLToken_OPERATOR, 29), token(protocol.SQLToken_WHITESPACE, 30),
		valued(protocol.SQLToken_STRING, 34, "é"), token(protocol.SQLToken_SEMICOLON, 35),
	}
}

func TestScanSQL_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		text   string
		tokens []*protocol.SQLToken
	}{
		{"every token of a statement", scanText, scanTokens()},
		{"an empty text has no token", "", nil},
		{"a string the scanner reads no value of", "'\\q'", []*protocol.SQLToken{token(protocol.SQLToken_STRING, 4)}},
		{"a translation setting the server reads", "--!syntax_v1\n", []*protocol.SQLToken{token(protocol.SQLToken_UNKNOWN, 12), token(protocol.SQLToken_WHITESPACE, 13)}},
		{"a comment and a separator", "/* x */;", []*protocol.SQLToken{token(protocol.SQLToken_COMMENT, 7), token(protocol.SQLToken_SEMICOLON, 8)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := scanDescription()
			request := scanRequest(test.text)
			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			c.Assert(provider.ValidateScanSQLRequest(d, request), qt.IsNil)
			c.Assert(provider.ValidateSQLScan(d, request, &protocol.SQLScan{Tokens: test.tokens}), qt.IsNil)
		})
	}
}

func TestScanSQLRequest_FailurePath(t *testing.T) {
	withoutProvider := scanDescription()
	withoutProvider.Features = description().Features
	withoutEngine := scanDescription()
	withoutEngine.Engines[0].Features = description().Engines[0].Features
	for _, test := range []struct {
		name        string
		description *protocol.Description
		request     *protocol.ScanSQLRequest
		want        string
	}{
		{"a provider that does not scan", withoutProvider, scanRequest(scanText), "the provider does not scan SQL"},
		{"an engine that does not scan", withoutEngine, scanRequest(scanText), "the engine does not scan SQL"},
		{"an engine the provider does not own", scanDescription(), &protocol.ScanSQLRequest{Engine: "other", Text: scanText},
			"the request names no engine this provider owns"},
		{"no engine", scanDescription(), &protocol.ScanSQLRequest{Text: scanText}, "the request names no engine this provider owns"},
		{"a text that is not UTF-8", scanDescription(), scanRequest("SELECT '\xff'"), "the text is not UTF-8"},
		{"no request", scanDescription(), nil, "the request is missing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.ValidateScanSQLRequest(test.description, test.request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, errorEndsWith("SQL scan: "+test.want))
		})
	}
}

func TestSQLScan_FailurePath(t *testing.T) {
	short := scanTokens()[:len(scanTokens())-1]
	gap := scanTokens()
	gap[1] = token(protocol.SQLToken_WHITESPACE, 6)
	past := append(scanTokens(), token(protocol.SQLToken_WHITESPACE, 36))
	inside := []*protocol.SQLToken{token(protocol.SQLToken_STRING, 2), token(protocol.SQLToken_STRING, 4)}
	unknown := scanTokens()
	unknown[0] = token(protocol.SQLToken_Kind(99), 6)
	unspecified := scanTokens()
	unspecified[0] = token(protocol.SQLToken_KIND_UNSPECIFIED, 6)
	misplaced := scanTokens()
	misplaced[1] = valued(protocol.SQLToken_WHITESPACE, 7, " ")
	for _, test := range []struct {
		name   string
		text   string
		result *protocol.SQLScan
		want   string
	}{
		{"no reply", scanText, nil, "the reply is missing"},
		{"tokens short of the end", scanText, &protocol.SQLScan{Tokens: short}, "the tokens do not reach the end of the text"},
		{"no token for a text", scanText, &protocol.SQLScan{}, "the tokens do not reach the end of the text"},
		{"an empty token", scanText, &protocol.SQLScan{Tokens: gap}, "a token is empty or ends before it starts"},
		{"a token past the end", scanText, &protocol.SQLScan{Tokens: past}, "a token ends past the end of the text"},
		{"a token inside a character", "'é'", &protocol.SQLScan{Tokens: inside}, "a token ends inside a UTF-8 sequence"},
		{"a kind the contract does not name", scanText, &protocol.SQLScan{Tokens: unknown}, "a token has no known kind"},
		{"no kind", scanText, &protocol.SQLScan{Tokens: unspecified}, "a token has no known kind"},
		{"a value on whitespace", scanText, &protocol.SQLScan{Tokens: misplaced}, "a value is on a token that is neither a string nor an identifier"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.ValidateSQLScan(scanDescription(), scanRequest(test.text), test.result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, errorEndsWith("SQL scan: "+test.want))
		})
	}
}

// An engine scans only through the provider's scanner.
func TestScanSQLEngine_FailurePath(t *testing.T) {
	c := qt.New(t)
	d := scanDescription()
	d.Features = description().Features
	err := provider.ValidateDescription(d, nil)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, errorEndsWith("engine sql.scan requires the provider's"))
}
