package main

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// ScanSQL reads the text by the fixture's rules. head changes nothing: the
// fixture's text has no settings ahead of a query.
func (*scanningProvider) ScanSQL(ctx context.Context, request *protocol.ScanSQLRequest) (*protocol.SQLScan, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request.GetEngine() != engine {
		return nil, status.Error(codes.InvalidArgument, "the fixture scans only its own engine")
	}
	return &protocol.SQLScan{Tokens: scan(request.GetText())}, nil
}

// scan returns every token of text.
func scan(text string) []*protocol.SQLToken {
	var tokens []*protocol.SQLToken
	for pos := 0; pos < len(text); {
		kind, end := next(text, pos)
		token := &protocol.SQLToken{Kind: kind, End: uint32(end)} // #nosec G115 -- the SDK bounds a request below 4 MiB
		if value, ok := valueOf(kind, text[pos:end]); ok {
			token.Value = &value
		}
		tokens = append(tokens, token)
		pos = end
	}
	return tokens
}

// next reads the token at pos and returns its kind and end.
func next(text string, pos int) (protocol.SQLToken_Kind, int) {
	ch, size := utf8.DecodeRuneInString(text[pos:])
	rest := text[pos:]
	switch {
	case unicode.IsSpace(ch):
		return protocol.SQLToken_WHITESPACE, pos + runEnd(rest, unicode.IsSpace)
	case strings.HasPrefix(rest, "--"):
		return protocol.SQLToken_COMMENT, pos + lineEnd(rest)
	case strings.HasPrefix(rest, "/*"):
		return protocol.SQLToken_COMMENT, pos + closeEnd(rest, 2, "*/")
	case strings.HasPrefix(rest, "@@"):
		return protocol.SQLToken_STRING, pos + multilineEnd(rest)
	case ch == '\'' || ch == '"':
		return protocol.SQLToken_STRING, pos + quotedEnd(rest, byte(ch))
	case ch == '`':
		return protocol.SQLToken_IDENTIFIER, pos + backtickEnd(rest)
	case ch == ';':
		return protocol.SQLToken_SEMICOLON, pos + 1
	case ch == '_' || unicode.IsLetter(ch) || unicode.IsDigit(ch):
		return protocol.SQLToken_IDENTIFIER, pos + runEnd(rest, isWord)
	default:
		return protocol.SQLToken_OPERATOR, pos + size
	}
}

func isWord(ch rune) bool { return ch == '_' || unicode.IsLetter(ch) || unicode.IsDigit(ch) }

// runEnd is the end of the run of characters accept takes at the start of text.
func runEnd(text string, accept func(rune) bool) int {
	end := strings.IndexFunc(text, func(ch rune) bool { return !accept(ch) })
	if end < 0 {
		return len(text)
	}
	return end
}

func lineEnd(text string) int {
	if end := strings.IndexByte(text, '\n'); end >= 0 {
		return end
	}
	return len(text)
}

// closeEnd is the end of a token that closes with closing after from, or the
// end of the text when nothing closes it.
func closeEnd(text string, from int, closing string) int {
	if end := strings.Index(text[from:], closing); end >= 0 {
		return from + end + len(closing)
	}
	return len(text)
}

// quotedEnd is the end of a string in quote, where a backslash escapes the
// next character.
func quotedEnd(text string, quote byte) int {
	for i := 1; i < len(text); i++ {
		switch text[i] {
		case '\\':
			i++
		case quote:
			return i + 1
		}
	}
	return len(text)
}

// multilineEnd is the end of an @@...@@ string, in which @@@@ stands for @@.
func multilineEnd(text string) int {
	for i := 2; i < len(text)-1; i++ {
		if text[i] != '@' || text[i+1] != '@' {
			continue
		}
		if strings.HasPrefix(text[i:], "@@@@") {
			i += 3
			continue
		}
		return i + 2
	}
	return len(text)
}

// backtickEnd is the end of a backticked name, in which a doubled backtick
// stands for one.
func backtickEnd(text string) int {
	for i := 1; i < len(text); i++ {
		if text[i] != '`' {
			continue
		}
		if i+1 < len(text) && text[i+1] == '`' {
			i++
			continue
		}
		return i + 1
	}
	return len(text)
}

// valueOf is what a string or an identifier stands for, and whether the
// token is a whole one.
func valueOf(kind protocol.SQLToken_Kind, token string) (string, bool) {
	switch {
	case kind == protocol.SQLToken_IDENTIFIER && strings.HasPrefix(token, "`"):
		if len(token) < 2 || !strings.HasSuffix(token, "`") {
			return "", false
		}
		return strings.ReplaceAll(token[1:len(token)-1], "``", "`"), true
	case kind == protocol.SQLToken_IDENTIFIER:
		return token, true
	case kind == protocol.SQLToken_STRING && strings.HasPrefix(token, "@@"):
		if len(token) < 4 || !strings.HasSuffix(token, "@@") {
			return "", false
		}
		return strings.ReplaceAll(token[2:len(token)-2], "@@@@", "@@"), true
	case kind == protocol.SQLToken_STRING:
		if len(token) < 2 || token[len(token)-1] != token[0] {
			return "", false
		}
		return token[1 : len(token)-1], true
	default:
		return "", false
	}
}
