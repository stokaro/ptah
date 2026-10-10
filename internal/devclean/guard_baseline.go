package devclean

import (
	"errors"
	"path"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
	"ptah.run/internal/ydburl"
)

// BaselineGuard holds the baseline a plan rehearsal writes, to recreate the
// target's current schema in the dev database, to the realm a migration replay
// on the same dev server is held to.
//
// The baseline is DDL Ptah derives from the target, but deriving a statement
// from the target does not confine its effects: a routine or a trigger body
// can write outside the dev database when it runs, a comment on an extension
// is not restored by the cleanup, and a role or a grant outlives it. So every
// refusal of [NewDevReplayGuard] holds, and a server the run owns lifts the
// same refusals it lifts for a replay.
//
// One statement is accepted that a replay refuses: a YDB GRANT or REVOKE whose
// every path is absolute and inside the dev realm, on a connection to a dev
// realm. See [isYDBRealmGrant].
type BaselineGuard struct {
	replay *ReplayGuard
	// ydbRealmRoot is the absolute path of the YDB dev realm the connection
	// names, or "" when it names none.
	ydbRealmRoot string
}

// NewBaselineGuard is the guard for the baseline of a plan rehearsal on the
// dev database reached through info.
func NewBaselineGuard(info catalog.ServerInfo) *BaselineGuard {
	guard := &BaselineGuard{replay: NewDevReplayGuard(info)}
	if platform.NormalizeDialect(info.Dialect) != platform.YDB {
		return guard
	}
	if parsed, err := ydburl.Parse(info.URL); err == nil && parsed.Realm != "" {
		guard.ydbRealmRoot = parsed.Root()
	}
	return guard
}

// ValidateStatement refuses a baseline statement whose effects cannot be
// confined to the dev database's realm. The refusal says it is the
// baseline's, since a reader of a migration replay's refusal would look for a
// migration file, and it ends with the remedy when owning the server would
// lift it.
func (g *BaselineGuard) ValidateStatement(stmt string) error {
	if isYDBRealmGrant(stmt, g.ydbRealmRoot) {
		return nil
	}
	err := g.replay.validate(stmt, g.replay.realm)
	if err == nil {
		return nil
	}
	if unsafe, ok := errors.AsType[*unsafeStatementError](err); ok {
		refusal := *unsafe
		refusal.baseline = true
		err = &refusal
	}
	return g.replay.withRemedy(stmt, err)
}

// isYDBRealmGrant reports a single GRANT or REVOKE that names only the dev
// realm whose absolute path is root, or paths under it, by their absolute
// paths. The realm's removal takes such a permission with it.
//
// The baseline needs it because the realm's root stands in for the target's
// database: the baseline recreates the permissions the target holds on its
// root, such as ydb.access.grant for ACCESS-ADMINS on /local, by granting
// them on the realm's root. YDB resolves a relative grant path against the
// database root, never against the realm's prefix (see
// ptah.run/internal/ydbacl.StatementPath), so only an absolute path under root
// is known to be inside.
//
// The statement is read whole, in the shape the YDB renderer writes: GRANT,
// one or more quoted permission names, ON, one or more backticked absolute
// paths, TO, and one or more principals, with FROM for a REVOKE, and at most a
// final semicolon. Anything else is left to the replay guard, which refuses
// every grant in a realm: a second statement, WITH GRANT OPTION, a `$` name, a
// relative path, a path that is not in its cleaned form, and a translation
// setting.
func isYDBRealmGrant(stmt, root string) bool {
	if root == "" || root == "/" || !path.IsAbs(root) || path.Clean(root) != root {
		return false
	}
	tokens := ydbStatementTokens(stmt)
	var principals string
	switch ydbKeywordAt(tokens, 0) {
	case "GRANT":
		principals = "TO"
	case "REVOKE":
		principals = "FROM"
	default:
		return false
	}
	rest, ok := ydbList(tokens[1:], func(token lexer.Token) bool { return token.Type == lexer.TokenString })
	if !ok || ydbKeywordAt(rest, 0) != "ON" {
		return false
	}
	rest, ok = ydbList(rest[1:], func(token lexer.Token) bool { return ydbPathInRealm(token, root) })
	if !ok || ydbKeywordAt(rest, 0) != principals {
		return false
	}
	rest, ok = ydbList(rest[1:], func(token lexer.Token) bool {
		return token.Type == lexer.TokenIdentifier && !strings.HasPrefix(token.Value, "$")
	})
	return ok && len(rest) == 0
}

// ydbStatementTokens is the statement without whitespace, comments and a final
// semicolon. Any other semicolon is kept, so a second statement reads as more
// tokens than the grammar allows.
func ydbStatementTokens(stmt string) []lexer.Token {
	lex := lexer.NewLexerWithOptions(stmt, replayLexerOptions(platform.YDB))
	var tokens []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		case lexer.TokenEOF:
			if n := len(tokens); n > 0 && tokens[n-1].Type == lexer.TokenSemicolon {
				tokens = tokens[:n-1]
			}
			return tokens
		default:
			tokens = append(tokens, token)
		}
	}
}

// ydbList reads `item [, item]...` from the start of tokens and returns what
// follows it, or false when tokens does not start with an item.
func ydbList(tokens []lexer.Token, item func(lexer.Token) bool) ([]lexer.Token, bool) {
	for i := 0; i < len(tokens); i += 2 {
		if !item(tokens[i]) {
			return nil, false
		}
		if i+1 >= len(tokens) || !tokens[i+1].MatchOperatorValue(",") {
			return tokens[i+1:], true
		}
	}
	return nil, false
}

// ydbPathInRealm reports a backticked absolute path, written in its cleaned
// form, that is root or lies under it.
func ydbPathInRealm(token lexer.Token, root string) bool {
	if token.Type != lexer.TokenIdentifier || !strings.HasPrefix(token.Value, "`") {
		return false
	}
	target := ydbUnquote(token.Value)
	// A cleaned path has no `.` or `..` segment, so it cannot climb out.
	if !path.IsAbs(target) || path.Clean(target) != target {
		return false
	}
	return target == root || strings.HasPrefix(target, root+"/")
}
