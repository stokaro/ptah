package postgresreplay

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// transactionScopedLockRefusal reads a LOCK statement and returns the reason it
// is refused, or "" when it is accepted.
//
// LOCK TABLE takes a lock that ends with the transaction it runs in, and
// PostgreSQL refuses it outside a transaction block: measured on 15.19,
// `LOCK TABLE t IN SHARE ROW EXCLUSIVE MODE` answers "LOCK TABLE can only be
// used in transaction blocks", and inside BEGIN ... COMMIT it succeeds and the
// lock is gone at COMMIT. A history written to serialize two writers with it
// therefore leaves nothing behind on the replay session, the same reason SET
// LOCAL is accepted ([postgresTransactionScopedSetting]).
//
// The form read is `LOCK [TABLE] [ONLY] name [*] [, ...] [IN mode MODE]
// [NOWAIT]`. A name in a namespace the server owns is refused, as a mutation
// of one is, because a lock on a catalog can stall the server. A form this does
// not read is refused with the other session statements.
func (p protection) transactionScopedLockRefusal(tokens []lexer.Token) string {
	const unread = "LOCK session or transaction state"
	if p.dialect != platform.Postgres {
		return unread
	}
	at := 1
	if replay.TokenSequenceAt(tokens, at, "TABLE") {
		at++
	}
	if replay.TokenSequenceAt(tokens, at, "ONLY") {
		at++
	}
	at, namespace, ok := p.lockTargets(tokens, at)
	switch {
	case namespace != "":
		return `LOCK of protected namespace "` + namespace + `"`
	case !ok || !lockTailIsRead(tokens, at):
		return unread
	}
	return ""
}

// lockTargets reads the comma-separated table names that follow LOCK [TABLE]
// [ONLY]. It returns the position after them, the first protected namespace a
// qualified name uses, and whether at least one name was read.
func (p protection) lockTargets(tokens []lexer.Token, at int) (int, string, bool) {
	names := 0
	for at < len(tokens) {
		if tokens[at].Type != lexer.TokenIdentifier && !isQuotedIdentifier(tokens[at]) {
			return at, "", false
		}
		names++
		qualified := at+1 < len(tokens) && tokens[at+1].MatchOperatorValue(".")
		if qualified && p.isProtectedPostgresNamespace(replay.IdentifierValue(tokens[at])) {
			return at, replay.IdentifierValue(tokens[at]), true
		}
		for at+1 < len(tokens) && tokens[at+1].MatchOperatorValue(".") {
			at += 2
		}
		at++
		if at < len(tokens) && tokens[at].MatchOperatorValue("*") {
			at++
		}
		if at >= len(tokens) || !tokens[at].MatchOperatorValue(",") {
			break
		}
		at++
	}
	return at, "", names > 0
}

// lockTailIsRead reports whether what follows the table names is no more than
// an optional `IN mode MODE` and an optional NOWAIT.
func lockTailIsRead(tokens []lexer.Token, at int) bool {
	if replay.TokenSequenceAt(tokens, at, "IN") {
		width := lockModeWidth(tokens, at+1)
		if width == 0 || !replay.TokenSequenceAt(tokens, at+1+width, "MODE") {
			return false
		}
		at += 2 + width
	}
	if replay.TokenSequenceAt(tokens, at, "NOWAIT") {
		at++
	}
	return at == len(tokens)
}

// lockModeWidth returns the number of words of the lock mode at index, or 0.
func lockModeWidth(tokens []lexer.Token, index int) int {
	for _, mode := range [][]string{
		{"ACCESS", "SHARE"}, {"ROW", "SHARE"}, {"ROW", "EXCLUSIVE"},
		{"SHARE", "UPDATE", "EXCLUSIVE"}, {"SHARE", "ROW", "EXCLUSIVE"},
		{"ACCESS", "EXCLUSIVE"}, {"SHARE"}, {"EXCLUSIVE"},
	} {
		if replay.TokenSequenceAt(tokens, index, mode...) {
			return len(mode)
		}
	}
	return 0
}

// isQuotedIdentifier reports a double-quoted identifier token.
func isQuotedIdentifier(token lexer.Token) bool {
	return token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`)
}
