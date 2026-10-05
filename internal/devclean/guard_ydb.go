package devclean

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// A YDB dev database is a root a connection treats as its database: a dev
// realm, which is a directory Ptah created in a database the operator named,
// or a whole database on a server the run owns. The connection sends `PRAGMA
// TablePathPrefix` with the realm's path before every query, so a relative
// path resolves inside the realm. What the pragma does not confine was
// measured on 26.2.1.14 and 25.1.4.7, each statement sent after the pragma:
//
//	CREATE TABLE `../escape` (...)          created beside the realm
//	CREATE TABLE `/local/abs` (...)         created at the database root
//	ALTER TABLE t RENAME TO `../renamed`    moved the table out of the realm
//	$p = "/local/outside"; INSERT INTO $p   wrote a row outside the realm
//	PRAGMA TablePathPrefix = "/local"; ...  resolved names at the database root
//	CREATE USER u1 PASSWORD "p"             created a user of the database
//	GRANT SELECT ON t TO u1                 refused: a grant takes no prefix
//	CREATE TOPIC tp                         created in the realm
//	CREATE SECRET s WITH (value = 'v')      created in the realm (26.2.1.14)
//
// Ptah's own coordination node statement (see internal/ydbcoordination) is
// resolved by Ptah's connection rather than the server, against the same
// prefix and with the same rule: a relative path lands in the realm, and the
// connection refuses one that leaves it.
//
// So a statement whose effect is not confined to the realm is refused: a
// write whose target is an absolute path, climbs out with `..`, is named
// through a `$` expression or carries a cluster, a pragma that moves the
// prefix, a statement that runs code it computes, and every object the whole
// database shares -- users, groups, permissions, the deprecated `OBJECT ...
// (TYPE SECRET)`, which belongs to the user rather than to a path, resource
// pools, backups. A topic, and a secret made with CREATE SECRET, is judged by
// its path like a table, since the realm's reset drops it. A statement that
// reads outside the realm is left alone, since a read leaves nothing behind.
//
// On a server the run owns, the realm is the server, so only what reaches past
// it stays refused: an external data source or table, async replication, a
// transfer and a streaming query read from or write to somewhere else. In a
// realm the prefix would not confine a replication or a transfer either:
// measured on 26.2.1.14, it prefixes a replication's own path and its
// replica's, and a transfer's own path, while a replication's FOR and a
// transfer's FROM and TO resolve at the database root (`Path does not exist`
// for a table of the realm), and a transfer created under the prefix stops at
// once, since YDB compiles its lambda under it (`Invalid table name
// "/local/<realm>/Input": prefix must be "Input"`).
//
// A statement the guard does not recognize is refused, as an unknown
// ClickHouse engine is.

// validateYDBReplayStatement is [ReplayGuard.ValidateStatement] for YDB.
func validateYDBReplayStatement(tokens []lexer.Token, realm ReplayRealm) error {
	if len(tokens) == 0 {
		return nil
	}
	if tokens[0].Type == lexer.TokenUnknown && strings.HasPrefix(tokens[0].Value, "--!") {
		// A translation setting changes how the rest of the text is read,
		// which the reading below cannot follow.
		return unsafeReplayStatement(platform.YDB, "translation setting "+tokens[0].Value)
	}
	if operation := ydbBeyondTheServer(tokens); operation != "" {
		return unsafeReplayStatement(platform.YDB, operation)
	}
	if realm == ReplayRealmServer {
		return nil
	}
	return validateYDBRealmStatement(tokens)
}

// ydbBeyondTheServer names the operation of a statement that reads from or
// writes to something outside the server, or "". An external data source is
// refused wherever the statement names one, since a table's TTL moves rows to
// one through `TO EXTERNAL DATA SOURCE`.
func ydbBeyondTheServer(tokens []lexer.Token) string {
	if ydbContainsKeywords(tokens, "EXTERNAL", "DATA", "SOURCE") {
		return "an external data source"
	}
	switch ydbKeyword(tokens[0]) {
	case "CREATE", "ALTER", "DROP":
	default:
		return ""
	}
	kind := ydbObjectKindIndex(tokens)
	switch {
	case ydbKeywordAt(tokens, kind) == "EXTERNAL" && ydbKeywordAt(tokens, kind+1) == "TABLE":
		return "an external table"
	case ydbKeywordAt(tokens, kind) == "ASYNC" && ydbKeywordAt(tokens, kind+1) == "REPLICATION":
		return "async replication"
	case ydbKeywordAt(tokens, kind) == "TRANSFER":
		return "a transfer"
	case ydbKeywordAt(tokens, kind) == "STREAMING" && ydbKeywordAt(tokens, kind+1) == "QUERY":
		return "a streaming query"
	default:
		return ""
	}
}

// ydbObjectKindIndex is the index of the word that names the kind of object a
// CREATE, ALTER or DROP statement is about, past an OR REPLACE.
func ydbObjectKindIndex(tokens []lexer.Token) int {
	if ydbKeywordAt(tokens, 1) == "OR" && ydbKeywordAt(tokens, 2) == "REPLACE" {
		return 3
	}
	return 1
}

// validateYDBRealmStatement refuses a statement whose effect leaves a dev
// realm or a database a run does not own as a whole.
func validateYDBRealmStatement(tokens []lexer.Token) error {
	first := ydbKeyword(tokens[0])
	switch first {
	case "SELECT", "DECLARE", "PROCESS", "REDUCE", "VALUES", "COMMIT", "ROLLBACK":
		return nil
	case "PRAGMA":
		if name := ydbPragmaName(tokens); strings.EqualFold(name, "TablePathPrefix") {
			return unsafeReplayStatement(platform.YDB, "PRAGMA TablePathPrefix")
		}
		return nil
	case "USE":
		return unsafeReplayStatement(platform.YDB, "USE")
	case "DEFINE":
		if ydbKeywordAt(tokens, 1) == "SUBQUERY" {
			return nil
		}
		return unsafeReplayStatement(platform.YDB, "DEFINE ACTION")
	case "DO", "EVALUATE":
		return unsafeReplayStatement(platform.YDB, first)
	case "GRANT", "REVOKE":
		return unsafeReplayStatement(platform.YDB, first+" permission change")
	case "BACKUP", "RESTORE":
		return unsafeReplayStatement(platform.YDB, first)
	case "INSERT", "UPSERT", "REPLACE":
		return ydbCheckTargetAfter(tokens, 1, "INTO")
	case "UPDATE":
		return ydbCheckTarget(tokens, 1)
	case "DELETE":
		return ydbCheckTargetAfter(tokens, 1, "FROM")
	case "BATCH":
		return validateYDBRealmStatement(tokens[1:])
	case "ANALYZE":
		return ydbCheckTarget(tokens, 1)
	case "TRUNCATE":
		return ydbCheckTargetAfter(tokens, 1, "TABLE")
	case "CREATE", "ALTER", "DROP":
		return validateYDBObjectStatement(tokens)
	}
	if strings.HasPrefix(tokens[0].Value, "$") && tokens[0].Type == lexer.TokenIdentifier {
		// A named expression or lambda, which defines a value and writes
		// nothing; a write that uses it is judged by its own statement.
		return nil
	}
	return unsafeReplayStatement(platform.YDB, "unrecognized statement "+strings.ToUpper(tokens[0].Value))
}

// validateYDBObjectStatement refuses a CREATE, ALTER or DROP whose object the
// whole database shares, or whose path leaves the realm.
func validateYDBObjectStatement(tokens []lexer.Token) error {
	kind := ydbObjectKindIndex(tokens)
	switch ydbKeywordAt(tokens, kind) {
	case "TABLE":
		if err := ydbCheckTarget(tokens, ydbSkipExistenceGuard(tokens, kind+1)); err != nil {
			return err
		}
		return ydbCheckRenameTarget(tokens)
	case "VIEW", "SEQUENCE", "TOPIC":
		return ydbCheckTarget(tokens, ydbSkipExistenceGuard(tokens, kind+1))
	case "COORDINATION":
		// Ptah's own statement for a coordination node, which Ptah's
		// connection resolves against the realm's prefix as YDB resolves a
		// table, and which the reset drops.
		if ydbKeywordAt(tokens, kind+1) != "NODE" {
			return unsafeReplayStatement(platform.YDB, "unrecognized object "+
				strings.ToUpper(tokens[0].Value)+" COORDINATION "+strings.ToUpper(tokenValueAt(tokens, kind+1)))
		}
		return ydbCheckTarget(tokens, kind+2)
	case "TEMP", "TEMPORARY":
		return unsafeReplayStatement(platform.YDB, "temporary table")
	case "USER", "GROUP":
		return unsafeReplayStatement(platform.YDB, "a "+strings.ToLower(ydbKeywordAt(tokens, kind))+
			" of the whole database")
	case "SECRET":
		// A secret is a path in the scheme tree: after the prefix, `CREATE
		// SECRET s` created the secret in the realm, and the realm's reset
		// drops one.
		return ydbCheckTarget(tokens, ydbSkipExistenceGuard(tokens, kind+1))
	case "OBJECT":
		// CREATE OBJECT makes the deprecated secret object, which belongs to
		// the user rather than to a path, and the other objects of the
		// database's metadata service.
		return unsafeReplayStatement(platform.YDB, "an object of the whole database")
	case "RESOURCE":
		return unsafeReplayStatement(platform.YDB, "a resource pool of the whole database")
	case "BACKUP":
		return unsafeReplayStatement(platform.YDB, "a backup collection of the whole database")
	case "DATABASE":
		return unsafeReplayStatement(platform.YDB, "a change to the whole database")
	default:
		return unsafeReplayStatement(platform.YDB, "unrecognized object "+
			strings.ToUpper(tokens[0].Value)+" "+strings.ToUpper(tokenValueAt(tokens, kind)))
	}
}

// ydbSkipExistenceGuard returns the index after an IF EXISTS or IF NOT EXISTS
// that starts at index, or index when none does.
func ydbSkipExistenceGuard(tokens []lexer.Token, index int) int {
	if ydbKeywordAt(tokens, index) != "IF" {
		return index
	}
	if ydbKeywordAt(tokens, index+1) == "NOT" {
		return index + 3
	}
	return index + 2
}

// ydbCheckRenameTarget checks the new path of `ALTER TABLE ... RENAME TO`.
// `RENAME INDEX a TO b` renames an index of the table, which is no path.
func ydbCheckRenameTarget(tokens []lexer.Token) error {
	for index := range tokens {
		if ydbKeyword(tokens[index]) == "RENAME" && ydbKeywordAt(tokens, index+1) == "TO" {
			if err := ydbCheckTarget(tokens, index+2); err != nil {
				return err
			}
		}
	}
	return nil
}

// ydbCheckTargetAfter checks the target that follows the keyword, when the
// keyword is at index or after a modifier such as `OR IGNORE`.
func ydbCheckTargetAfter(tokens []lexer.Token, index int, keyword string) error {
	position := slices.IndexFunc(tokens[min(index, len(tokens)):], func(token lexer.Token) bool {
		return ydbKeyword(token) == keyword
	})
	if position < 0 {
		return unsafeReplayStatement(platform.YDB, "a "+strings.ToUpper(tokens[0].Value)+" with no "+keyword+
			" the guard can read")
	}
	return ydbCheckTarget(tokens, index+position+1)
}

// ydbCheckTarget refuses the write target at index when it is not a relative
// path inside the realm.
func ydbCheckTarget(tokens []lexer.Token, index int) error {
	if index >= len(tokens) {
		return unsafeReplayStatement(platform.YDB, "a statement whose target the guard cannot read")
	}
	target := tokens[index]
	switch {
	case target.Type != lexer.TokenIdentifier:
		return unsafeReplayStatement(platform.YDB, "a target written as "+target.Value)
	case strings.HasPrefix(target.Value, "$"):
		return unsafeReplayStatement(platform.YDB, "a target named through "+target.Value)
	case index+1 < len(tokens) && (tokens[index+1].MatchOperatorValue(".") || tokens[index+1].MatchOperatorValue(":")):
		return unsafeReplayStatement(platform.YDB, "a target in the cluster "+target.Value)
	}
	targetPath := ydbUnquote(target.Value)
	if strings.HasPrefix(targetPath, "/") {
		return unsafeReplayStatement(platform.YDB, "the absolute path "+targetPath)
	}
	if slices.Contains(strings.Split(targetPath, "/"), "..") {
		return unsafeReplayStatement(platform.YDB, "the path "+targetPath+", which climbs out of the dev database")
	}
	return nil
}

// ydbUnquote is the path a YQL identifier names: a backticked one without its
// backticks and escapes, and a bare one as written.
func ydbUnquote(identifier string) string {
	if len(identifier) < 2 || identifier[0] != '`' || identifier[len(identifier)-1] != '`' {
		return identifier
	}
	inner := identifier[1 : len(identifier)-1]
	var b strings.Builder
	for i := 0; i < len(inner); i++ {
		switch {
		case inner[i] == '\\' && i+1 < len(inner):
			i++
			b.WriteByte(inner[i])
		case inner[i] == '`' && i+1 < len(inner) && inner[i+1] == '`':
			i++
			b.WriteByte('`')
		default:
			b.WriteByte(inner[i])
		}
	}
	return b.String()
}

// ydbPragmaName is the name a PRAGMA sets, without the namespace a dotted name
// carries: `TablePathPrefix` for `PRAGMA ydb.TablePathPrefix = ...`.
func ydbPragmaName(tokens []lexer.Token) string {
	name := ""
	for index := 1; index < len(tokens); index++ {
		token := tokens[index]
		if token.Type != lexer.TokenIdentifier {
			break
		}
		name = ydbUnquote(token.Value)
		if index+1 >= len(tokens) || !tokens[index+1].MatchOperatorValue(".") {
			break
		}
		index++
	}
	return name
}

// ydbKeyword is a bare word in upper case, or "" for anything else: a
// backticked identifier, a `$` name, a string or an operator is never a
// keyword.
func ydbKeyword(token lexer.Token) string {
	if token.Type != lexer.TokenIdentifier || token.Value == "" ||
		token.Value[0] == '`' || token.Value[0] == '$' {
		return ""
	}
	return strings.ToUpper(token.Value)
}

// ydbKeywordAt is ydbKeyword of the token at index, or "" past the end.
func ydbKeywordAt(tokens []lexer.Token, index int) string {
	if index < 0 || index >= len(tokens) {
		return ""
	}
	return ydbKeyword(tokens[index])
}

// tokenValueAt is the text of the token at index, or "" past the end.
func tokenValueAt(tokens []lexer.Token, index int) string {
	if index < 0 || index >= len(tokens) {
		return ""
	}
	return tokens[index].Value
}

// ydbContainsKeywords reports whether the bare words appear in sequence.
func ydbContainsKeywords(tokens []lexer.Token, words ...string) bool {
	for start := range tokens {
		matched := true
		for offset, word := range words {
			if ydbKeywordAt(tokens, start+offset) != word {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
