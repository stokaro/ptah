package devclean

import (
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// ReplayGuard rejects migration statements whose effects cannot be confined to
// the disposable database realm cleaned after migration replay.
type ReplayGuard struct {
	info  catalog.ServerInfo
	realm ReplayRealm
}

// ReplayRealm is how much of the dev server a replay may change.
type ReplayRealm int

const (
	// ReplayRealmDatabase confines a replay to the dev database. It is the
	// realm of a server the operator named: that server may hold other
	// databases and roles, and a role or a database a replay creates outlives
	// the cleanup, which empties the dev database and nothing else.
	ReplayRealmDatabase ReplayRealm = iota
	// ReplayRealmServer lets a replay change the whole server. It is the realm
	// of a server this run provisioned and removes afterwards, where a role,
	// another database or a routine body cannot reach anything the run does
	// not own.
	//
	// It lifts only the refusals whose reason is the realm. A statement that
	// changes the replay session, the server's catalogs or its configuration,
	// or that reaches past the server, is refused here too: those would
	// mislead the rest of the run, or leave the container.
	ReplayRealmServer
)

// NewReplayGuard creates a dialect-aware migration replay guard for a replay
// confined to realm.
func NewReplayGuard(info catalog.ServerInfo, realm ReplayRealm) *ReplayGuard {
	return &ReplayGuard{info: info, realm: realm}
}

// ValidateStatement rejects statements whose effects cannot be confined to
// the replay database realm.
func (g *ReplayGuard) ValidateStatement(stmt string) error {
	dialect := platform.NormalizeDialect(g.info.Dialect)
	if dialect == platform.MySQL || dialect == platform.MariaDB {
		if err := rejectMySQLExecutableComments(dialect, stmt); err != nil {
			return err
		}
	}
	tokens := significantTokens(stmt, replayLexerOptions(dialect))
	switch dialect {
	case platform.SQLite:
		return validateSQLiteReplayStatement(tokens)
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		if g.realm == ReplayRealmServer && postgresServerWideOperation(tokens) != "" {
			return validatePostgresServerWideStatement(dialect, tokens)
		}
		return validatePostgresReplayStatement(dialect, tokens)
	case platform.MySQL, platform.MariaDB:
		if g.realm == ReplayRealmServer && mysqlServerWideOperation(tokens) != "" {
			return nil
		}
		return validateMySQLReplayStatement(dialect, g.info.Schema, tokens, g.realm)
	case platform.SQLServer:
		return validateSQLServerReplayStatement(tokens)
	case platform.ClickHouse:
		return validateClickHouseReplayStatement(g.info.Schema, tokens)
	default:
		unsupportedDialect := strings.TrimSpace(g.info.Dialect)
		if unsupportedDialect == "" {
			unsupportedDialect = "unknown"
		}
		return unsafeReplayStatement(unsupportedDialect, "unsupported dialect")
	}
}

func rejectMySQLExecutableComments(dialect, stmt string) error {
	lex := lexer.NewLexerWithOptions(stmt, replayLexerOptions(dialect))
	for {
		token := lex.NextToken()
		if token.Type == lexer.TokenEOF {
			return nil
		}
		if token.Type != lexer.TokenComment {
			continue
		}
		comment := strings.ToUpper(token.Value)
		if strings.HasPrefix(comment, "/*!") || strings.HasPrefix(comment, "/*M!") {
			return unsafeReplayStatement(dialect, "executable comment")
		}
	}
}

func validateSQLiteReplayStatement(tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}

	first := normalizedIdentifier(tokens[0])
	switch first {
	case "ATTACH", "DETACH":
		return unsafeReplayStatement(platform.SQLite, first)
	case "CREATE":
		if createsSQLiteTemporaryObject(tokens) {
			return unsafeReplayStatement(platform.SQLite, "TEMP object")
		}
	case "PRAGMA":
		if !isRestorableSQLitePragma(tokens) {
			return unsafeReplayStatement(platform.SQLite, "state-changing or unsupported PRAGMA")
		}
	case "VACUUM":
		if containsIdentifier(tokens[1:], "INTO") {
			return unsafeReplayStatement(platform.SQLite, "VACUUM INTO")
		}
	}

	if containsIdentifier(tokens, "LOAD_EXTENSION") {
		return unsafeReplayStatement(platform.SQLite, "load_extension")
	}
	if mutatesSQLiteRealm(first) && containsQualifiedIdentifier(tokens, "temp") {
		return unsafeReplayStatement(platform.SQLite, "TEMP schema target")
	}
	return nil
}

func createsSQLiteTemporaryObject(tokens []lexer.Token) bool {
	if len(tokens) < 2 {
		return false
	}
	modifier := normalizedIdentifier(tokens[1])
	return modifier == "TEMP" || modifier == "TEMPORARY"
}

func isRestorableSQLitePragma(tokens []lexer.Token) bool {
	if len(tokens) < 2 {
		return true
	}
	return normalizedIdentifier(tokens[1]) == "FOREIGN_KEYS"
}

func significantTokens(stmt string, opts lexer.Options) []lexer.Token {
	lex := lexer.NewLexerWithOptions(stmt, opts)
	var tokens []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenSemicolon:
			continue
		case lexer.TokenEOF:
			return tokens
		default:
			tokens = append(tokens, token)
		}
	}
}

func containsIdentifier(tokens []lexer.Token, value string) bool {
	for _, token := range tokens {
		if normalizedIdentifier(token) == value {
			return true
		}
	}
	return false
}

func containsQualifiedIdentifier(tokens []lexer.Token, value string) bool {
	for index := range len(tokens) - 1 {
		if normalizedIdentifier(tokens[index]) == strings.ToUpper(value) &&
			tokens[index+1].MatchOperatorValue(".") {
			return true
		}
	}
	return false
}

func normalizedIdentifier(token lexer.Token) string {
	value := token.Value
	switch token.Type {
	case lexer.TokenIdentifier:
	case lexer.TokenString:
	default:
		return ""
	}
	value = strings.TrimPrefix(value, "`")
	value = strings.TrimSuffix(value, "`")
	value = strings.TrimPrefix(value, `"`)
	value = strings.TrimSuffix(value, `"`)
	value = strings.TrimPrefix(value, "'")
	value = strings.TrimSuffix(value, "'")
	value = strings.TrimPrefix(value, "[")
	value = strings.TrimSuffix(value, "]")
	value = strings.ReplaceAll(value, "``", "`")
	value = strings.ReplaceAll(value, `""`, `"`)
	value = strings.ReplaceAll(value, "''", "'")
	value = strings.ReplaceAll(value, "]]", "]")
	return strings.ToUpper(value)
}

func mutatesSQLiteRealm(first string) bool {
	switch first {
	case "ALTER", "CREATE", "DELETE", "DROP", "INSERT", "PRAGMA", "REINDEX",
		"REPLACE", "UPDATE", "VACUUM":
		return true
	default:
		return false
	}
}

func unsafeReplayStatement(dialect, operation string) error {
	return fmt.Errorf(
		"%s migration replay rejects %s because its effects cannot be confined to the disposable database realm",
		dialect,
		operation,
	)
}
