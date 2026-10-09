package devclean

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/lexer"
	"ptah.run/internal/ydburl"
)

// ReplayGuard rejects migration statements whose effects cannot be confined to
// the disposable database realm cleaned after migration replay.
type ReplayGuard struct {
	info  catalog.ServerInfo
	realm ReplayRealm
	// remedy ends a refusal that [ReplayRealmServer] lifts; see
	// [ReplayGuard.WithServerRealmRemedy].
	remedy string
	// purpose is what the checked statements are; see [NewBaselineGuard].
	purpose replayPurpose
}

// replayPurpose is what the statements a guard checks are, which decides the
// one class of refusal the two kinds of statement do not share.
type replayPurpose int

const (
	// purposeMigration is a migration file, written by anyone. An executable
	// body in it is opaque, so a routine, a trigger or an event is refused in
	// a realm whose cleanup it could reach past when it runs.
	purposeMigration replayPurpose = iota
	// purposeBaseline is the baseline a rehearsal writes to recreate the
	// target's current schema. Ptah derived it from the target, so a body in
	// it is the target's own and stays in the dev database; what escapes the
	// realm or reaches past the server is refused as for a migration.
	purposeBaseline
)

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
	// ReplayRealmServerDatabases confines a replay to the user databases of a
	// MySQL or MariaDB server the operator named as a whole, with a dev URL
	// that names no database (stokaro/ptah#3789). The claim found no user
	// database there and the cleanup drops every one, so the replay may
	// create, change and drop databases and write in any of them. A role, a
	// user, a privilege or a stored body outlives that cleanup, and is refused
	// as it is in [ReplayRealmDatabase].
	ReplayRealmServerDatabases
)

// NewReplayGuard creates a dialect-aware migration replay guard for a replay
// confined to realm.
func NewReplayGuard(info catalog.ServerInfo, realm ReplayRealm) *ReplayGuard {
	return &ReplayGuard{info: info, realm: realm}
}

// NewBaselineGuard is the guard for the baseline a plan rehearsal writes to
// recreate the target's current schema in the dev database reached through
// info. Its realm is [DevReplayRealm]'s, and a refusal the server realm would
// lift names the two ways to it, as [NewDevReplayGuard] does.
//
// It refuses what escapes that realm or reaches past the server, as a replay
// does: a role, a user, a group or a privilege the realm's cleanup leaves
// behind, a database-wide YDB object, a replication, a transfer, an external
// source or table, and a streaming query. It accepts what a migration file is
// refused only because a body nobody vetted is opaque: a routine in a trusted
// language, a trigger, and a comment on an object of the dev database. The
// baseline is DDL Ptah derived from the target, so those bodies are the
// target's own and stay in the dev database.
func NewBaselineGuard(info catalog.ServerInfo) *ReplayGuard {
	guard := NewDevReplayGuard(info)
	guard.purpose = purposeBaseline
	return guard
}

// NewDevReplayGuard is the guard for statements a run executes on the dev
// server reached through info: a migration replay, and a plan rehearsal on a
// whole MySQL or MariaDB server. The realm is [DevReplayRealm]'s, and a
// refusal the server realm would lift ends with [devdocker.OwnedServerRemedy],
// the two ways to that realm.
func NewDevReplayGuard(info catalog.ServerInfo) *ReplayGuard {
	return NewReplayGuard(info, DevReplayRealm(info)).WithServerRealmRemedy(devdocker.OwnedServerRemedy)
}

// DevReplayRealm is how much of the dev server reached through info a run may
// change: the whole server when the run owns it, every user database of a
// MySQL-family server whose URL named no database, and the dev database
// otherwise. The run owns a server this process provisioned and removes
// afterwards, and a server the operator declared disposable with
// [devdocker.DisposableServerEnvVar].
//
// Both answers come from the one record [devdocker.RunOwned] reads, keyed on
// the URL the connection was opened with, and the two cases cannot be asked
// apart: a second question would be a second place for the next consumer to
// forget. The answer is not re-derived from the operator's `--dev-url`
// spelling, because every consumer resolves a docker URL before it connects
// and hands the connectable URL on, so by the time statements run no docker
// URL is left to read. [Claim] reads the same record to decide whether the
// server's default database may be reset.
func DevReplayRealm(info catalog.ServerInfo) ReplayRealm {
	if devdocker.RunOwned(info.URL) {
		return ReplayRealmServer
	}
	// A dev URL naming no MySQL-family database gave the run every user
	// database on the server, and the claim found none there.
	if info.WholeServer {
		return ReplayRealmServerDatabases
	}
	return ReplayRealmDatabase
}

// WithServerRealmRemedy returns a copy of the guard that ends a refusal with
// remedy when [ReplayRealmServer] would have accepted the statement, so the
// operator reads how to reach that realm in the refusal itself.
//
// Whether the server realm would accept it is asked of the server realm's own
// rules, not of a second list: a list kept beside them would agree when it is
// written and stop agreeing when either is extended. A refusal the server realm
// keeps, such as ALTER SYSTEM, ends as it did, because reaching that realm
// would not lift it.
//
// Only a caller whose realm the operator can raise to the server passes a
// remedy; a guard built for a fixed realm has nothing to suggest. A guard for
// [ReplayRealmServer] never appends it, since the server realm keeps every
// refusal it makes.
func (g *ReplayGuard) WithServerRealmRemedy(remedy string) *ReplayGuard {
	guard := *g
	guard.remedy = remedy
	return &guard
}

// ValidateStatement rejects statements whose effects cannot be confined to
// the replay database realm.
func (g *ReplayGuard) ValidateStatement(stmt string) error {
	err := g.validate(stmt, g.realm)
	var unsafe *unsafeStatementError
	if g.purpose == purposeBaseline && errors.As(err, &unsafe) {
		unsafe.baseline = true
	}
	if err == nil || g.remedy == "" {
		return err
	}
	if g.validate(stmt, ReplayRealmServer) != nil {
		return err
	}
	return fmt.Errorf("%w; %s", err, g.remedy)
}

// validate is [ReplayGuard.ValidateStatement] for one realm.
func (g *ReplayGuard) validate(stmt string, realm ReplayRealm) error {
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
		if realm == ReplayRealmServer && postgresServerWideOperation(tokens) != "" {
			return validatePostgresServerWideStatement(dialect, tokens)
		}
		return validatePostgresReplayStatement(dialect, tokens, g.purpose)
	case platform.MySQL, platform.MariaDB:
		if realm == ReplayRealmServer && mysqlServerWideOperation(tokens) != "" {
			return nil
		}
		if realm == ReplayRealmServerDatabases && mysqlDatabaseOperation(tokens) {
			return nil
		}
		return validateMySQLReplayStatement(dialect, g.info.Schema, tokens, realm, g.purpose)
	case platform.SQLServer:
		return validateSQLServerReplayStatement(tokens, g.purpose)
	case platform.ClickHouse:
		return validateClickHouseReplayStatement(g.info.Schema, tokens)
	case platform.YDB:
		return validateYDBReplayStatement(tokens, realm, g.ydbBaselineRoot())
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
	return &unsafeStatementError{dialect: dialect, operation: operation}
}

// unsafeStatementError is a refusal of one statement. A baseline refusal says
// so, because a reader of "migration replay" would look for a migration file
// that does not exist.
type unsafeStatementError struct {
	dialect, operation string
	baseline           bool
}

func (e *unsafeStatementError) Error() string {
	if e.baseline {
		return fmt.Sprintf("%s rehearsal baseline refuses %s because its effects cannot be confined to the dev database realm",
			e.dialect, e.operation)
	}
	return fmt.Sprintf("%s migration replay rejects %s because its effects cannot be confined to the disposable database realm",
		e.dialect, e.operation)
}

// ydbBaselineRoot is the absolute path of the YDB dev realm a baseline writes
// in, which a baseline grant may name, or "" for a guard that is not a
// baseline's or a connection that names no realm.
func (g *ReplayGuard) ydbBaselineRoot() string {
	if g.purpose != purposeBaseline {
		return ""
	}
	parsed, err := ydburl.Parse(g.info.URL)
	if err != nil || parsed.Realm == "" {
		return ""
	}
	return parsed.Root()
}
