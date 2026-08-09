package migrator

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/lexer"
)

type noTransactionPrefixAction uint8

type postgresSearchPathKnowledge uint8

const (
	noTransactionPrefixDurable noTransactionPrefixAction = iota
	noTransactionPrefixReplay
	noTransactionPrefixReject
)

const (
	postgresSearchPathUnknown postgresSearchPathKnowledge = iota
	postgresSearchPathKnown
)

func (m *Migrator) withNoTransactionSession(
	ctx context.Context,
	use func(*Migrator) error,
) error {
	if m.conn.Writer().IsDryRun() || m.noTransactionSession != nil {
		return use(m)
	}
	if implicitCommitDialect(m.connectionDialect()) {
		return m.conn.WithSession(ctx, func(metadataConn *dbschema.DatabaseConnection) error {
			metadataScoped := *m
			metadataScoped.conn = metadataConn
			if metadataScoped.migrationsSchema == "" {
				metadataScoped.migrationsSchema = metadataScoped.connectionSchemaName()
			}
			if err := metadataScoped.refuseMySQLTemporaryMetadataShadow(ctx); err != nil {
				return err
			}
			return m.conn.WithSession(ctx, func(bodyConn *dbschema.DatabaseConnection) error {
				scoped := metadataScoped
				scoped.noTransactionSession = bodyConn
				scoped.postgresIndexObservation = nil
				return use(&scoped)
			})
		})
	}
	return m.conn.WithSession(ctx, func(conn *dbschema.DatabaseConnection) error {
		scoped := *m
		scoped.noTransactionSession = conn
		scoped.postgresIndexObservation = nil
		if platform.NormalizeDialect(scoped.connectionDialect()) == platform.SQLite {
			// In-memory SQLite has one physical connection, so using the pool for
			// checkpoints while that connection is pinned would deadlock. Keep
			// metadata on the session and qualify it against main so temporary
			// objects cannot shadow the revision table.
			scoped.conn = conn
			if scoped.migrationsSchema == "" {
				scoped.migrationsSchema = scoped.connectionSchemaName()
			}
		}
		return use(&scoped)
	})
}

func (m *Migrator) noTransactionConnection() *dbschema.DatabaseConnection {
	if m.noTransactionSession != nil {
		return m.noTransactionSession
	}
	return m.conn
}

func (m *Migrator) restoreNoTransactionSessionPrefix(
	ctx context.Context,
	migration *Migration,
	direction MigrationDirection,
	resumeFrom int,
) error {
	m.startPostgresIndexObservation()
	if resumeFrom <= 1 {
		return nil
	}
	executionConn := m.noTransactionConnection()
	statements := splitSQLStatementsForConnection(executionConn, migrationSQLForDirection(migration, direction))
	searchPathKnowledge := postgresSearchPathUnknown
	for i, statement := range statements[:resumeFrom-1] {
		if err := m.observePostgresIndexStatementForReplay(
			ctx,
			executionConn,
			statement,
			searchPathKnowledge,
		); err != nil {
			return fmt.Errorf(
				"migration %d cannot resolve the PostgreSQL index target recorded by statement %d: %w",
				migration.Version,
				i+1,
				err,
			)
		}
		switch noTransactionResumeAction(statement, m.connectionDialect()) {
		case noTransactionPrefixDurable:
			continue
		case noTransactionPrefixReplay:
			if err := executeSQLOutsideTransaction(ctx, executionConn, statement); err != nil {
				return fmt.Errorf(
					"migration %d cannot restore session state from committed statement %d before resume: %w",
					migration.Version,
					i+1,
					err,
				)
			}
			if changed, known := postgresSearchPathReplayState(statement); changed {
				searchPathKnowledge = postgresSearchPathUnknown
				if known {
					searchPathKnowledge = postgresSearchPathKnown
				}
			}
		case noTransactionPrefixReject:
			return fmt.Errorf(
				"migration %d cannot reconstruct session state after committed statement %d because %q may depend "+
					"on session-local state that cannot be restored safely; inspect the database and choose an explicit "+
					"metadata repair instead of replaying migration SQL",
				migration.Version,
				i+1,
				statement,
			)
		}
	}
	return nil
}

func postgresSearchPathReplayState(statement string) (changed, known bool) {
	tokens := significantSQLTokens(statement, platform.Postgres)
	if len(tokens) < 2 {
		return false, false
	}
	if tokens[0].MatchIdentifierValue("RESET") {
		if tokens[1].MatchIdentifierValue("ALL") || tokens[1].MatchIdentifierValue("SEARCH_PATH") {
			return true, false
		}
		return false, false
	}
	if !tokens[0].MatchIdentifierValue("SET") {
		return false, false
	}
	setting := 1
	local := tokens[setting].MatchIdentifierValue("LOCAL")
	if local || tokens[setting].MatchIdentifierValue("SESSION") {
		setting++
	}
	if setting >= len(tokens) || !tokens[setting].MatchIdentifierValue("SEARCH_PATH") {
		return false, false
	}
	if local {
		return true, false
	}
	for _, token := range tokens[setting+1:] {
		if matchesAnyKeyword(token, "DEFAULT", "CURRENT") || strings.Contains(strings.ToLower(token.Value), "$user") {
			return true, false
		}
	}
	return true, true
}

func noTransactionResumeAction(statement, dialect string) noTransactionPrefixAction {
	tokens := significantSQLTokens(statement, dialect)
	if len(tokens) == 0 {
		return noTransactionPrefixDurable
	}
	if createsTemporaryObject(tokens, dialect) {
		return noTransactionPrefixReject
	}
	if matchesAnyKeyword(tokens[0],
		"SET", "RESET", "USE", "PRAGMA", "DISCARD", "LISTEN", "UNLISTEN",
		"PREPARE", "DEALLOCATE", "ATTACH", "DETACH", "LOCK", "UNLOCK",
	) {
		return noTransactionPrefixReplay
	}
	if matchesAnyKeyword(tokens[0],
		"CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "MERGE",
		"TRUNCATE", "GRANT", "REVOKE", "COMMENT", "ANALYZE", "VACUUM",
		"REINDEX", "CLUSTER", "REFRESH", "COPY",
	) {
		return noTransactionPrefixDurable
	}
	return noTransactionPrefixReject
}

func (m *Migrator) validateNoTransactionSQL(migration *Migration, direction MigrationDirection) error {
	sqlText := migrationSQLForDirection(migration, direction)
	statements := splitSQLStatementsForConnection(m.conn, sqlText)
	for i, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" || !isTransactionControlStatement(statement, m.connectionDialect()) {
			continue
		}
		return fmt.Errorf(
			"migration %d cannot run %s statement %d without a transaction because %q controls transaction state; "+
				"remove the transaction control and let Ptah checkpoint each statement independently",
			migration.Version,
			direction,
			i+1,
			statement,
		)
	}
	return nil
}

func isTransactionControlStatement(statement, dialect string) bool {
	tokens := significantSQLTokens(statement, dialect)
	if len(tokens) == 0 {
		return false
	}
	normalized := platform.NormalizeDialect(dialect)
	return isPrimaryTransactionControl(tokens, normalized) || isSetTransactionControl(statement, dialect, tokens, normalized)
}

func isPrimaryTransactionControl(tokens []lexer.Token, dialect string) bool {
	first := tokens[0]
	if matchesAnyKeyword(first, "COMMIT", "ROLLBACK", "SAVEPOINT", "ABORT") {
		return true
	}
	if first.MatchIdentifierValue("END") && dialect != platform.SQLServer {
		return true
	}
	if first.MatchIdentifierValue("XA") {
		return true
	}
	if first.MatchIdentifierValue("SAVE") {
		return len(tokens) > 1 && matchesAnyKeyword(tokens[1], "TRAN", "TRANSACTION")
	}
	if first.MatchIdentifierValue("PREPARE") {
		return len(tokens) > 1 && tokens[1].MatchIdentifierValue("TRANSACTION")
	}
	if first.MatchIdentifierValue("RELEASE") {
		return len(tokens) > 1 && tokens[1].MatchIdentifierValue("SAVEPOINT")
	}
	if first.MatchIdentifierValue("START") {
		return len(tokens) > 1 && tokens[1].MatchIdentifierValue("TRANSACTION")
	}
	if first.MatchIdentifierValue("BEGIN") {
		if dialect != platform.SQLServer {
			return true
		}
		return len(tokens) > 1 && matchesAnyKeyword(tokens[1], "TRAN", "TRANSACTION", "DISTRIBUTED")
	}
	return false
}

func isSetTransactionControl(statement, dialect string, tokens []lexer.Token, normalizedDialect string) bool {
	first := tokens[0]
	if !first.MatchIdentifierValue("SET") {
		return false
	}
	if containsIdentifierSequence(statement, dialect, "TRANSACTION") ||
		containsIdentifierSequence(statement, dialect, "IMPLICIT_TRANSACTIONS") {
		return true
	}
	return (normalizedDialect == platform.MySQL || normalizedDialect == platform.MariaDB) &&
		containsIdentifierSequence(statement, dialect, "AUTOCOMMIT")
}

func createsTemporaryObject(tokens []lexer.Token, dialect string) bool {
	if platform.NormalizeDialect(dialect) == platform.SQLServer && slices.ContainsFunc(tokens, isSQLServerTemporaryIdentifier) {
		return true
	}
	if !tokens[0].MatchIdentifierValue("CREATE") {
		return false
	}
	for _, token := range tokens[1:min(len(tokens), 5)] {
		if token.MatchIdentifierValue("TEMP") || token.MatchIdentifierValue("TEMPORARY") {
			return true
		}
	}
	return false
}

func isSQLServerTemporaryIdentifier(token lexer.Token) bool {
	if token.Type != lexer.TokenIdentifier {
		return false
	}
	identifier := token.Value
	if strings.HasPrefix(identifier, "[") && strings.HasSuffix(identifier, "]") {
		identifier = strings.ReplaceAll(identifier[1:len(identifier)-1], "]]", "]")
	}
	return strings.HasPrefix(identifier, "#")
}

func matchesAnyKeyword(token lexer.Token, keywords ...string) bool {
	return slices.ContainsFunc(keywords, token.MatchIdentifierValue)
}
