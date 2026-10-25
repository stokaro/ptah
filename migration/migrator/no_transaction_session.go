package migrator

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/dbschema"
	"ptah.run/internal/lexer"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/builtin"
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
		if scoped.services().PinnedMetadata {
			// A database with one physical connection, such as in-memory
			// SQLite, would deadlock if checkpoints used the pool while that
			// connection is pinned. Keep metadata on the session and qualify it
			// against the connection's schema so temporary objects cannot
			// shadow the revision table.
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
	return m.restoreSessionPrefix(ctx, migration, direction, resumeFrom, ownTransactionRun{})
}

// restoreSessionPrefix replays, on a fresh session, the session state the
// committed prefix of a resumed migration left, and refuses a prefix whose
// session state it cannot restore.
//
// run describes a body that opens and closes its own transactions. A resumed
// attempt starts after a committed point, so every transaction of the prefix
// committed. The body's BEGIN and COMMIT change no session state, and neither
// do SET LOCAL, SET TRANSACTION, SET CONSTRAINTS and LOCK inside a
// transaction, which ended with it; replaying LOCK outside a transaction is
// refused by the server. A plain SET or RESET inside one outlives the
// transaction, and is replayed as it is outside one.
func (m *Migrator) restoreSessionPrefix(
	ctx context.Context,
	migration *Migration,
	direction MigrationDirection,
	resumeFrom int,
	run ownTransactionRun,
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
		switch ownTransactionResumeAction(i, statement, m.connectionDialect(), run) {
		case noTransactionPrefixDurable:
			continue
		case noTransactionPrefixReplay:
			if err := executeSQLOn(ctx, executionConn, statement); err != nil {
				return fmt.Errorf(
					"migration %d cannot restore session state from committed statement %d before resume: %w",
					migration.Version,
					i+1,
					err,
				)
			}
			if changed, known := searchPathChange(m.services().Indexes, statement); changed {
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

// searchPathChange reports whether a replayed statement changes how an
// unqualified name resolves, and whether the value it leaves is known. Only
// the index guard reads the answer, so an engine without one has none.
func searchPathChange(guard migratorext.IndexGuard, statement string) (changed, known bool) {
	if guard == nil {
		return false, false
	}
	return guard.SearchPath(statement)
}

// ownTransactionResumeAction is [noTransactionResumeAction] for the statement
// at index of a committed prefix of a body run describes; see
// [Migrator.restoreSessionPrefix].
func ownTransactionResumeAction(index int, statement, dialect string, run ownTransactionRun) noTransactionPrefixAction {
	if _, inside := run.transactions.Opening(index); !inside {
		return noTransactionResumeAction(statement, dialect)
	}
	if opened, ok := run.blocks.Opening(index); ok && (opened == index || run.blocks.Closes(index)) {
		return noTransactionPrefixDurable
	}
	tokens := significantSQLTokens(statement, dialect)
	if len(tokens) > 0 && tokens[0].MatchIdentifierValue("LOCK") {
		return noTransactionPrefixDurable
	}
	if len(tokens) > 1 && tokens[0].MatchIdentifierValue("SET") &&
		matchesAnyKeyword(tokens[1], "LOCAL", "TRANSACTION", "CONSTRAINTS") {
		return noTransactionPrefixDurable
	}
	return noTransactionResumeAction(statement, dialect)
}

func noTransactionResumeAction(statement, dialect string) noTransactionPrefixAction {
	services := builtin.Lookup(dialect)
	if services.Units != nil {
		// A query unit carries every definition and setting it needs, and
		// the engine keeps no session state between queries, so a committed
		// one leaves nothing a resumed session has to replay.
		return noTransactionPrefixDurable
	}
	tokens := significantSQLTokens(statement, dialect)
	if len(tokens) == 0 {
		return noTransactionPrefixDurable
	}
	if createsTemporaryObject(tokens, services) {
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
	services := builtin.Lookup(dialect)
	return isPrimaryTransactionControl(tokens, services) ||
		isSetTransactionControl(statement, dialect, tokens, services)
}

// isPrimaryTransactionControl reports whether a statement opens, ends or marks
// a transaction. Where the engine's BEGIN and END delimit a block, only BEGIN
// TRAN, BEGIN TRANSACTION and BEGIN DISTRIBUTED open one.
func isPrimaryTransactionControl(tokens []lexer.Token, services migratorext.Services) bool {
	first := tokens[0]
	if matchesAnyKeyword(first, "COMMIT", "ROLLBACK", "SAVEPOINT", "ABORT") {
		return true
	}
	if first.MatchIdentifierValue("END") && !services.BlockBegin {
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
		if !services.BlockBegin {
			return true
		}
		return len(tokens) > 1 && matchesAnyKeyword(tokens[1], "TRAN", "TRANSACTION", "DISTRIBUTED")
	}
	return false
}

func isSetTransactionControl(statement, dialect string, tokens []lexer.Token, services migratorext.Services) bool {
	first := tokens[0]
	if !first.MatchIdentifierValue("SET") {
		return false
	}
	if containsIdentifierSequence(statement, dialect, "TRANSACTION") ||
		containsIdentifierSequence(statement, dialect, "IMPLICIT_TRANSACTIONS") {
		return true
	}
	return services.AutocommitControl && containsIdentifierSequence(statement, dialect, "AUTOCOMMIT")
}

func createsTemporaryObject(tokens []lexer.Token, services migratorext.Services) bool {
	if services.HashTemporaryNames && slices.ContainsFunc(tokens, isHashTemporaryIdentifier) {
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

// isHashTemporaryIdentifier reports whether a token names a temporary object
// by a leading #, bracket-quoted or not.
func isHashTemporaryIdentifier(token lexer.Token) bool {
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
