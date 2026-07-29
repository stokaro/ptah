package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/sqlident"
	"github.com/stokaro/ptah/internal/sqlrunner"
)

const (
	sessionRestoreTimeout = 5 * time.Second
	metadataLockPollDelay = 10 * time.Millisecond
)

var protectedMySQLDatabases = []string{
	"information_schema",
	"metrics_schema",
	"mysql",
	"mysql_innodb_cluster_metadata",
	"mysql_innodb_cluster_metadata_backup",
	"mysql_innodb_cluster_metadata_bkp",
	"mysql_innodb_cluster_metadata_previous",
	"ndbinfo",
	"performance_schema",
	"sys",
}

func quoteIdent(name string) string {
	return sqlident.Quote(platform.MySQL, name)
}

func quoteQualifiedIdent(schema, name string) string {
	return sqlident.Qualified(platform.MySQL, schema, name)
}

// Writer writes schemas to MySQL/MariaDB databases
type Writer struct {
	db            sqlrunner.Runner
	connector     sqlrunner.Connector
	cleanupConn   *sql.Conn
	schema        string
	dialect       string
	serverVersion string
	dryRun        bool
}

type transactionWriter struct {
	mu     sync.Mutex
	tx     *sql.Tx
	schema string
	dryRun bool
}

// NewMySQLWriter creates a new MySQL-family schema writer.
func NewMySQLWriter(db *sql.DB, schema, dialect string) *Writer {
	return NewMySQLWriterWithServerVersion(db, schema, dialect, "")
}

// NewMySQLWriterWithServerVersion creates a MySQL-family schema writer using
// server metadata already resolved by the connection layer.
func NewMySQLWriterWithServerVersion(db *sql.DB, schema, dialect, serverVersion string) *Writer {
	if db == nil {
		return newMySQLWriter(nil, nil, nil, schema, dialect, serverVersion)
	}
	return newMySQLWriter(db, db, nil, schema, dialect, serverVersion)
}

// NewMySQLWriterForRunner creates a writer whose ordinary SQL runs on runner
// while multi-session cleanup acquires auxiliary connections from connector.
func NewMySQLWriterForRunner(
	runner sqlrunner.Runner,
	connector sqlrunner.Connector,
	schema,
	dialect,
	serverVersion string,
) *Writer {
	return newMySQLWriter(runner, connector, nil, schema, dialect, serverVersion)
}

// NewMySQLWriterForPinnedRunner creates a writer whose ordinary SQL and
// primary cleanup operations use the pinned session. connector supplies the
// auxiliary sessions needed for the protected view-drop handoff.
func NewMySQLWriterForPinnedRunner(
	runner sqlrunner.Runner,
	connector sqlrunner.Connector,
	cleanupConn *sql.Conn,
	schema,
	dialect,
	serverVersion string,
) *Writer {
	return newMySQLWriter(runner, connector, cleanupConn, schema, dialect, serverVersion)
}

func newMySQLWriter(
	runner sqlrunner.Runner,
	connector sqlrunner.Connector,
	cleanupConn *sql.Conn,
	schema,
	dialect,
	serverVersion string,
) *Writer {
	return &Writer{
		db:            runner,
		connector:     connector,
		cleanupConn:   cleanupConn,
		schema:        schema,
		dialect:       platform.NormalizeDialect(dialect),
		serverVersion: serverVersion,
	}
}

// ExecuteSQL executes a standalone SQL statement. Values
// must be passed via args and referenced through `?` placeholders; the SQL
// string itself should never be assembled with fmt.Sprintf for value
// interpolation. Identifiers (table/column names) cannot be parameterized
// and must be escaped via quoteIdent before being substituted in.
func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}

	_, err := w.db.ExecContext(ctx, sqlExpr, args...)
	if err != nil {
		return fmt.Errorf("SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// BeginTransaction starts a transaction and returns a transaction-scoped
// writer. The parent writer keeps no active transaction state.
func (w *Writer) BeginTransaction(ctx context.Context) (types.SchemaTransaction, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction")
		return &transactionWriter{schema: w.schema, dryRun: true}, nil
	}
	if w.db == nil {
		return nil, fmt.Errorf("no database connection")
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &transactionWriter{tx: tx, schema: w.schema}, nil
}

// ExecuteSQL executes SQL against the transaction.
func (w *transactionWriter) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tx == nil {
		return fmt.Errorf("transaction is closed")
	}
	_, err := w.tx.ExecContext(ctx, sqlExpr, args...)
	if err != nil {
		return fmt.Errorf("SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// Commit commits the transaction.
func (w *transactionWriter) Commit() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would commit transaction")
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tx == nil {
		return fmt.Errorf("transaction is closed")
	}
	err := w.tx.Commit()
	w.tx = nil
	return err
}

// Rollback rolls back the transaction.
func (w *transactionWriter) Rollback() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would rollback transaction")
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tx == nil {
		return nil
	}
	err := w.tx.Rollback()
	w.tx = nil
	return err
}

// IsDryRun returns whether dry-run mode is enabled.
func (w *transactionWriter) IsDryRun() bool { return w.dryRun }

type cleanupObject struct {
	Name string
	Kind string
}

type cleanupForeignKey struct {
	Table string
	Name  string
}

// DropAllTables drops all user schema objects in the configured database.
func (w *Writer) DropAllTables(ctx context.Context) error {
	return w.withCleanupConnection(ctx, "cleanup", func(conn *sql.Conn) error {
		return w.dropAllTablesOnConnection(ctx, conn)
	})
}

func (w *Writer) withCleanupConnection(
	ctx context.Context,
	label string,
	use func(*sql.Conn) error,
) (resultErr error) {
	if w.dryRun {
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	if w.connector == nil {
		return fmt.Errorf("mysql: cleanup requires a database connection pool")
	}

	conn := w.cleanupConn
	if conn != nil {
		return use(conn)
	}

	conn, err := acquireAuxiliaryConnection(ctx, w.connector, label)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, closeCleanupConnection(conn, label))
	}()
	return use(conn)
}

func (w *Writer) dropAllTablesOnConnection(
	ctx context.Context,
	conn *sql.Conn,
) error {
	schema, err := w.cleanupSchema(ctx, conn)
	if err != nil {
		return err
	}
	serverVersion, err := w.cleanupServerVersion(ctx, conn)
	if err != nil {
		return err
	}
	if w.dialect == platform.MySQL && !supportsMySQLViewTableUsage(serverVersion) {
		return fmt.Errorf(
			"mysql: refusing to clean database %q: server version %q lacks "+
				"information_schema.VIEW_TABLE_USAGE required for complete external-view dependency checks",
			schema,
			serverVersion,
		)
	}
	if err := requireGlobalMetadataVisibility(ctx, conn, schema, w.dialect); err != nil {
		return err
	}
	return withForeignKeyChecksEnabled(ctx, conn, func() error {
		return w.dropDatabaseObjects(ctx, conn, schema)
	})
}

func (w *Writer) dropDatabaseObjects(ctx context.Context, conn *sql.Conn, schema string) error {
	foreignKeys, err := listInternalForeignKeys(ctx, conn, schema)
	if err != nil {
		return err
	}
	objects, err := listCleanupObjects(ctx, conn, schema)
	if err != nil {
		return err
	}
	groups := groupCleanupObjects(objects)

	// Run dependency checks immediately before the first destructive statement.
	// FOREIGN_KEY_CHECKS remains enabled, so a cross-database foreign key
	// created after this preflight still makes DROP TABLE fail.
	if err := rejectExternalForeignKeys(ctx, conn, schema); err != nil {
		return err
	}
	if err := rejectExternalViews(ctx, conn, schema, w.dialect); err != nil {
		return err
	}
	if err := w.dropProtectedViews(ctx, conn, schema, groups.views, groups.tables); err != nil {
		return err
	}
	if err := dropCleanupForeignKeys(ctx, conn, schema, foreignKeys); err != nil {
		return err
	}
	if err := w.dropProtectedObjects(ctx, conn, schema, groups.tables); err != nil {
		return err
	}
	return dropRemainingCleanupObjects(ctx, conn, schema, groups.remaining)
}

func withForeignKeyChecksEnabled(
	ctx context.Context,
	conn *sql.Conn,
	use func() error,
) (resultErr error) {
	foreignKeyChecks, err := readForeignKeyChecks(ctx, conn)
	if err != nil {
		return err
	}
	if foreignKeyChecks {
		return use()
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionRestoreTimeout)
		defer cancel()
		if _, restoreErr := conn.ExecContext(cleanupCtx, "SET FOREIGN_KEY_CHECKS = 0"); restoreErr != nil {
			discardConn(conn)
			resultErr = errors.Join(resultErr, fmt.Errorf("mysql: restore foreign key checks: %w", restoreErr))
		}
	}()
	if _, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		return fmt.Errorf("mysql: enable foreign key checks for cleanup: %w", err)
	}
	return use()
}

type cleanupObjectGroups struct {
	views     []string
	tables    []string
	remaining []cleanupObject
}

func groupCleanupObjects(objects []cleanupObject) cleanupObjectGroups {
	var groups cleanupObjectGroups
	for _, object := range objects {
		switch object.Kind {
		case "VIEW":
			groups.views = append(groups.views, object.Name)
		case "TABLE":
			groups.tables = append(groups.tables, object.Name)
		default:
			groups.remaining = append(groups.remaining, object)
		}
	}
	return groups
}

func dropCleanupForeignKeys(
	ctx context.Context,
	conn *sql.Conn,
	schema string,
	foreignKeys []cleanupForeignKey,
) error {
	for _, foreignKey := range foreignKeys {
		//nolint:gosec // G202: schema and catalog identifiers are emitted only through identifier quoting.
		dropSQL := "ALTER TABLE " + quoteQualifiedIdent(schema, foreignKey.Table) +
			" DROP FOREIGN KEY " + quoteIdent(foreignKey.Name)
		if _, err := conn.ExecContext(ctx, dropSQL); err != nil {
			return fmt.Errorf(
				"failed to drop foreign key %s on table %s: SQL execution failed: %w\nSQL: %s",
				foreignKey.Name,
				foreignKey.Table,
				err,
				dropSQL,
			)
		}
	}
	return nil
}

func dropRemainingCleanupObjects(
	ctx context.Context,
	conn *sql.Conn,
	schema string,
	objects []cleanupObject,
) error {
	// MySQL DDL implicitly commits, so cleanup deliberately avoids a transaction.
	for _, object := range objects {
		//nolint:gosec // G202: schema and object.Name are emitted only through identifier quoting.
		dropSQL := "DROP " + object.Kind + " IF EXISTS " + quoteQualifiedIdent(schema, object.Name)
		if _, err := conn.ExecContext(ctx, dropSQL); err != nil {
			return fmt.Errorf("failed to drop %s %s: SQL execution failed: %w\nSQL: %s",
				strings.ToLower(object.Kind), object.Name, err, dropSQL)
		}
	}
	return nil
}

// DropDatabaseRealm drops the selected MySQL/MariaDB database realm and
// verifies that no supported user object remains.
func (w *Writer) DropDatabaseRealm(ctx context.Context) error {
	if w.dryRun {
		return nil
	}
	return w.withCleanupConnection(ctx, "realm-cleanup", func(conn *sql.Conn) error {
		schema, err := w.cleanupSchema(ctx, conn)
		if err != nil {
			return err
		}
		if err := rejectProtectedMySQLDatabase(schema); err != nil {
			return err
		}
		if err := w.dropAllTablesOnConnection(ctx, conn); err != nil {
			return err
		}
		return w.verifyDatabaseRealm(ctx, conn)
	})
}

func rejectProtectedMySQLDatabase(database string) error {
	if slices.Contains(protectedMySQLDatabases, strings.ToLower(database)) {
		return fmt.Errorf("mysql: refusing to clean protected database %q", database)
	}
	return nil
}

func (w *Writer) verifyDatabaseRealm(ctx context.Context, conn *sql.Conn) error {
	schema, err := w.cleanupSchema(ctx, conn)
	if err != nil {
		return err
	}
	objects, err := listCleanupObjects(ctx, conn, schema)
	if err != nil {
		return err
	}
	if len(objects) > 0 {
		return fmt.Errorf(
			"mysql: database-realm cleanup left %d user objects; first residual object is %s %s",
			len(objects),
			strings.ToLower(objects[0].Kind),
			objects[0].Name,
		)
	}
	return nil
}

func (w *Writer) cleanupServerVersion(ctx context.Context, conn *sql.Conn) (string, error) {
	if strings.TrimSpace(w.serverVersion) != "" {
		return w.serverVersion, nil
	}
	var version string
	if err := conn.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return "", fmt.Errorf("mysql: read server version for cleanup: %w", err)
	}
	return version, nil
}

func requireGlobalMetadataVisibility(
	ctx context.Context,
	conn *sql.Conn,
	schema,
	dialect string,
) error {
	privilegeNames := "SELECT, DROP, ALTER, ALTER ROUTINE, EVENT, LOCK TABLES, and PROCESS"
	if platform.NormalizeDialect(dialect) == platform.MariaDB {
		privilegeNames = "SELECT, DROP, ALTER, ALTER ROUTINE, EVENT, LOCK TABLES, PROCESS, and SHOW VIEW"
	}

	var hasSelect bool
	var hasDrop bool
	var hasAlter bool
	var hasAlterRoutine bool
	var hasEvent bool
	var hasLockTables bool
	var hasProcess bool
	var hasShowView bool
	if err := conn.QueryRowContext(ctx, `
		SELECT
			COALESCE(MAX(privilege_type = 'SELECT'), 0),
			COALESCE(MAX(privilege_type = 'DROP'), 0),
			COALESCE(MAX(privilege_type = 'ALTER'), 0),
			COALESCE(MAX(privilege_type = 'ALTER ROUTINE'), 0),
			COALESCE(MAX(privilege_type = 'EVENT'), 0),
			COALESCE(MAX(privilege_type = 'LOCK TABLES'), 0),
			COALESCE(MAX(privilege_type = 'PROCESS'), 0),
			COALESCE(MAX(privilege_type = 'SHOW VIEW'), 0)
		FROM information_schema.user_privileges
		WHERE grantee = CONCAT(
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', 1)),
			'@',
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', -1))
		)
		  AND privilege_type IN (
		    'SELECT',
		    'DROP',
		    'ALTER',
		    'ALTER ROUTINE',
		    'EVENT',
		    'LOCK TABLES',
		    'PROCESS',
		    'SHOW VIEW'
		  )
	`).Scan(
		&hasSelect,
		&hasDrop,
		&hasAlter,
		&hasAlterRoutine,
		&hasEvent,
		&hasLockTables,
		&hasProcess,
		&hasShowView,
	); err != nil {
		return fmt.Errorf("mysql: prove global metadata visibility: %w", err)
	}
	hasRequiredPrivileges := hasSelect &&
		hasDrop &&
		hasAlter &&
		hasAlterRoutine &&
		hasEvent &&
		hasLockTables &&
		hasProcess
	if platform.NormalizeDialect(dialect) == platform.MariaDB {
		hasRequiredPrivileges = hasRequiredPrivileges && hasShowView
	}
	if !hasRequiredPrivileges {
		return fmt.Errorf(
			"mysql: refusing to clean database %q: global %s privileges are required "+
				"to prove complete metadata visibility and protect destructive DDL",
			schema,
			privilegeNames,
		)
	}

	rows, err := conn.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return fmt.Errorf("mysql: inspect metadata visibility restrictions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return fmt.Errorf("mysql: scan metadata visibility grant: %w", err)
		}
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(grant)), "REVOKE ") {
			return fmt.Errorf(
				"mysql: refusing to clean database %q: partial privilege revokes "+
					"prevent proving complete metadata visibility",
				schema,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("mysql: iterate metadata visibility grants: %w", err)
	}
	return nil
}

func (w *Writer) dropProtectedViews(
	ctx context.Context,
	lockConn *sql.Conn,
	schema string,
	views,
	tables []string,
) (resultErr error) {
	if len(views) == 0 {
		return nil
	}

	dropConn, err := acquireAuxiliaryConnection(ctx, w.connector, "view-drop")
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, closeCleanupConnection(dropConn, "view-drop"))
	}()
	monitorConn, err := acquireAuxiliaryConnection(ctx, w.connector, "metadata-lock monitor")
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, closeCleanupConnection(monitorConn, "metadata-lock monitor"))
	}()

	lockTargets := make([]string, 0, len(views)+len(tables))
	lockTargets = append(lockTargets, views...)
	lockTargets = append(lockTargets, tables...)
	if _, err := lockConn.ExecContext(ctx, buildLockTablesSQL(schema, lockTargets)); err != nil {
		return fmt.Errorf("mysql: lock cleanup views and tables: %w", err)
	}
	locked := true
	defer func() {
		if locked {
			resultErr = errors.Join(resultErr, releaseTableLocks(ctx, lockConn))
		}
	}()

	if err := rejectExternalForeignKeys(ctx, lockConn, schema); err != nil {
		return err
	}
	if err := rejectExternalViews(ctx, lockConn, schema, w.dialect); err != nil {
		return err
	}

	var dropConnectionID int64
	if err := dropConn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&dropConnectionID); err != nil {
		return fmt.Errorf("mysql: read view-drop connection ID: %w", err)
	}
	dropSQL := buildDropObjectsSQL(schema, "VIEW", views)
	dropCtx, cancelDrop := context.WithCancel(ctx)
	defer cancelDrop()
	dropDone := make(chan error, 1)
	go func() {
		_, dropErr := dropConn.ExecContext(dropCtx, dropSQL)
		dropDone <- formatDropObjectsError("VIEW", views, dropSQL, dropErr)
	}()

	dropErr, finished, waitErr := waitForOwnedMetadataLock(
		ctx,
		monitorConn,
		dropConnectionID,
		dropDone,
	)
	if waitErr != nil {
		cancelDrop()
		return errors.Join(waitErr, waitForCanceledViewDrop(dropDone))
	}
	if finished {
		unlockErr := releaseTableLocks(ctx, lockConn)
		locked = false
		return errors.Join(dropErr, unlockErr)
	}
	unlockErr := releaseTableLocks(ctx, lockConn)
	locked = false
	if unlockErr != nil {
		cancelDrop()
		return errors.Join(unlockErr, waitForCanceledViewDrop(dropDone))
	}
	return <-dropDone
}

func waitForOwnedMetadataLock(
	ctx context.Context,
	conn *sql.Conn,
	dropConnectionID int64,
	dropDone <-chan error,
) (dropErr error, finished bool, resultErr error) {
	ticker := time.NewTicker(metadataLockPollDelay)
	defer ticker.Stop()

	for {
		select {
		case dropErr := <-dropDone:
			return dropErr, true, nil
		default:
		}

		var ownedWaiters int
		var otherWaiters int
		if err := conn.QueryRowContext(ctx, `
			SELECT
				COALESCE(SUM(id = ?), 0),
				COALESCE(SUM(id <> ?), 0)
			FROM information_schema.processlist
			WHERE LOWER(COALESCE(state, '')) LIKE '%metadata lock%'
		`, dropConnectionID, dropConnectionID).Scan(&ownedWaiters, &otherWaiters); err != nil {
			return nil, false, fmt.Errorf("mysql: inspect metadata-lock waiters: %w", err)
		}
		if otherWaiters > 0 {
			return nil, false, fmt.Errorf(
				"mysql: refusing view cleanup: %d competing metadata-lock waiters appeared before the protected DROP VIEW handoff",
				otherWaiters,
			)
		}
		if ownedWaiters == 1 {
			return nil, false, nil
		}

		select {
		case dropErr := <-dropDone:
			return dropErr, true, nil
		case <-ctx.Done():
			return nil, false, fmt.Errorf(
				"mysql: wait for protected DROP VIEW metadata lock: %w",
				ctx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func acquireAuxiliaryConnection(
	ctx context.Context,
	connector sqlrunner.Connector,
	label string,
) (*sql.Conn, error) {
	acquireCtx, cancel := context.WithTimeout(ctx, sessionRestoreTimeout)
	defer cancel()
	conn, err := connector.Conn(acquireCtx)
	if err != nil {
		return nil, fmt.Errorf("mysql: acquire %s connection: %w", label, err)
	}
	return conn, nil
}

func waitForCanceledViewDrop(dropDone <-chan error) error {
	timer := time.NewTimer(sessionRestoreTimeout)
	defer timer.Stop()
	select {
	case err := <-dropDone:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-timer.C:
		return fmt.Errorf("mysql: canceled DROP VIEW did not stop within %s", sessionRestoreTimeout)
	}
}

func closeCleanupConnection(conn *sql.Conn, label string) error {
	if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
		return fmt.Errorf("mysql: close %s connection: %w", label, err)
	}
	return nil
}

func (w *Writer) dropProtectedObjects(
	ctx context.Context,
	conn *sql.Conn,
	schema string,
	names []string,
) (resultErr error) {
	if len(names) == 0 {
		return nil
	}

	lockSQL := buildLockTablesSQL(schema, names)
	if _, err := conn.ExecContext(ctx, lockSQL); err != nil {
		return fmt.Errorf("mysql: lock cleanup tables: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, releaseTableLocks(ctx, conn))
	}()

	if err := rejectExternalForeignKeys(ctx, conn, schema); err != nil {
		return err
	}
	if err := rejectExternalViews(ctx, conn, schema, w.dialect); err != nil {
		return err
	}

	dropSQL := buildDropObjectsSQL(schema, "TABLE", names)
	_, err := conn.ExecContext(ctx, dropSQL)
	return formatDropObjectsError("TABLE", names, dropSQL, err)
}

func buildLockTablesSQL(schema string, names []string) string {
	locks := make([]string, 0, len(names))
	for _, name := range names {
		locks = append(locks, quoteQualifiedIdent(schema, name)+" WRITE")
	}
	return "LOCK TABLES " + strings.Join(locks, ", ")
}

func buildDropObjectsSQL(schema, kind string, names []string) string {
	qualifiedNames := make([]string, 0, len(names))
	for _, name := range names {
		qualifiedNames = append(qualifiedNames, quoteQualifiedIdent(schema, name))
	}
	return "DROP " + kind + " IF EXISTS " + strings.Join(qualifiedNames, ", ")
}

func releaseTableLocks(ctx context.Context, conn *sql.Conn) error {
	unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionRestoreTimeout)
	defer cancel()
	if _, err := conn.ExecContext(unlockCtx, "UNLOCK TABLES"); err != nil {
		discardConn(conn)
		return fmt.Errorf("mysql: release cleanup table locks: %w", err)
	}
	return nil
}

func formatDropObjectsError(kind string, names []string, dropSQL string, err error) error {
	if err == nil {
		return nil
	}
	objectLabel := strings.ToLower(kind)
	if len(names) > 1 {
		objectLabel += "s"
	}
	return fmt.Errorf(
		"failed to drop %s %s: SQL execution failed: %w\nSQL: %s",
		objectLabel,
		strings.Join(names, ", "),
		err,
		dropSQL,
	)
}

func (w *Writer) cleanupSchema(ctx context.Context, conn *sql.Conn) (string, error) {
	if strings.TrimSpace(w.schema) != "" {
		return w.schema, nil
	}
	var schema sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
		return "", fmt.Errorf("mysql: read current database: %w", err)
	}
	if !schema.Valid || strings.TrimSpace(schema.String) == "" {
		return "", fmt.Errorf("mysql: cleanup requires a selected database")
	}
	return schema.String, nil
}

func readForeignKeyChecks(ctx context.Context, conn *sql.Conn) (bool, error) {
	var value bool
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.FOREIGN_KEY_CHECKS").Scan(&value); err != nil {
		return false, fmt.Errorf("mysql: read foreign key checks: %w", err)
	}
	return value, nil
}

func rejectExternalForeignKeys(ctx context.Context, conn *sql.Conn, schema string) error {
	var count int
	if err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT DISTINCT constraint_schema, constraint_name
			FROM information_schema.key_column_usage
			WHERE referenced_table_schema = ?
			  AND constraint_schema <> ?
		) AS external_foreign_keys
	`, schema, schema).Scan(&count); err != nil {
		return fmt.Errorf("mysql: inspect cross-database foreign keys: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("mysql: refusing to clean database %q: %d foreign key constraints from other databases reference it", schema, count)
	}
	return nil
}

func listInternalForeignKeys(
	ctx context.Context,
	conn *sql.Conn,
	schema string,
) ([]cleanupForeignKey, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT table_name, constraint_name
		FROM (
			SELECT DISTINCT table_name, constraint_name
			FROM information_schema.key_column_usage
			WHERE constraint_schema = ?
			  AND referenced_table_name IS NOT NULL
		) AS internal_foreign_keys
		ORDER BY table_name, constraint_name
	`, schema)
	if err != nil {
		return nil, fmt.Errorf("mysql: query internal foreign keys: %w", err)
	}
	defer rows.Close()

	var foreignKeys []cleanupForeignKey
	for rows.Next() {
		var foreignKey cleanupForeignKey
		if err := rows.Scan(&foreignKey.Table, &foreignKey.Name); err != nil {
			return nil, fmt.Errorf("mysql: scan internal foreign key: %w", err)
		}
		foreignKeys = append(foreignKeys, foreignKey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: iterate internal foreign keys: %w", err)
	}

	return foreignKeys, nil
}

func rejectExternalViews(ctx context.Context, conn *sql.Conn, schema, dialect string) error {
	count, err := externalViewCount(ctx, conn, schema, dialect)
	if err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf(
			"mysql: refusing to clean database %q: %d views from other databases reference it",
			schema,
			count,
		)
	}
	return nil
}

func externalViewCount(ctx context.Context, conn *sql.Conn, schema, dialect string) (int, error) {
	if platform.NormalizeDialect(dialect) == platform.MariaDB {
		return mariaDBExternalViewCount(ctx, conn, schema)
	}
	return mySQLExternalViewCount(ctx, conn, schema)
}

func supportsMySQLViewTableUsage(version string) bool {
	numericVersion, _, _ := strings.Cut(version, "-")
	parts := strings.Split(numericVersion, ".")
	if len(parts) < 3 {
		return false
	}
	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(parts[1])
	patch, patchErr := strconv.Atoi(parts[2])
	if majorErr != nil || minorErr != nil || patchErr != nil {
		return false
	}
	if major != 8 {
		return major > 8
	}
	return minor > 0 || patch >= 13
}

func mySQLExternalViewCount(ctx context.Context, conn *sql.Conn, schema string) (int, error) {
	var count int
	if err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT DISTINCT view_schema, view_name
			FROM information_schema.view_table_usage
			WHERE table_schema = ?
			  AND view_schema <> ?
		) AS external_views
	`, schema, schema).Scan(&count); err != nil {
		return 0, fmt.Errorf("mysql: inspect cross-database views: %w", err)
	}
	return count, nil
}

func mariaDBExternalViewCount(ctx context.Context, conn *sql.Conn, schema string) (int, error) {
	// MariaDB has no information_schema.view_table_usage relation. It
	// normalizes table references in VIEW_DEFINITION to fully qualified,
	// backtick-quoted names, so the exact database qualifier is a conservative
	// dependency signal. Keeping the qualifier as a bound value avoids mixing
	// metadata values into the query text.
	var count int
	if err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.views
		WHERE table_schema <> ?
		  AND INSTR(view_definition, ?) > 0
	`, schema, quoteIdent(schema)+".").Scan(&count); err != nil {
		return 0, fmt.Errorf("mariadb: inspect cross-database views: %w", err)
	}
	return count, nil
}

func listCleanupObjects(ctx context.Context, conn *sql.Conn, schema string) ([]cleanupObject, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT object_name, object_kind
		FROM (
			SELECT
				table_name AS object_name,
				CASE table_type
					WHEN 'VIEW' THEN 'VIEW'
					WHEN 'SEQUENCE' THEN 'SEQUENCE'
					ELSE 'TABLE'
				END AS object_kind,
				CASE table_type
					WHEN 'VIEW' THEN 10
					WHEN 'SEQUENCE' THEN 40
					ELSE 30
				END AS priority
			FROM information_schema.tables
			WHERE table_schema = ?
			  AND table_type IN ('BASE TABLE', 'VIEW', 'SEQUENCE', 'SYSTEM VERSIONED')

			UNION ALL

			SELECT routine_name, routine_type, 20
			FROM information_schema.routines
			WHERE routine_schema = ?

			UNION ALL

			SELECT event_name, 'EVENT', 0
			FROM information_schema.events
			WHERE event_schema = ?
		) AS cleanup_objects
		ORDER BY priority, object_kind, object_name
	`, schema, schema, schema)
	if err != nil {
		return nil, fmt.Errorf("mysql: query schema objects: %w", err)
	}
	defer rows.Close()

	var objects []cleanupObject
	for rows.Next() {
		var object cleanupObject
		if err := rows.Scan(&object.Name, &object.Kind); err != nil {
			return nil, fmt.Errorf("mysql: scan schema object: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: iterate schema objects: %w", err)
	}

	return objects, nil
}

func discardConn(conn *sql.Conn) {
	// Never return a connection with unknown session state to the pool.
	_ = conn.Raw(func(any) error {
		return driver.ErrBadConn
	})
}

// isCreateTableStatement checks if a SQL statement is a CREATE TABLE statement
func (w *Writer) isCreateTableStatement(sqlExpr string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlExpr)), "CREATE TABLE")
}

// isCreateIndexStatement checks if a SQL statement is a CREATE INDEX statement
func (w *Writer) isCreateIndexStatement(sqlExpr string) bool {
	return strings.Contains(strings.ToUpper(strings.TrimSpace(sqlExpr)), "CREATE") &&
		strings.Contains(strings.ToUpper(strings.TrimSpace(sqlExpr)), "INDEX")
}

// extractTableNameFromCreateTable extracts table name from CREATE TABLE statement
func (w *Writer) extractTableNameFromCreateTable(sqlExpr string) string {
	// Simple regex to extract table name from "CREATE TABLE tablename ("
	parts := strings.Fields(strings.TrimSpace(sqlExpr))
	if len(parts) >= 3 && strings.ToUpper(parts[0]) == "CREATE" && strings.ToUpper(parts[1]) == "TABLE" {
		return strings.TrimSuffix(parts[2], "(")
	}
	return ""
}

// extractTableNameFromCreateIndex extracts table name from CREATE INDEX statement
func (w *Writer) extractTableNameFromCreateIndex(sqlExpr string) string {
	// Look for "ON tablename" pattern
	parts := strings.Fields(strings.TrimSpace(sqlExpr))
	for i, part := range parts {
		if strings.ToUpper(part) == "ON" && i+1 < len(parts) {
			return strings.TrimSuffix(parts[i+1], "(")
		}
	}
	return ""
}

// tableExists checks if a table exists in the database
func (w *Writer) tableExists(tableName string) bool { //nolint:unused // TODO: verify why this is not used
	if w.dryRun {
		// In dry run mode, assume table doesn't exist to show all operations
		return false
	}

	var exists bool
	checkSQL := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = DATABASE() AND table_name = ?
		)`

	err := w.db.QueryRow(checkSQL, tableName).Scan(&exists)
	return err == nil && exists
}

// SetDryRun enables or disables dry run mode
func (w *Writer) SetDryRun(dryRun bool) {
	w.dryRun = dryRun
}

// IsDryRun returns whether dry run mode is enabled
func (w *Writer) IsDryRun() bool {
	return w.dryRun
}
