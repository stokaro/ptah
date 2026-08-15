package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
)

// MigrationStatus represents the current state of migrations
type MigrationStatus struct {
	CurrentVersion          int64              `json:"current_version"`
	CurrentVersionKey       string             `json:"current_version_key,omitempty"`
	CurrentVersionKeySet    bool               `json:"-"`
	AppliedMigrations       []int64            `json:"applied_migrations"`
	AppliedMigrationKeys    []string           `json:"applied_migration_keys,omitempty"`
	PendingMigrations       []int64            `json:"pending_migrations"`
	PendingMigrationKeys    []string           `json:"pending_migration_keys,omitempty"`
	OutOfOrderMigrations    []int64            `json:"out_of_order_migrations"`
	OutOfOrderMigrationKeys []string           `json:"out_of_order_migration_keys,omitempty"`
	TotalMigrations         int                `json:"total_migrations"`
	HasPendingChanges       bool               `json:"has_pending_changes"`
	DirtyRevision           *MigrationRevision `json:"dirty_revision,omitempty"`
}

// MigrationStatusSnapshot contains a migration status and the revision rows
// used to derive it.
type MigrationStatusSnapshot struct {
	Status    *MigrationStatus
	Revisions []MigrationRevision
}

// MigrationDirection identifies the migration direction in a selected plan.
type MigrationDirection string

const (
	// MigrationDirectionUp applies pending migrations.
	MigrationDirectionUp MigrationDirection = "up"
	// MigrationDirectionDown rolls applied migrations back.
	MigrationDirectionDown MigrationDirection = "down"
)

// MigrationPlan describes the migration work selected while holding the
// migration lock.
type MigrationPlan struct {
	Direction            MigrationDirection
	CurrentVersion       int64
	CurrentVersionKey    string
	CurrentVersionKeySet bool
	TargetVersion        int64
	TargetVersionKey     string
	Versions             []int64
	VersionKeys          []string
}

// PreMigrationHook runs after the migrator has acquired its migration lock and
// selected the final migration plan, but before it changes schema or revision
// state.
type PreMigrationHook func(ctx context.Context, plan MigrationPlan) error

// MigrationPlanObserver sees the final migration plan while the migration lock
// is held, before static transaction-mode validation. It is intended for
// metadata capture only; unlike PreMigrationHook, it cannot abort execution.
type MigrationPlanObserver func(ctx context.Context, plan MigrationPlan)

// MigrateUpOptions selects the pending up migration plan.
type MigrateUpOptions struct {
	// TargetVersion limits the run to pending migrations at or below this
	// version. Zero means latest.
	TargetVersion int64
	// Amount limits the run to the first N pending migrations after exec-order
	// and target-version filtering. Zero means all selected migrations.
	Amount uint64
	// AllowDirty skips the default dirty revision guard and requests recovery of
	// a pending dirty migration. It does not bypass committed-prefix verification;
	// in exact Atlas identity mode, retired rows the current provider no longer
	// owns remain blocking. Callers should expose it only as an explicit recovery
	// action.
	AllowDirty bool
	// DiscardRolledBackFailure removes the Atlas revision row written for a
	// failed up migration only when this invocation observed a successful
	// transaction rollback. Existing dirty revisions and uncertain commit or
	// rollback outcomes remain recorded and block automatic retry.
	DiscardRolledBackFailure bool
	// AssumedAppliedVersions are treated as applied for plan selection without
	// reading or writing revision metadata. This is intended for dry-run paths
	// that need to model metadata-only operations such as baseline.
	AssumedAppliedVersions []int64
	// AssumedAppliedVersionKeys carries the exact revision identities aligned
	// with AssumedAppliedVersions. A present empty key is exact, not a numeric
	// fallback. An omitted entry keeps the numeric identity.
	AssumedAppliedVersionKeys []string
	// Preflight runs after the migration lock is acquired and the final plan is
	// selected, but before any schema or revision changes.
	Preflight PreMigrationHook
	// PlanObserver sees the selected plan under the migration lock before
	// transaction-mode validation. It runs even for an empty plan so callers
	// can replace metadata captured before lock acquisition.
	PlanObserver MigrationPlanObserver
	// ChecksDeferredObserver receives the versions whose pre-migration checks
	// were parsed and statically validated but not evaluated against the
	// database, because a dry run cannot produce the state they are about. It
	// runs after a successful run and only when the list is non-empty, so a
	// preview can say how much of the guard it did not answer instead of
	// dropping it silently.
	ChecksDeferredObserver ChecksDeferredObserver
}

// ChecksDeferredObserver is notified with the migration versions whose
// pre-migration checks a run declined to evaluate. The slice is owned by the
// caller and must not be retained.
type ChecksDeferredObserver func(ctx context.Context, versions []int64)

// Migrator handles database migrations for ptah
type Migrator struct {
	conn                 *dbschema.DatabaseConnection
	noTransactionSession *dbschema.DatabaseConnection
	migrationProvider    MigrationProvider
	defaultTimeouts      MigrationTimeouts
	migrationsTable      string
	migrationsSchema     string
	revisionTableFormat  RevisionTableFormat
	execOrder            ExecOrder
	outOfOrderExempt     []int64
	sourceVersions       map[int64]string
	atlasRevisionCompare AtlasRevisionVersionComparator
	txMode               MigrationTxMode
	migrationLockName    string
	migrationLockTimeout time.Duration
	migrationLockSkipped bool
	initialized          bool
	// initializedDryRun records the writer's dry-run mode at the time
	// initialized was set, so the memoized state is never reused across a
	// mode change.
	initializedDryRun        bool
	logger                   *slog.Logger
	observer                 Observer
	skipChecks               bool
	metadataAvailable        bool
	legacyRevisionTable      bool
	postgresIndexObservation *postgresIndexObservation
}

// NewFSMigrator creates a new migrator that loads migrations from a filesystem.
// It scans the provided filesystem for migration files following the naming convention
// NNNNNNNNNN_description.up.sql and NNNNNNNNNN_description.down.sql and automatically
// registers them with the migrator. Returns an error if the filesystem cannot be scanned
// or if any migrations are incomplete (missing up or down files).
func NewFSMigrator(conn *dbschema.DatabaseConnection, fsys fs.FS, opts ...FSProviderOption) (*Migrator, error) {
	provider, err := NewFSMigrationProvider(fsys, opts...)
	if err != nil {
		return nil, err
	}
	return NewMigrator(conn, provider), nil
}

// NewMigrator creates a new migrator with the given database connection
func NewMigrator(conn *dbschema.DatabaseConnection, provider MigrationProvider) *Migrator {
	return &Migrator{
		conn:                conn,
		migrationProvider:   provider,
		migrationsTable:     defaultPtahMigrationsTable,
		revisionTableFormat: RevisionTableFormatPtah,
		execOrder:           ExecOrderLinear,
		txMode:              MigrationTxModeFile,
		migrationLockName:   migrationAdvisoryLockName,
		logger:              slog.Default(),
		observer:            NoopObserver{},
	}
}

// WithLogger sets the logger for the migrator
func (m *Migrator) WithLogger(l *slog.Logger) *Migrator {
	tmp := *m
	if l == nil {
		l = slog.Default()
	}
	tmp.logger = l
	return &tmp
}

// WithObserver sets the migration observer used for tracing and metrics.
func (m *Migrator) WithObserver(observer Observer) *Migrator {
	tmp := *m
	if observer == nil {
		observer = NoopObserver{}
	}
	tmp.observer = observer
	return &tmp
}

// WithSkipChecks controls whether pre-migration `-- +ptah check` assertions and
// Atlas txtar checks.sql and checks/*.sql sections are evaluated before
// applying up migrations. The default (false) enforces checks; pass true as an
// explicit emergency bypass, mirroring --allow-destructive.
func (m *Migrator) WithSkipChecks(skip bool) *Migrator {
	tmp := *m
	tmp.skipChecks = skip
	return &tmp
}

// migrationCheckGroups collects a migration's pre-migration checks: Atlas
// txtar check files first (in archive order), then `-- +ptah check` directives
// parsed from the up SQL.
func (m *Migrator) migrationCheckGroups(migration *Migration) ([]checkGroup, error) {
	dialect := m.connectionDialect()
	groups := make([]checkGroup, 0, len(migration.atlasCheckFiles)+1)
	for _, file := range migration.atlasCheckFiles {
		mode := atlasCheckFileMode(file.sql, dialect)
		checks := parseAtlasTxtarChecks(file.name, file.sql, dialect)
		if len(checks) == 0 && mode != checkGroupOneOf {
			continue
		}
		groups = append(groups, checkGroup{
			name:   file.name,
			checks: checks,
			mode:   mode,
		})
	}

	parsed, err := ParseChecks(migration.UpSQL, dialect)
	if err != nil {
		return nil, fmt.Errorf("migration %d has invalid pre-migration check directives: %w", migration.Version, err)
	}
	if len(parsed) > 0 {
		groups = append(groups, checkGroup{checks: parsed, mode: checkGroupAll})
	}
	return groups, nil
}

// runMigrationChecks evaluates the pre-migration assertion checks embedded in a
// migration's up SQL (`-- +ptah check` directives and Atlas txtar checks.sql
// sections) against conn, before any body statement runs. It is a no-op when
// checks are skipped. A malformed check directive or an unsatisfied assertion
// returns an error so the caller aborts with nothing applied.
func (m *Migrator) runMigrationChecks(ctx context.Context, conn *dbschema.DatabaseConnection, migration *Migration) error {
	if m.skipChecks {
		return nil
	}
	groups, err := m.migrationCheckGroups(migration)
	if err != nil {
		return err
	}
	info := conn.Info()
	return runCheckGroups(ctx, conn, info.Dialect, info.Version, migration.Version, groups)
}

// validateDeferredMigrationChecks statically validates the checks of a
// migration whose assertions a dry run is about to defer, and reports whether
// the migration declared any. It never queries the database.
//
// Deferring evaluation must not mean dropping the check from the report
// entirely: whether an assertion is malformed or write-shaped is decided by its
// text, so that verdict is as available in a dry run as in a real apply and is
// still worth failing on. Only the part of the verdict that needs state — does
// the predicate hold? — is deferred.
func (m *Migrator) validateDeferredMigrationChecks(migration *Migration) (bool, error) {
	if m.skipChecks {
		return false, nil
	}
	groups, err := m.migrationCheckGroups(migration)
	if err != nil {
		return false, err
	}
	if len(groups) == 0 {
		return false, nil
	}
	info := m.conn.Info()
	if err := validateCheckGroups(groups, info.Dialect, info.Version, migration.Version); err != nil {
		return false, err
	}
	return true, nil
}

// deferPreMigrationChecks reports whether a migration's assertions must not be
// evaluated against the live database.
//
// A pre-migration check is a read, and a dry run intercepts only writes, so
// every check in a dry run is evaluated for real against a database the dry run
// has refused to change. That is sound for exactly one migration: the first one
// executed in the run observes precisely the state a real apply would give it.
// Every later migration's precondition is asked about state that only exists
// once its predecessors apply — state the dry run has, by construction, refused
// to produce — so a failure there is an artifact of the preview rather than a
// finding about the migrations (#1005).
//
// Position in the RUN decides this, not version and not file order: a migration
// sitting second in its directory is first in the run once its predecessor is
// already applied, and its checks are accurate again.
func (m *Migrator) deferPreMigrationChecks(observesApplyState bool) bool {
	return !observesApplyState && m.conn.Writer().IsDryRun()
}

// rejectChecksUnderTxModeAll refuses a migration that declares pre-migration
// checks when running with tx-mode all. Under a single shared transaction a
// check reads committed pre-batch state on the pool connection and cannot see
// earlier batched migrations' uncommitted changes, so it would silently
// evaluate a precondition against stale state. Bypassing checks lifts the
// restriction.
//
// A dry run is NOT exempt, and the reason is what a preview is for. The refusal
// is decidable without touching the database -- tx-mode is all, the migration
// declares checks, checks are not skipped -- so the real apply of the same
// directory refuses deterministically. Exempting the preview made it report
// "Would have applied 2 migrations." with an empty stderr for a run that cannot
// succeed, which is worse than not previewing at all.
//
// "No batch transaction executes here" answers whether the check could be
// evaluated. A preview answers what the real run will do.
func (m *Migrator) rejectChecksUnderTxModeAll(migration *Migration) error {
	if m.skipChecks {
		return nil
	}
	groups, err := m.migrationCheckGroups(migration)
	if err != nil {
		return err
	}
	if len(groups) > 0 {
		return fmt.Errorf("migration %d declares pre-migration checks, which cannot run with tx-mode all; use the default per-file transaction mode", migration.Version)
	}
	return nil
}

// WithExecOrder sets how this migrator handles pending migrations whose
// version is below the current high-water mark.
func (m *Migrator) WithExecOrder(execOrder ExecOrder) *Migrator {
	tmp := *m
	tmp.execOrder = normalizeExecOrder(execOrder)
	return &tmp
}

// WithOutOfOrderExempt exempts specific versions from the linear execution
// guard. It is an escape hatch, and using it wrongly disables a safety check
// silently, so read this before reaching for it.
//
// WHAT THE GUARD ASSUMES. Under [ExecOrderLinear] the migrator refuses a pending
// migration whose version is below the highest version already applied, and
// under [ExecOrderLinearSkip] it leaves such a migration unapplied. Both treat
// "version below the current one" as evidence that the migration was AUTHORED
// before what is already in the database — someone branched, and their migration
// arrived late. That inference is only sound while the version is a chronology:
// a number that increases as migrations are written.
//
// WHEN IT IS NOT A CHRONOLOGY. A migration directory laid out in another tool's
// convention is converted in memory, and its Atlas version is whatever that
// conversion assigns. For Flyway the version is a projection of Atlas CE's
// atlas.sum ORDER, which is not authoring order: a surviving baseline is emitted
// and executed FIRST whatever its own version — measured, and measured to hold
// across runs and not only within a single conversion — so it is deliberately
// placed below every migration it squashes. On a database that already has
// migrations recorded it therefore sorts below all of them and trips a guard
// that has nothing to guard against. Exempting that one version is what this
// method is for; see atlasmigrateimport.FlywaySurvivingBaseline.
//
// HOW TO MISUSE IT. The exempt list is taken on trust. Supplying a version that
// IS chronological — an ordinary migration that really was authored late —
// silently turns off out-of-order detection for it: no error, no warning, it
// simply applies, and under linear-skip it is no longer skipped either. Pass
// only versions whose position you computed yourself and know to be an artifact
// of a layout projection. Anything derived from user input, or a whole band of
// versions rather than the specific ones, defeats the guard for real branching
// mistakes, which is the failure it exists to catch.
//
// It changes only whether the guard refuses. Execution order is still version
// order, and an exempt migration is applied in its numeric position like any
// other.
func (m *Migrator) WithOutOfOrderExempt(versions []int64) *Migrator {
	tmp := *m
	tmp.outOfOrderExempt = slices.Clone(versions)
	return &tmp
}

// WithSourceVersions supplies, for a migration directory converted from another
// tool's layout, the SOURCE version token each executed version was projected
// from. It makes the linear execution guard stricter; it never makes it looser.
//
// WHY A SECOND KEY EXISTS AT ALL. The int64 version a converted directory
// executes under is a projection of the source tool's ORDER. For Flyway that
// order is numeric on the version components — V2 executes before V10 — and
// reproducing it is what lets Ptah write the same atlas.sum and run the same
// sequence as the tool it is standing in for. But "was this migration added
// after everything already applied" is not an ordering question, and the source
// tool does not answer it with the ordering: Flyway's version token is compared
// as a STRING, where "10" sorts below "2". Supplying the token lets the guard
// ask the linearity question on the operand that decides it, while execution
// order, atlas.sum and every recorded revision keep the int64 they already have
// (stokaro/ptah#1098).
//
// The two comparisons are unioned. A pending migration is refused under
// [ExecOrderLinear], and left unapplied under [ExecOrderLinearSkip], when its
// version sorts below the current one OR its token does not sort above every
// applied token. Neither half subsumes the other: V3 added to a database
// holding V2 and V10 is caught only by the first, and V10 added to a database
// holding V2 only by the second.
//
// Passing tokens for a NATIVE Atlas directory would be a mistake rather than a
// no-op: there the int64 is the version, not a projection of one, and comparing
// its decimal spelling as text would refuse ordinary sequences. Only the
// converted apply path sets this; see
// atlasmigrateimport.FlywaySourceVersions.
//
// Versions absent from the map are governed by the numeric comparison alone, so
// a partial map narrows what the guard can see rather than corrupting it.
func (m *Migrator) WithSourceVersions(sourceVersions map[int64]string) *Migrator {
	tmp := *m
	tmp.sourceVersions = maps.Clone(sourceVersions)
	return &tmp
}

// WithAtlasRevisionVersionComparator supplies the source format's ordering
// rule for exact revision identities that no migration in the current provider
// owns. SetRevision uses it only to decide whether retired exact history lies
// above the selected target. Each [AtlasRevisionOrderIdentity] carries the row
// type and operator marker needed to preserve a source role when one was
// recorded. The comparator returns a negative value when left precedes right
// and a positive value when it follows right. Its bool result must be false
// when the pair cannot be ordered without missing source context; SetRevision
// then refuses before changing metadata rather than guessing from the identity
// bytes.
func (m *Migrator) WithAtlasRevisionVersionComparator(
	compare AtlasRevisionVersionComparator,
) *Migrator {
	tmp := *m
	tmp.atlasRevisionCompare = compare
	return &tmp
}

// WithTransactionMode sets how pending up migrations are wrapped in
// transactions.
func (m *Migrator) WithTransactionMode(mode MigrationTxMode) *Migrator {
	tmp := *m
	tmp.txMode = normalizeMigrationTxMode(mode)
	return &tmp
}

// WithMigrationsTable sets the table used to record applied migrations.
func (m *Migrator) WithMigrationsTable(schema, table string) *Migrator {
	tmp := *m
	tmp.migrationsSchema = strings.TrimSpace(schema)
	tmp.migrationsTable = strings.TrimSpace(table)
	if tmp.migrationsTable == "" {
		tmp.migrationsTable = tmp.defaultMigrationsTable()
	}
	tmp.initialized = false
	tmp.initializedDryRun = false
	tmp.metadataAvailable = false
	tmp.legacyRevisionTable = false
	return &tmp
}

// WithRevisionTableFormat sets the database table layout used for migration
// revisions. Both layouts retain Ptah's dirty-state protection; the Atlas
// layout encodes rollback direction in its existing operator_version column.
func (m *Migrator) WithRevisionTableFormat(format RevisionTableFormat) *Migrator {
	tmp := *m
	tmp.revisionTableFormat = format
	if tmp.migrationsTable == "" || tmp.migrationsTable == defaultPtahMigrationsTable {
		tmp.migrationsTable = tmp.defaultMigrationsTable()
	}
	tmp.initialized = false
	tmp.initializedDryRun = false
	tmp.metadataAvailable = false
	tmp.legacyRevisionTable = false
	return &tmp
}

func (m *Migrator) defaultMigrationsTable() string {
	if m.revisionTableFormat.isAtlas() {
		return defaultAtlasRevisionsTable
	}
	return defaultPtahMigrationsTable
}

func (m *Migrator) qualifiedMigrationsTable() string {
	table := m.migrationsTableName()
	schema := m.metadataTableSchemaName()
	if schema == "" {
		return m.quoteIdentifier(table)
	}
	return m.quoteIdentifier(schema) + "." + m.quoteIdentifier(table)
}

// MigrationsTableIdentifier returns the dialect-quoted metadata table name.
func (m *Migrator) MigrationsTableIdentifier() string {
	return m.qualifiedMigrationsTable()
}

func (m *Migrator) migrationsSchemaStatement() string {
	schema := m.migrationsSchema
	if m.isSQLServer() {
		schema = m.metadataTableSchemaName()
		if strings.EqualFold(schema, "dbo") {
			return ""
		}
	}
	if schema == "" {
		return ""
	}
	if platform.NormalizeDialect(m.connectionDialect()) == platform.SQLite {
		return ""
	}
	if m.isSQLServer() {
		return fmt.Sprintf(
			"IF SCHEMA_ID(%s) IS NULL EXEC(%s)",
			sqlStringLiteral(schema),
			sqlStringLiteral("CREATE SCHEMA "+m.quoteIdentifier(schema)),
		)
	}
	return "CREATE SCHEMA IF NOT EXISTS " + m.quoteIdentifier(schema)
}

func (m *Migrator) quoteIdentifier(identifier string) string {
	if m.conn == nil {
		return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
	}
	switch m.conn.Info().Dialect {
	case "mysql", "mariadb", "clickhouse":
		return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
	case platform.SQLServer:
		return "[" + strings.ReplaceAll(identifier, "]", "]]") + "]"
	default:
		return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
	}
}

func (m *Migrator) isSQLServer() bool {
	return m.conn != nil && m.conn.Info().Dialect == platform.SQLServer
}

func (m *Migrator) connectionDialect() string {
	if m.conn == nil {
		return ""
	}
	return m.conn.Info().Dialect
}

func (m *Migrator) connectionSchemaName() string {
	if m.conn == nil {
		return ""
	}
	return m.conn.Info().Schema
}

func sqlStringLiteral(value string) string {
	return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func (m *Migrator) createMigrationsTableSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return m.createAtlasRevisionsTableSQL()
	}
	if m.isSQLServer() {
		return fmt.Sprintf(`IF OBJECT_ID(%s, N'U') IS NULL
BEGIN
    CREATE TABLE %s (
        version BIGINT PRIMARY KEY,
        description NVARCHAR(MAX) NOT NULL,
        applied_at DATETIME2 NOT NULL,
        state NVARCHAR(32) NOT NULL DEFAULT 'applied',
        applied INT NOT NULL DEFAULT 1,
        total INT NOT NULL DEFAULT 1,
        error NVARCHAR(MAX) NULL,
        error_stmt NVARCHAR(MAX) NULL,
        execution_time_ms BIGINT NOT NULL DEFAULT 0,
        checksum NVARCHAR(64) NOT NULL DEFAULT ''
    )
END`, sqlStringLiteral(m.sqlServerObjectName()), m.qualifiedMigrationsTable())
	}
	engineClause := ""
	if implicitCommitDialect(m.connectionDialect()) {
		engineClause = " ENGINE=InnoDB"
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied INTEGER NOT NULL DEFAULT 1,
    total INTEGER NOT NULL DEFAULT 1,
    error TEXT NULL,
    error_stmt TEXT NULL,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL DEFAULT ''
)%s`, m.qualifiedMigrationsTable(), engineClause)
}

func (m *Migrator) getVersionSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(
			"SELECT COALESCE(MAX(%s), 0) FROM %s",
			m.atlasVersionNumberExpression(),
			m.qualifiedMigrationsTable(),
		)
	}
	if m.legacyRevisionTable {
		return fmt.Sprintf("SELECT COALESCE(MAX(version), 0) FROM %s", m.qualifiedMigrationsTable())
	}
	return fmt.Sprintf("SELECT COALESCE(MAX(version), 0) FROM %s WHERE state = 'applied'", m.qualifiedMigrationsTable())
}

func (m *Migrator) getAppliedMigrationsSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(
			"SELECT version FROM %s WHERE %s AND %s ORDER BY %s, version",
			m.qualifiedMigrationsTable(),
			atlasAppliedRevisionPredicate,
			m.atlasRevisionRowPredicate(),
			m.atlasVersionNumberExpression(),
		)
	}
	if m.legacyRevisionTable {
		return fmt.Sprintf("SELECT version FROM %s ORDER BY version", m.qualifiedMigrationsTable())
	}
	return fmt.Sprintf("SELECT version FROM %s WHERE state = 'applied' ORDER BY version", m.qualifiedMigrationsTable())
}

func (m *Migrator) deleteMigrationSQL() string {
	return fmt.Sprintf("DELETE FROM %s WHERE version = ?", m.qualifiedMigrationsTable())
}

// Initialize creates the migrations table if it doesn't exist
func (m *Migrator) Initialize(ctx context.Context) error {
	dryRun := m.conn.Writer().IsDryRun()

	// Skip if already initialized. The memoized result is only valid for the
	// dry-run mode it was computed under: a real Initialize records that the
	// metadata now exists, while a dry-run Initialize only records what the
	// metadata looks like, so a writer that later leaves dry-run mode must
	// still get its table created.
	if m.initialized && m.initializedDryRun == dryRun {
		return nil
	}

	if dryRun {
		return m.initializeDryRun(ctx)
	}

	if schemaSQL := m.migrationsSchemaStatement(); schemaSQL != "" {
		// Deliberately outside the migration writer: Initialize runs before any
		// per-migration transaction exists. Dry-run returns above so metadata DDL
		// is never written when callers asked for a simulation.
		if _, err := m.conn.ExecContext(ctx, schemaSQL); err != nil {
			return fmt.Errorf("failed to create migrations schema: %w", err)
		}
	}

	// Deliberately outside the migration writer for the same reason as schema
	// creation: there is no active migration transaction yet.
	if _, err := m.conn.ExecContext(ctx, m.createMigrationsTableSQL()); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}
	// Check the engine before upgrading an existing table. ALTER TABLE itself
	// commits on the MySQL family, so validating afterward could mutate metadata
	// that Ptah has already decided is unsafe to use as a transaction witness.
	if err := m.requireTransactionalMetadataEngine(ctx); err != nil {
		return err
	}
	if m.revisionTableFormat.isAtlas() {
		if err := m.validateAtlasRevisionIdentityCollation(ctx); err != nil {
			return err
		}
	}
	if !m.revisionTableFormat.isAtlas() {
		if err := m.ensureMigrationsVersionColumn(ctx); err != nil {
			return fmt.Errorf("failed to prepare migrations version column: %w", err)
		}
		if err := m.ensureMigrationsRevisionColumns(ctx); err != nil {
			return fmt.Errorf("failed to prepare migrations revision columns: %w", err)
		}
	}

	// Mark as initialized
	m.initialized = true
	m.initializedDryRun = false
	m.metadataAvailable = true
	m.legacyRevisionTable = false
	return nil
}

// initializeDryRun records what a real Initialize would find without writing
// anything. Like the real path it memoizes its result: every read entry point
// calls Initialize, so without memoization a single dry run re-inspects the
// metadata table — and repeats the "[DRY RUN] Would initialize" narration —
// once per read (stokaro/ptah#967).
func (m *Migrator) initializeDryRun(ctx context.Context) error {
	available, legacy, err := m.inspectDryRunMetadata(ctx)
	if err != nil {
		return err
	}
	if !available {
		m.logger.Info("[DRY RUN] Would initialize migrations metadata", "table", m.qualifiedMigrationsTable())
	}

	m.metadataAvailable = available
	m.legacyRevisionTable = legacy
	m.initialized = true
	m.initializedDryRun = true
	return nil
}

// inspectDryRunMetadata reports whether the revision metadata a dry run would
// read is present, and whether it still uses the legacy Ptah layout.
func (m *Migrator) inspectDryRunMetadata(ctx context.Context) (available, legacy bool, err error) {
	exists, err := m.migrationsTableExists(ctx)
	if err != nil {
		return false, false, fmt.Errorf("failed to inspect migrations table: %w", err)
	}
	if !exists {
		return false, false, nil
	}
	if err := m.requireTransactionalMetadataEngine(ctx); err != nil {
		return false, false, err
	}
	if m.revisionTableFormat.isAtlas() {
		if err := m.validateAtlasRevisionIdentityCollation(ctx); err != nil {
			return false, false, err
		}
		return true, false, nil
	}

	legacy, err = m.migrationsTableUsesLegacyRevisionLayout(ctx)
	if err != nil {
		return false, false, fmt.Errorf("failed to inspect migrations table layout: %w", err)
	}
	return true, legacy, nil
}

func (m *Migrator) ensureMigrationsRevisionColumns(ctx context.Context) error {
	columns := []struct {
		name       string
		definition string
	}{
		{name: "state", definition: "VARCHAR(32) NOT NULL DEFAULT 'applied'"},
		{name: "applied", definition: "INTEGER NOT NULL DEFAULT 1"},
		{name: "total", definition: "INTEGER NOT NULL DEFAULT 1"},
		{name: "error", definition: "TEXT NULL"},
		{name: "error_stmt", definition: "TEXT NULL"},
		{name: "execution_time_ms", definition: "BIGINT NOT NULL DEFAULT 0"},
		{name: "checksum", definition: "VARCHAR(64) NOT NULL DEFAULT ''"},
	}
	for _, column := range columns {
		if err := m.ensureMigrationsRevisionColumn(ctx, column.name, column.definition); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) ensureMigrationsRevisionColumn(ctx context.Context, name, definition string) error {
	exists, err := m.migrationsColumnExists(ctx, name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	query := fmt.Sprintf(
		"ALTER TABLE %s ADD %s %s",
		m.qualifiedMigrationsTable(),
		m.quoteIdentifier(name),
		m.migrationsRevisionColumnDefinition(name, definition),
	)
	if _, err := m.conn.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to add migrations metadata column %s: %w", name, err)
	}
	return nil
}

func (m *Migrator) migrationsRevisionColumnDefinition(name, fallback string) string {
	if !m.isSQLServer() {
		return fallback
	}
	switch name {
	case "state":
		return "NVARCHAR(32) NOT NULL DEFAULT 'applied'"
	case "error", "error_stmt":
		return "NVARCHAR(MAX) NULL"
	case "checksum":
		return "NVARCHAR(64) NOT NULL DEFAULT ''"
	default:
		return fallback
	}
}

func (m *Migrator) migrationsColumnExists(ctx context.Context, name string) (bool, error) {
	switch m.conn.Info().Dialect {
	case platform.ClickHouse:
		return m.clickHouseMigrationsColumnExists(ctx, name)
	case platform.SQLite:
		return m.sqliteMigrationsColumnExists(ctx, name)
	}
	query := `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ? AND column_name = ?`
	schema := m.metadataSchemaName()
	if m.isPostgresFamily() {
		query = `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema = COALESCE(NULLIF(?, ''), current_schema())
  AND table_name = ? AND column_name = ?`
		schema = m.metadataTableSchemaName()
	} else if m.isSQLServer() {
		// Preserve the catalog's canonical identifier casing. Under Turkish
		// collations, lowercase information_schema does not resolve to
		// INFORMATION_SCHEMA even when the catalog is case-insensitive.
		query = `
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`
	}
	query = sqlutil.Rebind(m.conn.Info().Dialect, query)
	var count int
	if err := m.conn.QueryRowContext(ctx, query, schema, m.migrationsTableName(), name).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", name, err)
	}
	return count > 0, nil
}

func (m *Migrator) sqliteMigrationsColumnExists(ctx context.Context, name string) (bool, error) {
	conn, err := m.conn.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", name, err)
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, "PRAGMA table_info("+m.quoteIdentifier(m.migrationsTableName())+")")
	if err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid          int
			columnName   string
			dataType     string
			notNull      int
			defaultValue sql.NullString
			primaryKey   int
		)
		if err := rows.Scan(&cid, &columnName, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, fmt.Errorf("failed to scan migrations metadata column %s: %w", name, err)
		}
		if columnName == name {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", name, err)
	}
	return false, nil
}

func (m *Migrator) clickHouseMigrationsColumnExists(ctx context.Context, name string) (bool, error) {
	var count int
	if err := m.conn.QueryRowContext(
		ctx,
		`SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = ? AND name = ?`,
		m.migrationsTableName(),
		name,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", name, err)
	}
	return count > 0, nil
}

func (m *Migrator) ensureMigrationsVersionColumn(ctx context.Context) error {
	switch m.conn.Info().Dialect {
	case "postgres", "cockroachdb", "yugabytedb":
		return m.ensurePostgresMigrationsVersionColumn(ctx)
	case "mysql", "mariadb":
		return m.ensureMySQLMigrationsVersionColumn(ctx)
	default:
		return nil
	}
}

func (m *Migrator) ensurePostgresMigrationsVersionColumn(ctx context.Context) error {
	dataType, err := m.migrationsVersionColumnType(
		ctx,
		sqlutil.Rebind(m.conn.Info().Dialect, `
SELECT data_type
FROM information_schema.columns
WHERE table_schema = COALESCE(NULLIF(?, ''), current_schema())
  AND table_name = ? AND column_name = 'version'`),
		m.metadataTableSchemaName(),
		m.migrationsTableName(),
	)
	if err != nil {
		return err
	}
	if dataType == "bigint" {
		return nil
	}
	_, err = m.conn.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s ALTER COLUMN %s TYPE BIGINT",
		m.qualifiedMigrationsTable(),
		m.quoteIdentifier("version"),
	))
	if err != nil {
		return fmt.Errorf("failed to widen version column from %s to BIGINT: %w", dataType, err)
	}
	return nil
}

func (m *Migrator) ensureMySQLMigrationsVersionColumn(ctx context.Context) error {
	dataType, err := m.migrationsVersionColumnType(
		ctx,
		`SELECT data_type
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ? AND column_name = 'version'`,
		m.metadataSchemaName(),
		m.migrationsTableName(),
	)
	if err != nil {
		return err
	}
	if dataType == "bigint" {
		return nil
	}
	_, err = m.conn.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s MODIFY COLUMN %s BIGINT NOT NULL",
		m.qualifiedMigrationsTable(),
		m.quoteIdentifier("version"),
	))
	if err != nil {
		return fmt.Errorf("failed to widen version column from %s to BIGINT: %w", dataType, err)
	}
	return nil
}

func (m *Migrator) migrationsVersionColumnType(ctx context.Context, query string, args ...any) (string, error) {
	var dataType string
	err := m.conn.QueryRowContext(ctx, query, args...).Scan(&dataType)
	if err != nil {
		return "", fmt.Errorf("failed to inspect migrations version column: %w", err)
	}
	return strings.ToLower(dataType), nil
}

func (m *Migrator) isPostgresFamily() bool {
	switch platform.NormalizeDialect(m.connectionDialect()) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		return true
	default:
		return false
	}
}

func (m *Migrator) metadataSchemaName() string {
	return metadataInformationSchemaName(m.connectionDialect(), m.connectionSchemaName(), m.migrationsSchema)
}

func metadataInformationSchemaName(dialect, connectionSchema, configuredSchema string) string {
	if schema := metadataTableSchemaName(dialect, connectionSchema, configuredSchema); schema != "" {
		return schema
	}
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres:
		return "public"
	case platform.Spanner, platform.MySQL, platform.MariaDB:
		return strings.TrimSpace(connectionSchema)
	}
	return ""
}

func (m *Migrator) metadataTableSchemaName() string {
	return metadataTableSchemaName(m.connectionDialect(), m.connectionSchemaName(), m.migrationsSchema)
}

func metadataTableSchemaName(dialect, connectionSchema, configuredSchema string) string {
	if schema := strings.TrimSpace(configuredSchema); schema != "" {
		return schema
	}
	if platform.NormalizeDialect(dialect) != platform.SQLServer {
		return ""
	}
	if schema := strings.TrimSpace(connectionSchema); schema != "" {
		return schema
	}
	return "dbo"
}

func (m *Migrator) migrationsTableName() string {
	if m.migrationsTable == "" {
		return m.defaultMigrationsTable()
	}
	return m.migrationsTable
}

func (m *Migrator) sqlServerObjectName() string {
	if schema := m.metadataTableSchemaName(); schema != "" {
		return m.quoteIdentifier(schema) + "." + m.quoteIdentifier(m.migrationsTableName())
	}
	return m.quoteIdentifier(m.migrationsTableName())
}

// GetCurrentVersion returns the current migration version from the database
func (m *Migrator) GetCurrentVersion(ctx context.Context) (int64, error) {
	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get migration revisions: %w", err)
	}
	if m.revisionTableFormat.isAtlas() {
		return maxRevisionVersion(revisions), nil
	}
	return maxAppliedVersion(appliedRevisionVersions(revisions)), nil
}

// GetAppliedMigrations returns a list of applied migration versions
func (m *Migrator) GetAppliedMigrations(ctx context.Context) ([]int64, error) {
	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get migration revisions: %w", err)
	}
	return appliedRevisionVersions(revisions), nil
}

// GetAppliedRevisions returns full metadata rows for applied migrations.
func (m *Migrator) GetAppliedRevisions(ctx context.Context) ([]MigrationRevision, error) {
	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get migration revisions: %w", err)
	}
	applied := make([]MigrationRevision, 0, len(revisions))
	for _, revision := range revisions {
		if revision.State == migrationStateApplied {
			applied = append(applied, revision)
		}
	}
	return applied, nil
}

// GetRevisions returns every migration metadata row, including dirty rows.
func (m *Migrator) GetRevisions(ctx context.Context) ([]MigrationRevision, error) {
	return queryMigrationRows(
		ctx,
		m,
		(*Migrator).getRevisionsSQL,
		m.scanRevisionRow,
		"failed to query migration revisions",
		"failed to scan migration revision",
		"error iterating migration revisions",
	)
}

func queryMigrationRows[T any](
	ctx context.Context,
	m *Migrator,
	query func(*Migrator) string,
	scan func(rowScanner) (T, error),
	queryErr string,
	scanErr string,
	iterErr string,
) ([]T, error) {
	if err := m.Initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if !m.metadataAvailable {
		return []T{}, nil
	}

	var revisions []T
	err := m.withMigrationMetadataSession(ctx, func(scoped *Migrator) error {
		rows, err := queryMigrationRowsFrom(
			ctx,
			scoped.conn,
			query(scoped),
			scan,
			queryErr,
			scanErr,
			iterErr,
		)
		revisions = rows
		return err
	})
	return revisions, err
}

func (m *Migrator) withMigrationMetadataSession(ctx context.Context, use func(*Migrator) error) error {
	if !implicitCommitDialect(m.connectionDialect()) {
		return use(m)
	}
	return m.conn.WithSessionOrCurrent(ctx, func(conn *dbschema.DatabaseConnection) error {
		scoped := *m
		scoped.conn = conn
		if scoped.migrationsSchema == "" {
			scoped.migrationsSchema = scoped.connectionSchemaName()
		}
		if err := scoped.refuseMySQLTemporaryMetadataShadow(ctx); err != nil {
			return err
		}
		return use(&scoped)
	})
}

type migrationRowsQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func queryMigrationRowsFrom[T any](
	ctx context.Context,
	queryer migrationRowsQueryer,
	query string,
	scan func(rowScanner) (T, error),
	queryErr string,
	scanErr string,
	iterErr string,
) ([]T, error) {
	rows, err := queryer.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", queryErr, err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]T, 0)
	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", scanErr, err)
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", iterErr, err)
	}
	return items, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func parseAtlasRevisionVersion(version string) (int64, error) {
	trimmed := strings.TrimSpace(version)
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err == nil {
		return parsed, nil
	}
	if trimmed == "R" {
		return 0, nil
	}
	if prefix, ok := strings.CutSuffix(trimmed, "R"); ok && allDigits(prefix) {
		parsed, parseErr := strconv.ParseInt(prefix, 10, 64)
		if parseErr == nil {
			return parsed, nil
		}
	}
	return 0, fmt.Errorf("Atlas revision version %q is not a numeric or repeatable Ptah migration version: %w", version, err)
}

// GetPendingMigrations returns a list of pending migration versions
func (m *Migrator) GetPendingMigrations(ctx context.Context) ([]int64, error) {
	snapshot, err := m.GetMigrationStatusSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snapshot.Status.PendingMigrations, nil
}

// GetPreviousMigrationVersion finds the previous migration version compared to the current one.
// Returns an error and -1 if no previous migrations exist.
func (m *Migrator) GetPreviousMigrationVersion(ctx context.Context) (int64, error) {
	applied, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return -1, fmt.Errorf("failed to get applied migrations: %w", err)
	}
	if len(applied) == 0 {
		return -1, fmt.Errorf("no previous migrations exist")
	}
	if len(applied) == 1 {
		return 0, nil
	}

	return applied[len(applied)-2], nil
}

// GetMigrationStatus returns information about the current migration status using the provided filesystem
func (m *Migrator) GetMigrationStatus(ctx context.Context) (status *MigrationStatus, err error) {
	snapshot, err := m.GetMigrationStatusSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snapshot.Status, nil
}

// GetMigrationStatusSnapshot returns status and the revision rows used to
// derive it from one metadata query.
func (m *Migrator) GetMigrationStatusSnapshot(
	ctx context.Context,
) (snapshot MigrationStatusSnapshot, err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.status", m.operationAttributes("")...)
	defer func() { span.End(err) }()

	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return MigrationStatusSnapshot{}, fmt.Errorf("failed to get migration revisions: %w", err)
	}
	appliedMigrations := appliedRevisionVersions(revisions)
	appliedMigrationKeys := appliedRevisionVersionKeys(revisions)
	effectiveAppliedMigrations := m.effectiveAppliedVersionsFromRevisions(appliedMigrations, revisions)
	effectiveAppliedIdentities := m.effectiveAppliedIdentitySetFromRevisions(effectiveAppliedMigrations, revisions)
	currentVersion := maxAppliedVersion(appliedMigrations)
	exactRevisionOrder := m.hasExactAtlasRevisionOrder()
	currentVersionKey, currentVersionKeySet := m.currentRevisionVersionKey(revisions)
	if m.revisionTableFormat.isAtlas() {
		currentVersion = maxRevisionVersion(revisions)
	}
	providerMigrations := m.MigrationProvider().Migrations()
	bootstrap := checkpointBootstrap(providerMigrations, effectiveAppliedMigrations, 0)
	floor := checkpointFloor(providerMigrations, effectiveAppliedMigrations, bootstrap)
	pendingMigrationList := pendingMigrationsFloored(providerMigrations, effectiveAppliedIdentities, bootstrap, floor, 0)
	pendingMigrations := migrationVersions(pendingMigrationList)
	pendingMigrationKeys := migrationVersionKeys(pendingMigrationList)
	outOfOrderMigrations := outOfOrderMigrationVersions(pendingMigrations, currentVersion)
	outOfOrderMigrationKeys := outOfOrderMigrationKeys(pendingMigrationList, currentVersion)
	dirtyRevision := firstDirtyRevision(revisions)
	if dirtyRevision != nil {
		currentVersionKey = dirtyRevision.RevisionVersion()
		currentVersionKeySet = true
	} else if !exactRevisionOrder {
		currentVersionKey, currentVersionKeySet = currentMigrationVersionKey(
			providerMigrations,
			effectiveAppliedIdentities,
			currentVersionKey,
			currentVersionKeySet,
		)
	}

	status := &MigrationStatus{
		CurrentVersion:          currentVersion,
		CurrentVersionKey:       currentVersionKey,
		CurrentVersionKeySet:    currentVersionKeySet,
		AppliedMigrations:       appliedMigrations,
		AppliedMigrationKeys:    appliedMigrationKeys,
		PendingMigrations:       pendingMigrations,
		PendingMigrationKeys:    pendingMigrationKeys,
		OutOfOrderMigrations:    outOfOrderMigrations,
		OutOfOrderMigrationKeys: outOfOrderMigrationKeys,
		TotalMigrations:         len(providerMigrations),
		HasPendingChanges:       len(pendingMigrationList) > 0 || dirtyRevision != nil,
		DirtyRevision:           dirtyRevision,
	}
	span.SetAttributes(
		attr("migration.current_version", status.CurrentVersion),
		attr("migration.pending_count", len(status.PendingMigrations)),
		attr("migration.out_of_order_count", len(status.OutOfOrderMigrations)),
		attr("migration.total_count", status.TotalMigrations),
	)
	return MigrationStatusSnapshot{Status: status, Revisions: revisions}, nil
}

func appliedRevisionVersions(revisions []MigrationRevision) []int64 {
	versions := make([]int64, 0, len(revisions))
	for _, revision := range revisions {
		if revision.State == migrationStateApplied {
			versions = append(versions, revision.Version)
		}
	}
	return versions
}

func appliedRevisionVersionKeys(revisions []MigrationRevision) []string {
	keys := make([]string, 0, len(revisions))
	for _, revision := range revisions {
		if revision.State == migrationStateApplied {
			keys = append(keys, revision.RevisionVersion())
		}
	}
	return keys
}

func (m *Migrator) currentRevisionVersionKey(revisions []MigrationRevision) (string, bool) {
	if m.hasExactAtlasRevisionOrder() {
		return currentExactRevisionVersionKey(revisions)
	}
	return currentRuntimeRevisionVersionKey(revisions)
}

func (m *Migrator) hasExactAtlasRevisionOrder() bool {
	return m.revisionTableFormat.isAtlas() && m.hasAtlasRevisionVersionMap()
}

func currentRuntimeRevisionVersionKey(revisions []MigrationRevision) (string, bool) {
	current := ""
	var currentVersion int64
	found := false
	for _, revision := range revisions {
		if revision.State != migrationStateApplied {
			continue
		}
		if !found || revision.Version > currentVersion {
			current = revision.RevisionVersion()
			currentVersion = revision.Version
			found = true
		}
	}
	return current, found
}

func currentExactRevisionVersionKey(revisions []MigrationRevision) (string, bool) {
	current := ""
	found := false
	for _, revision := range revisions {
		if revision.State == migrationStateApplied &&
			(!found || revision.RevisionVersion() > current) {
			current = revision.RevisionVersion()
			found = true
		}
	}
	return current, found
}

func currentMigrationVersionKey(
	migrations []*Migration,
	applied migrationIdentitySet,
	fallback string,
	fallbackSet bool,
) (string, bool) {
	current := fallback
	var currentVersion int64
	found := false
	for _, migration := range migrations {
		if applied.containsMigration(migration) &&
			migration.RevisionVersion() != "" &&
			(!found || migration.Version > currentVersion) {
			current = migration.RevisionVersion()
			currentVersion = migration.Version
			found = true
		}
	}
	if found {
		return current, true
	}
	return current, fallbackSet
}

func maxRevisionVersion(revisions []MigrationRevision) int64 {
	var version int64
	for _, revision := range revisions {
		if revision.Version > version {
			version = revision.Version
		}
	}
	return version
}

type migrationIdentitySet struct {
	versions  map[int64]struct{}
	exactKeys map[string]struct{}
}

func newMigrationIdentitySet(versions []int64, revisions []MigrationRevision) migrationIdentitySet {
	set := migrationIdentitySet{
		versions:  versionSet(versions),
		exactKeys: make(map[string]struct{}, len(revisions)),
	}
	for _, revision := range revisions {
		if revision.State == migrationStateApplied {
			set.exactKeys[revision.RevisionVersion()] = struct{}{}
		}
	}
	return set
}

func (s migrationIdentitySet) addMigration(migration *Migration) {
	s.versions[migration.Version] = struct{}{}
	s.exactKeys[migration.RevisionVersion()] = struct{}{}
}

func (s migrationIdentitySet) addVersionsWithKeys(versions []int64, keys []string) {
	for index, version := range versions {
		s.versions[version] = struct{}{}
		key := strconv.FormatInt(version, 10)
		if index < len(keys) {
			key = keys[index]
		}
		s.exactKeys[key] = struct{}{}
	}
}

func assumedAppliedVersionKey(versions []int64, keys []string, target int64) string {
	for index, version := range versions {
		if version != target {
			continue
		}
		if index < len(keys) {
			return keys[index]
		}
		break
	}
	return strconv.FormatInt(target, 10)
}

func (s migrationIdentitySet) containsMigration(migration *Migration) bool {
	if _, ok := s.exactKeys[migration.RevisionVersion()]; ok {
		return true
	}
	if migration.atlasRevisionVersionMapped {
		return false
	}
	_, ok := s.versions[migration.Version]
	return ok
}

func (s migrationIdentitySet) revisionKeys() []string {
	return slices.Sorted(maps.Keys(s.exactKeys))
}

func firstDirtyRevision(revisions []MigrationRevision) *MigrationRevision {
	for _, revision := range revisions {
		if revision.State != migrationStateApplied {
			revision.Dirty = true
			return &revision
		}
	}
	return nil
}

// MigrateUp migrates the database up to the latest version
func (m *Migrator) MigrateUp(ctx context.Context) error {
	return m.MigrateUpWithPreflight(ctx, nil)
}

// MigrateUpWithPreflight migrates up after running hook inside the migration
// advisory lock. A nil hook is equivalent to [Migrator.MigrateUp].
func (m *Migrator) MigrateUpWithPreflight(ctx context.Context, hook PreMigrationHook) (err error) {
	return m.MigrateUpWithOptions(ctx, MigrateUpOptions{Preflight: hook})
}

// MigrateUpWithOptions migrates up using an explicitly selected apply plan.
func (m *Migrator) MigrateUpWithOptions(ctx context.Context, opts MigrateUpOptions) (err error) {
	if err := validateMigrateUpOptions(opts); err != nil {
		return err
	}
	observer := m.migrationObserver()
	attrs := m.operationAttributes(MigrationDirectionUp)
	if opts.TargetVersion > 0 {
		attrs = append(attrs, attr("migration.requested_target_version", opts.TargetVersion))
	}
	if opts.Amount > 0 {
		attrs = append(attrs, attr("migration.requested_amount", opts.Amount))
	}
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.up", attrs...)
	defer func() { span.End(err) }()
	ctx = contextWithRootSpan(ctx, span)
	return m.withMigrationLock(ctx, "migrate up", func(ctx context.Context) error {
		return m.migrateUpLocked(ctx, opts)
	})
}

func validateMigrateUpOptions(opts MigrateUpOptions) error {
	if opts.TargetVersion < 0 {
		return fmt.Errorf("target version must be greater than or equal to zero")
	}
	if opts.TargetVersion > 0 && opts.Amount > 0 {
		return fmt.Errorf("target version and amount cannot both be set")
	}
	return nil
}

func (m *Migrator) migrateUpLocked(ctx context.Context, opts MigrateUpOptions) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if !opts.AllowDirty {
		if err := m.failIfDirty(ctx); err != nil {
			return err
		}
	}

	migrations := m.migrationProvider.Migrations()
	if opts.TargetVersion > 0 && !hasMigrationVersion(migrations, opts.TargetVersion) {
		return fmt.Errorf("target version %d was not found in the migration provider", opts.TargetVersion)
	}

	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}
	if opts.AllowDirty {
		if err := m.failIfUnownedDirtyRevision(revisions, migrations); err != nil {
			return err
		}
	}
	appliedMigrations := appliedRevisionVersions(revisions)
	appliedMigrations = m.effectiveAppliedVersionsFromRevisions(appliedMigrations, revisions)
	appliedIdentities := m.effectiveAppliedIdentitySetFromRevisions(appliedMigrations, revisions)
	appliedMigrations = mergeAppliedVersions(appliedMigrations, opts.AssumedAppliedVersions)
	appliedIdentities.addVersionsWithKeys(opts.AssumedAppliedVersions, opts.AssumedAppliedVersionKeys)
	currentVersion := maxAppliedVersion(appliedMigrations)
	currentVersionKey, currentVersionKeySet := m.currentRevisionVersionKey(revisions)
	if assumedCurrent := maxAppliedVersion(opts.AssumedAppliedVersions); assumedCurrent > 0 && assumedCurrent >= currentVersion {
		currentVersionKey = assumedAppliedVersionKey(
			opts.AssumedAppliedVersions,
			opts.AssumedAppliedVersionKeys,
			assumedCurrent,
		)
		currentVersionKeySet = true
	}

	reconcileChecksums, err := m.verifyAppliedMigrationChecksums(ctx, migrations)
	if err != nil {
		return err
	}

	migrationsToApply, err := m.migrationsToApply(migrations, appliedMigrations, appliedIdentities, opts.TargetVersion)
	if err != nil {
		return err
	}
	migrationsToApply = limitMigrationsToApply(migrationsToApply, opts.Amount)
	plan := MigrationPlan{
		Direction:            MigrationDirectionUp,
		CurrentVersion:       currentVersion,
		CurrentVersionKey:    currentVersionKey,
		CurrentVersionKeySet: currentVersionKeySet,
		TargetVersion:        upTargetVersion(currentVersion, migrationsToApply),
		TargetVersionKey:     upTargetVersionKey(currentVersionKey, migrationsToApply),
		Versions:             migrationVersions(migrationsToApply),
		VersionKeys:          migrationVersionKeys(migrationsToApply),
	}
	notifyMigrationPlanObserver(ctx, opts.PlanObserver, plan)
	if err := m.validateUpTransactionMode(migrationsToApply); err != nil {
		return err
	}
	if err := runPreMigrationHook(ctx, opts.Preflight, plan); err != nil {
		return err
	}
	if span := rootSpanFromContext(ctx); span != nil {
		span.SetAttributes(
			attr("migration.current_version", currentVersion),
			attr("migration.target_version", upTargetVersion(currentVersion, migrationsToApply)),
			attr("migration.pending_count", len(migrationsToApply)),
		)
	}

	m.logger.Info("Migrating up", "currentVersion", currentVersion, "totalMigrations", len(migrations))
	checksDeferred, err := m.applyUpMigrations(ctx, migrationsToApply)
	if err != nil {
		if opts.DiscardRolledBackFailure {
			return errors.Join(err, m.discardRolledBackFailure(ctx, err))
		}
		return err
	}
	notifyChecksDeferredObserver(ctx, opts.ChecksDeferredObserver, checksDeferred)
	if reconcileChecksums {
		if err := m.reconcileAppliedMigrationChecksums(ctx, migrations); err != nil {
			return err
		}
	}

	m.logger.Info("All migrations applied successfully")
	return nil
}

func hasMigrationVersion(migrations []*Migration, version int64) bool {
	for _, migration := range migrations {
		if migration.Version == version {
			return true
		}
	}
	return false
}

func limitMigrationsToApply(migrations []*Migration, amount uint64) []*Migration {
	if amount == 0 || amount >= uint64(len(migrations)) {
		return migrations
	}
	return migrations[:amount]
}

func mergeAppliedVersions(applied []int64, assumed []int64) []int64 {
	if len(assumed) == 0 {
		return applied
	}
	merged := make([]int64, 0, len(applied)+len(assumed))
	seen := make(map[int64]struct{}, len(applied)+len(assumed))
	for _, version := range applied {
		if _, ok := seen[version]; ok {
			continue
		}
		seen[version] = struct{}{}
		merged = append(merged, version)
	}
	for _, version := range assumed {
		if _, ok := seen[version]; ok {
			continue
		}
		seen[version] = struct{}{}
		merged = append(merged, version)
	}
	slices.Sort(merged)
	return merged
}

func (m *Migrator) effectiveAppliedVersionsFromRevisions(
	applied []int64,
	revisions []MigrationRevision,
) []int64 {
	boundary := atlasRevisionBoundary(revisions)
	if boundary == 0 {
		return applied
	}

	implicit := migrationVersions(m.migrationsAtOrBelow(boundary))
	return mergeAppliedVersions(applied, implicit)
}

func (m *Migrator) effectiveAppliedIdentitySetFromRevisions(
	applied []int64,
	revisions []MigrationRevision,
) migrationIdentitySet {
	set := newMigrationIdentitySet(applied, revisions)
	boundary := atlasRevisionBoundary(revisions)
	if boundary == 0 {
		return set
	}
	for _, migration := range m.migrationsAtOrBelow(boundary) {
		set.addMigration(migration)
	}
	return set
}

func atlasRevisionBoundary(revisions []MigrationRevision) int64 {
	var boundary int64
	for _, revision := range revisions {
		if revision.State == migrationStateApplied &&
			revision.AtlasType == AtlasRevisionTypeBaseline &&
			revision.Version > boundary {
			boundary = revision.Version
		}
	}
	return boundary
}

// MigrateDown migrates the database down to the previous version
func (m *Migrator) MigrateDown(ctx context.Context) (err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.down", m.operationAttributes(MigrationDirectionDown)...)
	defer func() { span.End(err) }()
	ctx = contextWithRootSpan(ctx, span)
	return m.withMigrationLock(ctx, "migrate down", func(ctx context.Context) error {
		return m.migrateDownLocked(ctx)
	})
}

func (m *Migrator) migrateDownLocked(ctx context.Context) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if err := m.failIfDirty(ctx); err != nil {
		return err
	}

	targetVersion, err := m.GetPreviousMigrationVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get previous version: %w", err)
	}

	return m.migrateDownToLocked(ctx, targetVersion, nil)
}

// MigrateDownTo migrates the database down to the specified target version
func (m *Migrator) MigrateDownTo(ctx context.Context, targetVersion int64) error {
	return m.MigrateDownToWithPreflight(ctx, targetVersion, nil)
}

// MigrateDownToWithPreflight migrates down after running hook inside the
// migration advisory lock. A nil hook is equivalent to [Migrator.MigrateDownTo].
func (m *Migrator) MigrateDownToWithPreflight(ctx context.Context, targetVersion int64, hook PreMigrationHook) (err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.down", append(m.operationAttributes(MigrationDirectionDown), attr("migration.requested_target_version", targetVersion))...)
	defer func() { span.End(err) }()
	ctx = contextWithRootSpan(ctx, span)
	return m.withMigrationLock(ctx, "migrate down", func(ctx context.Context) error {
		return m.migrateDownToLocked(ctx, targetVersion, hook)
	})
}

func (m *Migrator) migrateDownToLocked(ctx context.Context, targetVersion int64, hook PreMigrationHook) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if err := m.failIfDirty(ctx); err != nil {
		return err
	}

	appliedMigrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}
	currentVersion := maxAppliedVersion(appliedMigrations)

	// Skip if already at or below target version (shouldn't happen)
	if targetVersion >= currentVersion {
		m.logger.Info("Already at or below target version", "targetVersion", targetVersion, "currentVersion", currentVersion)
		return nil
	}

	if boundary := checkpointRollbackBoundary(m.migrationProvider.Migrations(), appliedMigrations, targetVersion); boundary > 0 {
		return &CheckpointRollbackError{TargetVersion: targetVersion, CheckpointVersion: boundary}
	}

	migrations := m.migrationProvider.Migrations()
	migrationMap := migrationsByVersion(migrations)
	reconcileChecksums, err := m.verifyAppliedMigrationChecksums(ctx, migrations)
	if err != nil {
		return err
	}
	migrationsToRollback, err := migrationsToRollback(migrationMap, appliedMigrations, targetVersion)
	if err != nil {
		return err
	}
	if err := m.validateDownMigrations(migrationsToRollback); err != nil {
		return err
	}
	if err := runPreMigrationHook(ctx, hook, MigrationPlan{
		Direction:      MigrationDirectionDown,
		CurrentVersion: currentVersion,
		TargetVersion:  downTargetVersion(appliedMigrations, targetVersion),
		Versions:       migrationVersions(migrationsToRollback),
	}); err != nil {
		return err
	}
	if span := rootSpanFromContext(ctx); span != nil {
		span.SetAttributes(
			attr("migration.current_version", currentVersion),
			attr("migration.target_version", downTargetVersion(appliedMigrations, targetVersion)),
			attr("migration.pending_count", len(migrationsToRollback)),
		)
	}

	m.logger.Info("Migrating down", "targetVersion", targetVersion, "currentVersion", currentVersion, "totalMigrations", len(m.migrationProvider.Migrations()))

	// Rebind once: template + dialect are loop-invariant. Migration version
	// is bound as a parameter via the dialect-native placeholder.
	deleteSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.deleteMigrationSQL())

	for _, migration := range migrationsToRollback {
		if err := m.rollbackMigration(ctx, migration, deleteSQL); err != nil {
			return err
		}
	}
	if reconcileChecksums {
		if err := m.reconcileAppliedMigrationChecksums(ctx, migrations); err != nil {
			return err
		}
	}

	m.logger.Info("All migrations rolled back successfully")
	return nil
}

// MigrateTo migrates the database to a specific version (up or down)
func (m *Migrator) MigrateTo(ctx context.Context, targetVersion int64) (err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.to", append(m.operationAttributes(""), attr("migration.requested_target_version", targetVersion))...)
	defer func() { span.End(err) }()
	ctx = contextWithRootSpan(ctx, span)
	return m.withMigrationLock(ctx, "migrate to", func(ctx context.Context) error {
		return m.migrateToLocked(ctx, targetVersion)
	})
}

func (m *Migrator) migrateToLocked(ctx context.Context, targetVersion int64) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if err := m.failIfDirty(ctx); err != nil {
		return err
	}

	appliedMigrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}
	currentVersion := maxAppliedVersion(appliedMigrations)

	if targetVersion == currentVersion {
		m.logger.Info("Already at target version", "version", targetVersion)
		return nil
	}

	if targetVersion > currentVersion {
		// Migrate up to target version
		return m.migrateUpTo(ctx, targetVersion)
	}

	if targetVersion > 0 && !slices.Contains(appliedMigrations, targetVersion) {
		return fmt.Errorf("target version %d is below current version %d but is not applied", targetVersion, currentVersion)
	}

	// Migrate down to target version
	return m.migrateDownToLocked(ctx, targetVersion, nil)
}

// MigrationProvider returns the migration provider
func (m *Migrator) MigrationProvider() MigrationProvider {
	return m.migrationProvider
}

// migrateUpTo migrates the database up to a specific version
func (m *Migrator) migrateUpTo(ctx context.Context, targetVersion int64) error {
	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}
	appliedMigrations := appliedRevisionVersions(revisions)
	appliedMigrations = m.effectiveAppliedVersionsFromRevisions(appliedMigrations, revisions)
	appliedIdentities := m.effectiveAppliedIdentitySetFromRevisions(appliedMigrations, revisions)
	currentVersion := maxAppliedVersion(appliedMigrations)

	migrations := m.migrationProvider.Migrations()
	reconcileChecksums, err := m.verifyAppliedMigrationChecksums(ctx, migrations)
	if err != nil {
		return err
	}
	migrationsToApply, err := m.migrationsToApply(migrations, appliedMigrations, appliedIdentities, targetVersion)
	if err != nil {
		return err
	}
	if err := m.validateUpTransactionMode(migrationsToApply); err != nil {
		return err
	}
	if span := rootSpanFromContext(ctx); span != nil {
		span.SetAttributes(
			attr("migration.current_version", currentVersion),
			attr("migration.target_version", upTargetVersion(currentVersion, migrationsToApply)),
			attr("migration.pending_count", len(migrationsToApply)),
		)
	}

	m.logger.Info("Migrating up", "currentVersion", currentVersion, "targetVersion", targetVersion, "totalMigrations", len(migrations))
	if _, err := m.applyUpMigrations(ctx, migrationsToApply); err != nil {
		return err
	}
	if reconcileChecksums {
		if err := m.reconcileAppliedMigrationChecksums(ctx, migrations); err != nil {
			return err
		}
	}

	m.logger.Info("Migrated successfully", "targetVersion", targetVersion)
	return nil
}

func notifyMigrationPlanObserver(ctx context.Context, observer MigrationPlanObserver, plan MigrationPlan) {
	if observer == nil {
		return
	}
	plan.Versions = slices.Clone(plan.Versions)
	plan.VersionKeys = slices.Clone(plan.VersionKeys)
	observer(ctx, plan)
}

func notifyChecksDeferredObserver(ctx context.Context, observer ChecksDeferredObserver, versions []int64) {
	if observer == nil || len(versions) == 0 {
		return
	}
	observer(ctx, slices.Clone(versions))
}

func runPreMigrationHook(ctx context.Context, hook PreMigrationHook, plan MigrationPlan) error {
	if hook == nil || len(plan.Versions) == 0 {
		return nil
	}
	plan.Versions = slices.Clone(plan.Versions)
	plan.VersionKeys = slices.Clone(plan.VersionKeys)
	return hook(ctx, plan)
}

func migrationVersions(migrations []*Migration) []int64 {
	versions := make([]int64, 0, len(migrations))
	for _, migration := range migrations {
		versions = append(versions, migration.Version)
	}
	return versions
}

func migrationVersionKeys(migrations []*Migration) []string {
	keys := make([]string, 0, len(migrations))
	for _, migration := range migrations {
		keys = append(keys, migration.RevisionVersion())
	}
	return keys
}

func maxMigrationVersion(migrations []*Migration) int64 {
	var maxVersion int64
	for _, migration := range migrations {
		if migration.Version > maxVersion {
			maxVersion = migration.Version
		}
	}
	return maxVersion
}

func upTargetVersion(currentVersion int64, migrations []*Migration) int64 {
	return max(currentVersion, maxMigrationVersion(migrations))
}

func upTargetVersionKey(currentVersionKey string, migrations []*Migration) string {
	if len(migrations) == 0 {
		return currentVersionKey
	}
	return migrations[len(migrations)-1].RevisionVersion()
}

func downTargetVersion(applied []int64, targetVersion int64) int64 {
	var finalVersion int64
	for _, version := range applied {
		if version <= targetVersion && version > finalVersion {
			finalVersion = version
		}
	}
	return finalVersion
}

// applyUpMigrations executes the run and returns the versions whose
// pre-migration checks were parsed and statically validated but not evaluated
// against the database. That list is empty outside a dry run.
func (m *Migrator) applyUpMigrations(ctx context.Context, migrations []*Migration) ([]int64, error) {
	if err := m.reportMisplacedDirectives(migrations, MigrationDirectionUp); err != nil {
		return nil, err
	}
	switch m.txMode {
	case MigrationTxModeAll:
		return m.applyUpMigrationsInSingleTransaction(ctx, migrations)
	default:
		return m.applyUpMigrationsPerFile(ctx, migrations)
	}
}

// reportMisplacedDirectives warns about every directive line the run recognized
// and did not honor because of where it sits.
//
// It runs once per direction per run, on the execution path, so a dry run
// reports the same finding a real apply would -- the operator who is about to
// discover that `txmode none` did nothing is the one running `--dry-run` first.
// A migration whose directives are all honored produces nothing, which keeps a
// clean run silent on stderr the way Atlas is.
func (m *Migrator) reportMisplacedDirectives(migrations []*Migration, direction MigrationDirection) error {
	dialect := m.connectionDialect()
	for _, migration := range migrations {
		source, sourcePath := migration.UpSQL, migration.upSourcePath
		if direction == MigrationDirectionDown {
			source, sourcePath = migration.DownSQL, migration.downSourcePath
		}
		for _, misplaced := range misplacedDirectives(source, dialect) {
			if misplaced.err != nil {
				// Reported as the run's refusal by [misplacedDirectiveError],
				// which names the line too. Warning about it here as well would
				// print the same line twice on a run that is about to abort.
				continue
			}
			m.logger.Warn(
				"Migration directive was not honored because of where it appears in the file",
				"version", migration.Version,
				"direction", string(direction),
				"file", migrationTxModeSourceName(sourcePath, migration.Description),
				"line", misplaced.line,
				"directive", misplaced.text,
				"remedy", misplaced.remedy,
			)
		}
	}
	return nil
}

func (m *Migrator) applyUpMigrationsPerFile(ctx context.Context, migrations []*Migration) ([]int64, error) {
	var checksDeferred []int64
	// The loop index is the migration's position in the RUN, which is what
	// decides whether its checks observe apply state — see
	// [Migrator.deferPreMigrationChecks].
	for i, migration := range migrations {
		txMode, err := m.resolveUpMigrationTxMode(migration)
		if err != nil {
			return nil, err
		}
		deferred, err := m.applyUpMigrationObserved(ctx, migration, txMode, i == 0)
		if err != nil {
			return nil, err
		}
		if deferred {
			checksDeferred = append(checksDeferred, migration.Version)
		}
	}

	return checksDeferred, nil
}

func (m *Migrator) applyUpMigrationsInSingleTransaction(ctx context.Context, migrations []*Migration) ([]int64, error) {
	if len(migrations) == 0 {
		return nil, nil
	}
	if err := m.validateUpTransactionMode(migrations); err != nil {
		return nil, err
	}
	checksDeferred, err := m.runBatchPreMigrationChecks(ctx, migrations)
	if err != nil {
		return nil, err
	}

	// Every revision row this batch touches is decided before the transaction
	// opens: once it holds the only connection of a single-connection pool, a
	// read would deadlock. See [Migrator.planUpRetry].
	plans := make(map[string]upRetryPlan, len(migrations))
	for _, migration := range migrations {
		plan, err := m.planUpRetry(ctx, migration)
		if err != nil {
			return nil, err
		}
		// Same reason the plans are read here: the probe queries the catalog, so
		// it has to run before the batch transaction takes the connection. See
		// [Migrator.refuseUpOverUnsafeIndex].
		if err := m.refuseUpOverUnsafeIndex(ctx, migration, plan.resumeFrom, MigrationTxModeAll); err != nil {
			return nil, err
		}
		plans[migration.RevisionVersion()] = plan
	}

	tx, err := m.conn.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx-mode all transaction: %w", err)
	}
	txConn := m.conn.WithExecutor(tx)
	startedAt := make(map[string]time.Time, len(migrations))
	for _, migration := range migrations {
		key := migration.RevisionVersion()
		startedAt[key] = time.Now()
		plan := plans[key]
		migrationCtx := withMigrationResume(ctx, plan.resumeFrom)
		if err := m.applyUpMigrationInExistingTransaction(migrationCtx, txConn, migration, startedAt[key]); err != nil {
			err = migrationFailureAfterRollback(migration.Version, err, tx.Rollback())
			return nil, m.recordRolledBackBatchFailure(ctx, migration, startedAt[key], err, plan)
		}
		if err := m.recordAppliedMigrationOn(ctx, txConn, migration, startedAt[key], plan); err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("failed to record migration %d in tx-mode all transaction: %w", migration.Version, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit tx-mode all transaction: %w", err)
	}
	m.logger.Info("Applied migrations in one transaction", "count", len(migrations))
	return checksDeferred, nil
}

// runBatchPreMigrationChecks evaluates pre-migration checks for a tx-mode all
// run under the same rule as every other up path.
//
// It has work to do only in a dry run. A real batch never reaches here with
// checks, because [Migrator.rejectChecksUnderTxModeAll] refuses a checked
// directory before any migration is examined; a dry run is exempt from that
// refusal, because it opens no batch transaction whose uncommitted state a
// check could miss.
func (m *Migrator) runBatchPreMigrationChecks(ctx context.Context, migrations []*Migration) ([]int64, error) {
	if !m.conn.Writer().IsDryRun() {
		return nil, nil
	}
	var checksDeferred []int64
	for i, migration := range migrations {
		deferred, err := m.runPreMigrationChecks(ctx, migration, i == 0)
		if err != nil {
			return nil, err
		}
		if deferred {
			checksDeferred = append(checksDeferred, migration.Version)
		}
	}
	return checksDeferred, nil
}

func (m *Migrator) validateUpTransactionMode(migrations []*Migration) error {
	resolvedTimeouts := make(map[*Migration]MigrationTimeouts, len(migrations))
	for _, migration := range migrations {
		timeouts, err := m.effectiveUpTimeouts(migration)
		if err != nil {
			return err
		}
		resolvedTimeouts[migration] = timeouts
	}
	if len(migrations) > 0 && m.txMode != MigrationTxModeAll {
		if _, err := m.resolveUpMigrationTxMode(migrations[0]); err != nil {
			return err
		}
	}

	switch m.txMode {
	case MigrationTxModeAll:
		if err := m.validateTxModeAllDialect(); err != nil {
			return err
		}
		for _, migration := range migrations {
			if _, err := m.resolveUpMigrationTxMode(migration); err != nil {
				return err
			}
			if !resolvedTimeouts[migration].IsZero() {
				return fmt.Errorf("migration %d has timeouts and cannot run with tx-mode all", migration.Version)
			}
			if err := m.rejectChecksUnderTxModeAll(migration); err != nil {
				return err
			}
		}
	case MigrationTxModeNone:
		for _, migration := range migrations {
			fileMode := migration.parsedUpTxModeForDialect(m.connectionDialect())
			if fileMode.mode == MigrationFileTxModeFile || fileMode.err != nil {
				continue
			}
			if !resolvedTimeouts[migration].IsZero() {
				return fmt.Errorf("migration %d has timeouts and cannot run with tx-mode none", migration.Version)
			}
		}
	}
	return nil
}

func (m *Migrator) effectiveUpTimeouts(migration *Migration) (MigrationTimeouts, error) {
	timeouts, err := migration.upTimeoutsForDialect(m.connectionDialect())
	if err != nil {
		return MigrationTimeouts{}, fmt.Errorf(
			"migration %d has invalid timeout directives: %w",
			migration.Version,
			err,
		)
	}
	return mergeMigrationTimeouts(m.defaultTimeouts, timeouts), nil
}

func (m *Migrator) effectiveDownTimeouts(migration *Migration) (MigrationTimeouts, error) {
	timeouts, err := migration.downTimeoutsForDialect(m.connectionDialect())
	if err != nil {
		return MigrationTimeouts{}, fmt.Errorf(
			"migration %d has invalid timeout directives: %w",
			migration.Version,
			err,
		)
	}
	return mergeMigrationTimeouts(m.defaultTimeouts, timeouts), nil
}

func (m *Migrator) validateTxModeAllDialect() error {
	dialect := platform.NormalizeDialect(m.conn.Info().Dialect)
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.SQLite:
		return nil
	default:
		return fmt.Errorf("tx-mode all is not supported for dialect %q", m.conn.Info().Dialect)
	}
}

func (m *Migrator) applyUpMigrationObserved(
	ctx context.Context,
	migration *Migration,
	txMode MigrationTxMode,
	observesApplyState bool,
) (checksDeferred bool, err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.apply", m.migrationAttributes(MigrationDirectionUp, migration)...)
	startedAt := time.Now()
	defer func() {
		duration := time.Since(startedAt)
		span.End(err)
		metricAttrs := m.migrationMetricAttributes(MigrationDirectionUp, migration)
		observer.RecordDuration(ctx, "ptah_migration_duration_seconds", duration, metricAttrs...)
		if err != nil {
			observer.AddCounter(ctx, "ptah_migrations_failed_total", 1, metricAttrs...)
			return
		}
		observer.AddCounter(ctx, "ptah_migrations_applied_total", 1, metricAttrs...)
	}()

	m.logger.Info("Applying migration", "version", migration.Version, "description", migration.Description)
	if txMode == MigrationTxModeNone {
		timeouts, timeoutErr := m.effectiveUpTimeouts(migration)
		if timeoutErr != nil {
			return false, timeoutErr
		}
		if err := ensureNoTransactionHasNoTimeouts(
			migration.Version,
			timeouts,
		); err != nil {
			return false, err
		}
		if err := m.validateNoTransactionSQL(migration, MigrationDirectionUp); err != nil {
			return false, err
		}
	}
	if usesTransactionalProgressWitness(m.connectionDialect(), txMode) {
		if err := m.validateTransactionalProgressSQL(migration, MigrationDirectionUp); err != nil {
			return false, err
		}
		if err := m.requireTransactionalTargetEngines(ctx); err != nil {
			return false, err
		}
		if err := m.requireTransactionalTargetIsolation(ctx, migration, MigrationDirectionUp); err != nil {
			return false, err
		}
	}
	plan, err := m.planUpRetry(ctx, migration)
	if err != nil {
		return false, err
	}
	if plan.resumeFrom <= 1 {
		checksDeferred, err = m.runPreMigrationChecks(ctx, migration, observesApplyState)
		if err != nil {
			return false, err
		}
	} else {
		m.logger.Info(
			"Skipping pre-migration checks for partially applied migration",
			"version", migration.Version,
			"resumeFromStatement", plan.resumeFrom,
		)
	}
	// Read-only, and before the revision row is touched: a refusal has to leave
	// whatever an earlier attempt recorded exactly as it found it. See
	// [Migrator.refuseUpOverUnsafeIndex].
	if err := m.refuseUpOverUnsafeIndex(ctx, migration, plan.resumeFrom, txMode); err != nil {
		return false, err
	}
	if txMode == MigrationTxModeNone {
		return checksDeferred, m.applyUpMigrationNoTransaction(ctx, migration, startedAt, plan)
	}
	if usesTransactionalProgressWitness(m.connectionDialect(), txMode) && !m.conn.Writer().IsDryRun() {
		ctx = withMigrationResume(ctx, plan.resumeFrom)
		return checksDeferred, m.applyUpMigrationTransactionalWithPlan(ctx, migration, startedAt, plan)
	}
	if err := m.recordPendingMigrationRevisionOn(ctx, m.conn, migration, startedAt, plan); err != nil {
		return false, fmt.Errorf("failed to record pending migration %d: %w", migration.Version, err)
	}
	if plan.resumeFrom > 1 {
		m.logger.Info(
			"Resuming migration after a partially applied attempt",
			"version", migration.Version,
			"resumeFromStatement", plan.resumeFrom,
		)
	}
	ctx = withMigrationResume(ctx, plan.resumeFrom)
	return checksDeferred, m.applyUpMigrationTransactional(ctx, migration, startedAt)
}

// runPreMigrationChecks evaluates a migration's pre-migration assertions before
// any revision bookkeeping exists for it, and is the only place the up paths
// run them.
//
// Ordering is load-bearing. Checks are read-only reads of committed
// pre-migration state, so running them before the pending-revision insert means
// a failed check leaves the revision table byte-identical: the migration was
// never started, rather than started and marked dirty. Recording the failure
// instead would still cost the Atlas-compatible surface a flag it does not
// have: `ptah-compat migrate apply` registers no --skip-checks (Atlas has none
// either), so a check failure that recorded a dirty row would force every
// subsequent apply through --allow-dirty even after the data that tripped the
// check was fixed. (It would no longer wedge outright — since #966 that flag
// reuses the dirty row instead of failing on a re-insert — but a gate that
// leaves nothing behind needs no recovery at all.) Atlas itself writes no row
// when its checks fail, and the retry simply works (#956).
// observesApplyState says whether this migration observes the state a real
// apply would evaluate its assertions against — true for the first migration
// executed in the run, and always true outside a dry run. When it is false the
// assertions are still parsed and statically validated, but not evaluated, and
// the migration's version is reported back as deferred so the run can say so
// out loud. See [Migrator.deferPreMigrationChecks].
// observesApplyState travels as a parameter rather than as a Migrator field on
// purpose: every With* builder copies the Migrator by value, so a field would
// leak one run's position into every derived migrator.
func (m *Migrator) runPreMigrationChecks( //revive:disable-line:flag-parameter // see above: a field would leak across derived migrators
	ctx context.Context,
	migration *Migration,
	observesApplyState bool,
) (bool, error) {
	if m.deferPreMigrationChecks(observesApplyState) {
		declaresChecks, err := m.validateDeferredMigrationChecks(migration)
		if err != nil {
			return false, fmt.Errorf("pre-migration check failed for migration %d: %w", migration.Version, err)
		}
		return declaresChecks, nil
	}
	if err := m.runMigrationChecks(ctx, m.conn, migration); err != nil {
		return false, fmt.Errorf("pre-migration check failed for migration %d: %w", migration.Version, err)
	}
	return false, nil
}

func (m *Migrator) applyUpMigrationInExistingTransaction(
	ctx context.Context,
	txConn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
) error {
	scoped := *m
	m.logger.Info("Applying migration in tx-mode all", "version", migration.Version, "description", migration.Description)
	// Pre-migration checks are rejected under tx-mode all by
	// validateUpTransactionMode, because a check on the pool connection cannot
	// observe earlier batched migrations' uncommitted changes and would evaluate
	// against stale state. Nothing to run here.
	timeouts, err := m.effectiveUpTimeouts(migration)
	if err != nil {
		return err
	}
	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, timeouts)
	if err != nil {
		return fmt.Errorf("failed to apply timeouts for migration %d: %w", migration.Version, err)
	}
	executionCtx := scoped.withPostgresIndexObservation(ctx, txConn)
	if err := migration.executeUp(executionCtx, txConn, migrationExecutionTransactional); err != nil {
		err = m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err)
		return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
	}
	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		return err
	}
	if err := scoped.refuseUpCompletionOverUnsafeIndex(ctx, txConn, migration); err != nil {
		return err
	}
	m.logger.Info("Applied migration in tx-mode all", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) recordRolledBackBatchFailure(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	plan upRetryPlan,
) error {
	ctx = withMigrationResume(ctx, plan.resumeFrom)
	if beginErr := m.recordPendingMigrationRevisionOn(ctx, m.conn, migration, startedAt, plan); beginErr != nil {
		return fmt.Errorf("%w; additionally failed to record pending migration %d after tx-mode all rollback: %v", failure, migration.Version, beginErr)
	}
	return m.failMigrationWithDirtyStateWithMode(
		ctx,
		migration,
		startedAt,
		failure,
		migration.UpSQL,
		"",
		MigrationTxModeAll,
		MigrationDirectionUp,
	)
}

func (m *Migrator) recordAppliedMigrationOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
	plan upRetryPlan,
) error {
	if err := m.recordPendingMigrationRevisionOn(ctx, conn, migration, startedAt, plan); err != nil {
		return fmt.Errorf("failed to record pending migration %d: %w", migration.Version, err)
	}
	if err := m.completeMigrationRevisionOn(ctx, conn, migration, startedAt); err != nil {
		return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
	}
	return nil
}

func (m *Migrator) applyUpMigrationTransactional(ctx context.Context, migration *Migration, startedAt time.Time) error {
	return m.applyUpMigrationTransactionalOnSession(ctx, migration, startedAt)
}

func (m *Migrator) applyUpMigrationTransactionalWithPlan(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	plan upRetryPlan,
) error {
	return m.withTransactionalMigrationSession(
		ctx,
		migration,
		MigrationDirectionUp,
		func(scoped *Migrator) error {
			if err := scoped.recordPendingMigrationRevisionOn(ctx, scoped.conn, migration, startedAt, plan); err != nil {
				return fmt.Errorf("failed to record pending migration %d: %w", migration.Version, err)
			}
			if plan.resumeFrom > 1 {
				scoped.logger.Info(
					"Resuming migration after a partially applied attempt",
					"version", migration.Version,
					"resumeFromStatement", plan.resumeFrom,
				)
			}
			return scoped.applyUpMigrationTransactionalOnSession(ctx, migration, startedAt)
		},
	)
}

func (m *Migrator) applyUpMigrationTransactionalOnSession(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
) error {
	scoped := *m
	timeouts, err := m.effectiveUpTimeouts(migration)
	if err != nil {
		return err
	}
	// Pre-migration checks already ran in runPreMigrationChecks, before this
	// migration had any revision row: they read committed state on the pool, so
	// they cannot execute inside this transaction (the schema executor exposes no
	// query path) and must not run while the tx holds the only connection of a
	// single-connection pool.
	tx, err := m.conn.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to begin transaction for migration %d", migration.Version),
		)
	}
	txConn := m.conn.WithExecutor(tx)

	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, timeouts)
	if err != nil {
		err = migrationFailureAfterRollback(migration.Version, err, tx.Rollback())
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to apply timeouts for migration %d", migration.Version),
		)
	}

	executionCtx := scoped.withTransactionalProgressRecorder(
		ctx,
		txConn,
		migration,
		startedAt,
		MigrationDirectionUp,
	)
	executionCtx = scoped.withPostgresIndexObservation(executionCtx, txConn)
	if err := migration.executeUp(executionCtx, txConn, migrationExecutionTransactional); err != nil {
		err = m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err)
		err = migrationFailureAfterRollback(migration.Version, err, tx.Rollback())
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to apply migration %d", migration.Version),
		)
	}

	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		err = migrationFailureAfterRollback(migration.Version, err, tx.Rollback())
		return m.failMigrationWithDirtyState(ctx, migration, startedAt, err, migration.UpSQL, "")
	}
	if err := scoped.refuseUpCompletionOverUnsafeIndex(ctx, txConn, migration); err != nil {
		err = migrationFailureAfterRollback(migration.Version, err, tx.Rollback())
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to verify migration %d", migration.Version),
		)
	}
	if err := m.completeMigrationRevisionOn(ctx, txConn, migration, startedAt); err != nil {
		err = migrationFailureAfterRollback(migration.Version, err, tx.Rollback())
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to record migration %d", migration.Version),
		)
	}

	if err := tx.Commit(); err != nil {
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to commit transaction for migration %d", migration.Version),
		)
	}
	m.logger.Info("Applied migration", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) withTransactionalMigrationSession(
	ctx context.Context,
	migration *Migration,
	direction MigrationDirection,
	use func(*Migrator) error,
) error {
	return m.conn.WithSession(ctx, func(conn *dbschema.DatabaseConnection) error {
		scoped := *m
		scoped.conn = conn
		if scoped.migrationsSchema == "" {
			scoped.migrationsSchema = scoped.connectionSchemaName()
		}
		if err := scoped.refuseMySQLTemporaryMetadataShadow(ctx); err != nil {
			return err
		}
		if err := scoped.restoreNoTransactionSessionPrefix(
			ctx,
			migration,
			direction,
			migrationResumeFrom(ctx),
		); err != nil {
			return fmt.Errorf("failed to restore session state for migration %d: %w", migration.Version, err)
		}
		if err := scoped.requireTransactionalTargetEngines(ctx); err != nil {
			return err
		}
		if err := scoped.requireTransactionalTargetIsolation(ctx, migration, direction); err != nil {
			return err
		}
		return use(&scoped)
	})
}

type migrationTransactionRolledBackError struct {
	version int64
	cause   error
}

func (e *migrationTransactionRolledBackError) Error() string {
	return e.cause.Error()
}

func (e *migrationTransactionRolledBackError) Unwrap() error {
	return e.cause
}

func migrationFailureAfterRollback(version int64, failure, rollbackErr error) error {
	if rollbackErr != nil {
		return fmt.Errorf("%w; additionally failed to roll back migration transaction: %v", failure, rollbackErr)
	}
	return &migrationTransactionRolledBackError{version: version, cause: failure}
}

func migrationTransactionRollbackVersion(err error) (int64, bool) {
	var target *migrationTransactionRolledBackError
	if !errors.As(err, &target) {
		return 0, false
	}
	return target.version, true
}

func (m *Migrator) applyUpMigrationNoTransaction(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	plan upRetryPlan,
) error {
	return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
		if err := scoped.restoreNoTransactionSessionPrefix(
			ctx,
			migration,
			MigrationDirectionUp,
			plan.resumeFrom,
		); err != nil {
			return fmt.Errorf("failed to restore session state for migration %d: %w", migration.Version, err)
		}
		if err := scoped.recordPendingMigrationRevisionOn(ctx, scoped.conn, migration, startedAt, plan); err != nil {
			return fmt.Errorf("failed to record pending migration %d: %w", migration.Version, err)
		}
		if plan.resumeFrom > 1 {
			scoped.logger.Info(
				"Resuming migration after a partially applied attempt",
				"version", migration.Version,
				"resumeFromStatement", plan.resumeFrom,
			)
		}
		executionCtx := withMigrationResume(ctx, plan.resumeFrom)
		return scoped.applyUpMigrationNoTransactionOnSession(executionCtx, migration, startedAt)
	})
}

func (m *Migrator) applyUpMigrationNoTransactionOnSession(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
) error {
	executionConn := m.noTransactionConnection()
	// Pre-migration checks already ran in runPreMigrationChecks, before this
	// migration had any revision row.
	executionCtx := withStatementProgressRecorder(
		ctx,
		func(ctx context.Context, event StatementEvent) error {
			return m.markMigrationStatementInFlight(ctx, migration, startedAt, event, MigrationDirectionUp)
		},
		func(ctx context.Context, event StatementEvent) error {
			return m.checkpointMigrationRevision(ctx, migration, startedAt, event, MigrationDirectionUp)
		},
	)
	executionCtx = m.withPostgresIndexObservation(executionCtx, executionConn)
	if err := migration.executeUp(executionCtx, executionConn, migrationExecutionNoTransaction); err != nil {
		return m.failMigrationWithDirtyStateWithMode(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to apply migration %d", migration.Version),
			MigrationTxModeNone,
			MigrationDirectionUp,
		)
	}
	if err := m.refuseUpCompletionOverUnsafeIndex(ctx, executionConn, migration); err != nil {
		return m.failMigrationWithDirtyStateWithMode(
			ctx,
			migration,
			startedAt,
			completedMigrationIndexObservationError(migration, m.connectionDialect(), MigrationDirectionUp, err),
			migration.UpSQL,
			fmt.Sprintf("failed to verify migration %d", migration.Version),
			MigrationTxModeNone,
			MigrationDirectionUp,
		)
	}
	recordCtx, cancelRecord := durableRevisionWriteContext(ctx)
	defer cancelRecord()
	if err := m.completeMigrationRevision(recordCtx, migration, startedAt); err != nil {
		return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
	}
	m.logger.Info("Applied non-transactional migration", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) rollbackMigration(ctx context.Context, migration *Migration, deleteSQL string) error {
	return m.rollbackMigrationObserved(ctx, migration, deleteSQL)
}

func (m *Migrator) rollbackMigrationObserved(ctx context.Context, migration *Migration, deleteSQL string) (err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.rollback", m.migrationAttributes(MigrationDirectionDown, migration)...)
	startedAt := time.Now()
	defer func() {
		duration := time.Since(startedAt)
		span.End(err)
		metricAttrs := m.migrationMetricAttributes(MigrationDirectionDown, migration)
		observer.RecordDuration(ctx, "ptah_migration_duration_seconds", duration, metricAttrs...)
		if err != nil {
			observer.AddCounter(ctx, "ptah_migrations_failed_total", 1, metricAttrs...)
			return
		}
		observer.AddCounter(ctx, "ptah_migrations_rolled_back_total", 1, metricAttrs...)
	}()

	m.logger.Info("Rolling back migration", "version", migration.Version, "description", migration.Description)
	txMode, err := m.resolveDownMigrationTxMode(migration)
	if err != nil {
		return err
	}
	if txMode == MigrationTxModeNone {
		if err := m.validateNoTransactionSQL(migration, MigrationDirectionDown); err != nil {
			return err
		}
		return m.rollbackMigrationNoTransaction(ctx, migration, startedAt, deleteSQL)
	}
	usesWitness := usesTransactionalProgressWitness(m.connectionDialect(), txMode)
	if usesWitness {
		if err := m.validateTransactionalProgressSQL(migration, MigrationDirectionDown); err != nil {
			return err
		}
		if err := m.requireTransactionalTargetEngines(ctx); err != nil {
			return err
		}
		if err := m.requireTransactionalTargetIsolation(ctx, migration, MigrationDirectionDown); err != nil {
			return err
		}
	}
	if !usesWitness || m.conn.Writer().IsDryRun() {
		if err := m.beginRollbackRevision(ctx, migration, startedAt); err != nil {
			return fmt.Errorf("failed to record pending rollback %d: %w", migration.Version, err)
		}
	}
	return m.rollbackMigrationTransactional(ctx, migration, startedAt, deleteSQL)
}

func (m *Migrator) operationAttributes(direction MigrationDirection) []ObservationAttribute {
	attrs := []ObservationAttribute{
		attr("db.system", m.connectionDialect()),
	}
	if direction != "" {
		attrs = append(attrs, attr("migration.direction", string(direction)))
	}
	return attrs
}

func (m *Migrator) migrationAttributes(direction MigrationDirection, migration *Migration) []ObservationAttribute {
	return []ObservationAttribute{
		attr("db.system", m.connectionDialect()),
		attr("migration.direction", string(direction)),
		attr("migration.version", migration.Version),
		attr("migration.description", migration.Description),
	}
}

func (m *Migrator) migrationMetricAttributes(direction MigrationDirection, migration *Migration) []ObservationAttribute {
	return []ObservationAttribute{
		attr("db.system", m.connectionDialect()),
		attr("migration.direction", string(direction)),
		attr("migration.version", migration.Version),
	}
}

func (m *Migrator) rollbackMigrationTransactional(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	deleteSQL string,
) error {
	if usesTransactionalProgressWitness(m.connectionDialect(), MigrationTxModeFile) && !m.conn.Writer().IsDryRun() {
		return m.withTransactionalMigrationSession(
			ctx,
			migration,
			MigrationDirectionDown,
			func(scoped *Migrator) error {
				if err := scoped.beginRollbackRevision(ctx, migration, startedAt); err != nil {
					return fmt.Errorf("failed to record pending rollback %d: %w", migration.Version, err)
				}
				return scoped.rollbackMigrationTransactionalOnSession(ctx, migration, startedAt, deleteSQL)
			},
		)
	}
	return m.rollbackMigrationTransactionalOnSession(ctx, migration, startedAt, deleteSQL)
}

func (m *Migrator) rollbackMigrationTransactionalOnSession(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	deleteSQL string,
) error {
	scoped := *m
	timeouts, err := m.effectiveDownTimeouts(migration)
	if err != nil {
		return err
	}
	tx, err := m.conn.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return m.failRollbackWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to begin transaction for migration %d", migration.Version),
		)
	}
	txConn := m.conn.WithExecutor(tx)

	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, timeouts)
	if err != nil {
		err = migrationFailureAfterRollback(migration.Version, err, tx.Rollback())
		return m.failRollbackWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to apply timeouts for migration %d", migration.Version),
		)
	}

	executionCtx := scoped.withTransactionalProgressRecorder(
		ctx,
		txConn,
		migration,
		startedAt,
		MigrationDirectionDown,
	)
	executionCtx = scoped.withPostgresIndexObservation(executionCtx, txConn)
	if err := migration.executeDown(executionCtx, txConn, migrationExecutionTransactional); err != nil {
		err = m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err)
		err = migrationFailureAfterRollback(migration.Version, err, tx.Rollback())
		return m.failRollbackWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to revert migration %d", migration.Version),
		)
	}

	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		err = migrationFailureAfterRollback(migration.Version, err, tx.Rollback())
		return m.failRollbackWithDirtyState(ctx, migration, startedAt, err, migration.DownSQL, "")
	}
	if err := scoped.refuseRollbackCompletionOverUnsafeIndexOn(ctx, txConn, migration); err != nil {
		err = migrationFailureAfterRollback(migration.Version, err, tx.Rollback())
		return m.failRollbackWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to verify rollback of migration %d", migration.Version),
		)
	}

	if err := txConn.Writer().ExecuteSQL(ctx, deleteSQL, m.migrationRevisionVersionArg(migration)); err != nil {
		err = migrationFailureAfterRollback(migration.Version, err, tx.Rollback())
		return m.failRollbackWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to record migration reversion %d", migration.Version),
		)
	}

	if err := tx.Commit(); err != nil {
		return m.failRollbackWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to commit transaction for migration %d", migration.Version),
		)
	}

	m.logger.Info("Rolled back migration", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) rollbackMigrationNoTransaction(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	deleteSQL string,
) error {
	return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
		if err := scoped.beginRollbackRevision(ctx, migration, startedAt); err != nil {
			return fmt.Errorf("failed to record pending rollback %d: %w", migration.Version, err)
		}
		return scoped.rollbackMigrationNoTransactionOnSession(ctx, migration, startedAt, deleteSQL)
	})
}

func (m *Migrator) rollbackMigrationNoTransactionOnSession(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	deleteSQL string,
) error {
	executionConn := m.noTransactionConnection()
	m.startPostgresIndexObservation()
	executionCtx := withStatementProgressRecorder(
		ctx,
		func(ctx context.Context, event StatementEvent) error {
			return m.markMigrationStatementInFlight(ctx, migration, startedAt, event, MigrationDirectionDown)
		},
		func(ctx context.Context, event StatementEvent) error {
			return m.checkpointMigrationRevision(ctx, migration, startedAt, event, MigrationDirectionDown)
		},
	)
	executionCtx = m.withPostgresIndexObservation(executionCtx, executionConn)
	if err := migration.executeDown(executionCtx, executionConn, migrationExecutionNoTransaction); err != nil {
		return m.failRollbackWithDirtyStateWithMode(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to revert migration %d", migration.Version),
			MigrationTxModeNone,
		)
	}
	if err := m.refuseRollbackCompletionOverUnsafeIndexOn(ctx, executionConn, migration); err != nil {
		return m.failRollbackWithDirtyStateWithMode(
			ctx,
			migration,
			startedAt,
			completedMigrationIndexObservationError(migration, m.connectionDialect(), MigrationDirectionDown, err),
			migration.DownSQL,
			fmt.Sprintf("failed to verify rollback of migration %d", migration.Version),
			MigrationTxModeNone,
		)
	}
	recordCtx, cancelRecord := durableRevisionWriteContext(ctx)
	defer cancelRecord()
	if err := executeSQLOutsideTransaction(recordCtx, m.conn, deleteSQL, m.migrationRevisionVersionArg(migration)); err != nil {
		return fmt.Errorf("failed to record migration reversion %d: %w", migration.Version, err)
	}
	m.logger.Info("Rolled back non-transactional migration", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) failMigrationWithDirtyState(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	sqlText string,
	prefix string,
) error {
	return m.failMigrationWithDirtyStateWithMode(
		ctx,
		migration,
		startedAt,
		failure,
		sqlText,
		prefix,
		MigrationTxModeFile,
		MigrationDirectionUp,
	)
}

// failRollbackWithDirtyState records a failed rollback as dirty state. Ptah
// keeps the Atlas revision-table schema compatible but does not reproduce the
// upstream behavior that hides a partially applied rollback behind a clean row.
func (m *Migrator) failRollbackWithDirtyState(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	sqlText string,
	prefix string,
) error {
	return m.failRollbackWithDirtyStateWithMode(ctx, migration, startedAt, failure, sqlText, prefix, MigrationTxModeFile)
}

func (m *Migrator) failRollbackWithDirtyStateWithMode(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	sqlText string,
	prefix string,
	txMode MigrationTxMode,
) error {
	return m.failMigrationWithDirtyStateWithMode(
		ctx,
		migration,
		startedAt,
		failure,
		sqlText,
		prefix,
		txMode,
		MigrationDirectionDown,
	)
}

func (m *Migrator) failMigrationWithDirtyStateWithMode(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	sqlText string,
	prefix string,
	txMode MigrationTxMode,
	direction MigrationDirection,
) error {
	revisionErr := m.failMigrationRevisionWithMode(ctx, migration, startedAt, failure, sqlText, txMode, direction)
	if revisionErr != nil {
		if prefix == "" {
			return fmt.Errorf("%w; additionally failed to record dirty migration state: %v", failure, revisionErr)
		}
		return fmt.Errorf("%s: %w; additionally failed to record dirty migration state: %v", prefix, failure, revisionErr)
	}
	if prefix == "" {
		return failure
	}
	return fmt.Errorf("%s: %w", prefix, failure)
}

func ensureNoTransactionHasNoTimeouts(version int64, timeouts MigrationTimeouts) error {
	if timeouts.IsZero() {
		return nil
	}
	return fmt.Errorf("migration %d is marked no_transaction, so migration timeouts cannot be applied safely", version)
}

func (m *Migrator) migrationsToApply(
	migrations []*Migration,
	applied []int64,
	appliedIdentities migrationIdentitySet,
	targetVersion int64,
) ([]*Migration, error) {
	currentVersion := maxAppliedVersion(applied)
	bootstrap := checkpointBootstrap(migrations, applied, targetVersion)
	floor := checkpointFloor(migrations, applied, bootstrap)
	pendingMigrationList := pendingMigrationsFloored(migrations, appliedIdentities, bootstrap, floor, targetVersion)
	pendingVersions := migrationVersions(pendingMigrationList)
	// Two comparisons, one verdict. The numeric one asks whether a pending
	// version sorts below the mark; the source one asks whether the tool this
	// directory belongs to would call the same file out of order. They
	// disagree on real directories, so both are asked and the exemption is
	// applied to the union rather than to either half — see
	// [Migrator.WithSourceVersions].
	outOfOrderVersions := outOfOrderExempt(
		mergeOutOfOrderVersions(
			outOfOrderMigrationVersions(pendingVersions, currentVersion),
			outOfOrderSourceVersions(
				pendingVersions,
				applied,
				appliedIdentities.revisionKeys(),
				m.sourceVersions,
			),
		),
		m.outOfOrderExempt,
	)
	execOrder := normalizeExecOrder(m.execOrder)

	if execOrder == ExecOrderLinear && len(outOfOrderVersions) > 0 {
		err := NewOutOfOrderSourceError(currentVersion, outOfOrderVersions, m.sourceVersions)
		if _, mappedCurrent := m.sourceVersions[currentVersion]; !mappedCurrent {
			if currentSource, ok := highestAppliedSourceVersion(
				applied,
				appliedIdentities.revisionKeys(),
				m.sourceVersions,
			); ok {
				err.currentSourceVersion = currentSource
				err.currentSourceVersionSet = true
			}
		}
		return nil, err
	}

	migrationsToApply := make([]*Migration, 0, len(pendingMigrationList))
	for _, migration := range pendingMigrationList {
		// linear-skip leaves unapplied exactly what linear refuses, so it reads
		// the verdict computed above instead of re-deriving it. Re-deriving is
		// what let a converted directory skip nothing under linear-skip while
		// linear refused, and execute a migration the source tool leaves
		// pending.
		if execOrder == ExecOrderLinearSkip && slices.Contains(outOfOrderVersions, migration.Version) {
			m.logger.Warn("Skipping out-of-order migration", "version", migration.Version, "currentVersion", currentVersion)
			continue
		}
		migrationsToApply = append(migrationsToApply, migration)
	}

	return migrationsToApply, nil
}

// checkpointBootstrap returns the newest checkpoint the migrator runs to
// bootstrap a fresh database, or nil. A checkpoint only bootstraps a database
// with no applied migrations; an already-migrated database never runs one.
func checkpointBootstrap(migrations []*Migration, applied []int64, targetVersion int64) *Migration {
	if len(applied) != 0 {
		return nil
	}
	var best *Migration
	for _, migration := range migrations {
		if !migration.IsCheckpoint {
			continue
		}
		if targetVersion > 0 && migration.Version > targetVersion {
			continue
		}
		if best == nil || migration.Version > best.Version {
			best = migration
		}
	}
	return best
}

// checkpointFloor returns the version below which migrations are squashed by a
// checkpoint and must not be applied individually: the bootstrap checkpoint's
// version on a fresh database, otherwise the highest applied checkpoint's
// version (0 when no checkpoint applies).
func checkpointFloor(migrations []*Migration, applied []int64, bootstrap *Migration) int64 {
	if bootstrap != nil {
		return bootstrap.Version
	}
	appliedSet := versionSet(applied)
	var floor int64
	for _, migration := range migrations {
		if !migration.IsCheckpoint {
			continue
		}
		if _, ok := appliedSet[migration.Version]; ok && migration.Version > floor {
			floor = migration.Version
		}
	}
	return floor
}

// checkpointRunnable reports whether a migration is eligible to run given the
// checkpoint bootstrap decision. Only the bootstrap checkpoint runs; other
// checkpoints never do, and ordinary migrations below the squash floor are
// covered by the checkpoint and skipped.
func checkpointRunnable(migration *Migration, bootstrap *Migration, floor int64) bool {
	if migration.IsCheckpoint {
		return migration == bootstrap
	}
	return migration.Version >= floor
}

// checkpointRollbackBoundary returns the applied checkpoint version that blocks
// a rollback to targetVersion, or 0 when the rollback is allowed. A checkpoint
// squashes the history below its version into a single snapshot, so rolling
// back to a version between 1 and that boundary cannot reconstruct the
// intermediate pre-checkpoint state. Rolling back to the checkpoint version
// itself, or all the way to 0 (drop everything), stays allowed.
func checkpointRollbackBoundary(migrations []*Migration, applied []int64, targetVersion int64) int64 {
	if targetVersion <= 0 {
		return 0
	}
	boundary := checkpointFloor(migrations, applied, nil)
	if boundary > 0 && targetVersion < boundary {
		return boundary
	}
	return 0
}

func pendingMigrationVersions(migrations []*Migration, applied []int64) []int64 {
	bootstrap := checkpointBootstrap(migrations, applied, 0)
	floor := checkpointFloor(migrations, applied, bootstrap)
	return migrationVersions(pendingMigrationsFloored(
		migrations,
		newMigrationIdentitySet(applied, nil),
		bootstrap,
		floor,
		0,
	))
}

// pendingMigrationsFloored computes the migrations that would be applied
// next given a checkpoint bootstrap decision: the bootstrap checkpoint (on a
// fresh database) plus any ordinary migration at or above the squash floor that
// is not yet applied. Squashed history and non-bootstrap checkpoints are not
// pending.
func pendingMigrationsFloored(
	migrations []*Migration,
	applied migrationIdentitySet,
	bootstrap *Migration,
	floor int64,
	targetVersion int64,
) []*Migration {
	pending := make([]*Migration, 0, len(migrations))
	for _, migration := range migrations {
		if !checkpointRunnable(migration, bootstrap, floor) {
			continue
		}
		if applied.containsMigration(migration) {
			continue
		}
		if targetVersion > 0 && migration.Version > targetVersion {
			continue
		}
		pending = append(pending, migration)
	}
	return pending
}

func outOfOrderMigrationVersions(pending []int64, currentVersion int64) []int64 {
	outOfOrder := make([]int64, 0)
	for _, version := range pending {
		if version < currentVersion {
			outOfOrder = append(outOfOrder, version)
		}
	}
	return outOfOrder
}

func outOfOrderMigrationKeys(pending []*Migration, currentVersion int64) []string {
	outOfOrder := make([]string, 0)
	for _, migration := range pending {
		if migration.Version < currentVersion {
			outOfOrder = append(outOfOrder, migration.RevisionVersion())
		}
	}
	return outOfOrder
}

func maxAppliedVersion(applied []int64) int64 {
	if len(applied) == 0 {
		return 0
	}
	return slices.Max(applied)
}

func versionSet(versions []int64) map[int64]struct{} {
	set := make(map[int64]struct{}, len(versions))
	for _, version := range versions {
		set[version] = struct{}{}
	}
	return set
}

func migrationsByVersion(migrations []*Migration) map[int64]*Migration {
	result := make(map[int64]*Migration, len(migrations))
	for _, migration := range migrations {
		result[migration.Version] = migration
	}
	return result
}

func migrationsToRollback(migrationsByVersion map[int64]*Migration, applied []int64, targetVersion int64) ([]*Migration, error) {
	rollbackVersions := make([]int64, 0, len(applied))
	for _, version := range applied {
		if version > targetVersion {
			rollbackVersions = append(rollbackVersions, version)
		}
	}
	sort.Slice(rollbackVersions, func(i, j int) bool { return rollbackVersions[i] > rollbackVersions[j] })

	rollbackMigrations := make([]*Migration, 0, len(rollbackVersions))
	for _, version := range rollbackVersions {
		migration, ok := migrationsByVersion[version]
		if !ok {
			return nil, fmt.Errorf("applied migration %d is above target version %d but is missing from the migration provider", version, targetVersion)
		}
		rollbackMigrations = append(rollbackMigrations, migration)
	}
	return rollbackMigrations, nil
}

func (m *Migrator) validateDownMigrations(migrations []*Migration) error {
	if err := m.reportMisplacedDirectives(migrations, MigrationDirectionDown); err != nil {
		return err
	}
	for _, migration := range migrations {
		timeouts, err := m.effectiveDownTimeouts(migration)
		if err != nil {
			return err
		}
		if migration.downUnavailable {
			return &AtlasDownNotImplementedError{
				Version:     migration.Version,
				Description: migration.Description,
			}
		}
		txMode, err := m.resolveDownMigrationTxMode(migration)
		if err != nil {
			return err
		}
		if txMode == MigrationTxModeNone {
			if err := ensureNoTransactionHasNoTimeouts(
				migration.Version,
				timeouts,
			); err != nil {
				return err
			}
		}
	}
	return nil
}
