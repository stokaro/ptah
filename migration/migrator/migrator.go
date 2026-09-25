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
	"sync/atomic"
	"time"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/revisiontable"
	"ptah.run/internal/sqliterebuild"
	"ptah.run/migration/migrationfile"
)

// MigrationStatus represents the current state of migrations: what the
// revision metadata table records, measured against what the migration
// provider holds. [Migrator.GetMigrationStatus] computes it; MarshalJSON and
// UnmarshalJSON keep the CurrentVersionKeySet presence bit across
// serialization.
type MigrationStatus struct {
	// CurrentVersion is the highest version the revision table records: the
	// highest applied version under the Ptah format, and the highest recorded
	// revision version under the Atlas format.
	CurrentVersion int64 `json:"current_version"`
	// CurrentVersionKey is the exact revision identity the status points at:
	// the current version's identity, or the dirty revision's while one
	// exists. For a native directory it is a decimal version spelling; Atlas
	// repeatable migrations carry opaque tokens such as "2R".
	CurrentVersionKey string `json:"current_version_key,omitempty"`
	// CurrentVersionKeySet reports whether CurrentVersionKey carries an
	// identity at all: it distinguishes an exact empty identity, which one
	// Flyway repeatable migration records, from no identity. The bit is
	// authoritative — a CurrentVersionKey left behind with the bit clear is not
	// serialized as an identity — and it survives a JSON round trip.
	CurrentVersionKeySet bool `json:"-"`
	// AppliedMigrations holds the applied versions; AppliedMigrationKeys aligns
	// with it index for index and carries each row's exact revision identity.
	AppliedMigrations    []int64  `json:"applied_migrations"`
	AppliedMigrationKeys []string `json:"applied_migration_keys,omitempty"`
	// PendingMigrations holds the provider migrations not yet applied;
	// PendingMigrationKeys aligns with it index for index.
	PendingMigrations    []int64  `json:"pending_migrations"`
	PendingMigrationKeys []string `json:"pending_migration_keys,omitempty"`
	// OutOfOrderMigrations holds the pending versions below CurrentVersion;
	// OutOfOrderMigrationKeys aligns with it index for index. Whether such a
	// migration runs is decided by the migrator's [ExecOrder].
	OutOfOrderMigrations    []int64  `json:"out_of_order_migrations"`
	OutOfOrderMigrationKeys []string `json:"out_of_order_migration_keys,omitempty"`
	// TotalMigrations counts the migrations the provider holds.
	TotalMigrations int `json:"total_migrations"`
	// HasPendingChanges reports that pending migrations exist or that a dirty
	// revision does.
	HasPendingChanges bool `json:"has_pending_changes"`
	// DirtyRevision is a revision row a failed or interrupted run left in a
	// non-applied state, and nil when every row is clean. While it is non-nil,
	// migration operations refuse to continue until the row is repaired; see
	// [Migrator.RepairMigration].
	DirtyRevision *MigrationRevision `json:"dirty_revision,omitempty"`
	// ContractVersion is [StatusContractVersion], so a consumer can refuse a
	// document it does not understand instead of reading the fields it
	// recognizes out of a shape that means something else.
	ContractVersion int `json:"contract_version"`
	// Migrations is every migration the directory holds, in directory order,
	// with what the file is, what the database recorded for it, and whether the
	// two still agree. The version lists above answer "which versions"; this
	// answers the questions a caller has to settle before it may execute
	// anything.
	Migrations []MigrationRecord `json:"migrations,omitempty"`
	// MissingMigrations are the revisions the database records as applied that
	// the directory holds no file for, in recorded order. Each carries the
	// state "missing" and an empty Checksum, because there is no file to hash.
	//
	// They are reported separately because Migrations answers "what does this
	// directory hold"; a row with no file is not an entry in that list, and
	// folding it in would change what the list means. A revision below a
	// checkpoint or a baseline is absent from here: those boundaries are what
	// make a missing file the intended shape.
	MissingMigrations []MigrationRecord `json:"missing_migrations,omitempty"`
	// CheckpointVersion is the checkpoint that covers the migrations below it:
	// the one a fresh database bootstraps from, or the newest one this database
	// has applied. It is zero when no checkpoint covers anything. A migration
	// below it is covered rather than missing, and stays covered once the
	// checkpoint is applied -- applying it is what the bootstrap did.
	CheckpointVersion int64 `json:"checkpoint_version,omitempty"`
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

// MigrationPlanGuard decides whether a selected plan may run at all. It runs
// under the migration lock for every selection, the empty one included, before
// transaction-mode validation, any pre-migration hook and any change, so a
// refusal leaves the schema and the revision table exactly as they were.
type MigrationPlanGuard func(ctx context.Context, plan MigrationPlan) error

// MigrateUpOptions selects the pending up migration plan.
type MigrateUpOptions struct {
	// TargetVersion limits the run to pending migrations at or below this
	// version. Zero means latest.
	TargetVersion int64
	// RefuseTargetVersionAlreadyPassed fails a run that cannot reach
	// TargetVersion with a [*TargetVersionPassedError] instead of selecting
	// nothing. It is read only when TargetVersion is above zero, and only when
	// the selection came out empty, so a target still reachable through an
	// out-of-order pending migration is applied either way. A caller that hands
	// an operator's exact version to the migrator sets it; a caller for which
	// an empty run is a successful no-op leaves it clear.
	RefuseTargetVersionAlreadyPassed bool
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
	// PlanGuard refuses a selected plan before anything else acts on it. Unlike
	// Preflight it runs for an empty selection too: a caller that approved one
	// exact plan has not approved "nothing to do", and a run that selected
	// nothing where work was approved is a history that moved, not a success.
	PlanGuard MigrationPlanGuard
	// ChecksDeferredObserver receives the versions whose checks were parsed
	// and statically validated but not evaluated against the database, because
	// a dry run cannot produce the state they are about. A postcondition is
	// deferred by any dry run and a precondition by every position but the
	// first, so one version reaching this list can mean either. It
	// runs after a successful run and only when the list is non-empty, so a
	// preview can say how much of the guard it did not answer instead of
	// dropping it silently.
	ChecksDeferredObserver ChecksDeferredObserver
}

// ChecksDeferredObserver is notified with the migration versions whose checks
// a run declined to evaluate. The slice is owned by the caller and must not be
// retained.
type ChecksDeferredObserver func(ctx context.Context, versions []int64)

// Migrator executes and tracks database migrations: it takes migrations from
// a [MigrationProvider], applies or rolls them back over one
// dbschema.DatabaseConnection, and records each outcome in the revision
// metadata table. Configuration happens through the With* methods, which
// return modified copies rather than mutating the receiver. A Migrator
// instance is not safe for concurrent use from multiple goroutines; concurrent
// runs from separate processes are serialized by the migration advisory lock
// instead, on the dialects that support one.
type Migrator struct {
	conn                 *dbschema.DatabaseConnection
	noTransactionSession *dbschema.DatabaseConnection
	migrationProvider    MigrationProvider
	defaultTimeouts      migrationfile.Timeouts
	migrationsTable      string
	migrationsSchema     string
	// atlasPlacedSchema reports that migrationsSchema holds Atlas's placement
	// for its revision table rather than a schema the caller named. See
	// placeAtlasRevisionTable.
	atlasPlacedSchema    bool
	migrationsEngine     string
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
	initializedDryRun bool
	logger            *slog.Logger
	// migrationLogEnabled turns the append-only operation log on. It is a
	// separate table from the revision one and answers a separate question;
	// see [MigrationLogEntry].
	migrationLogEnabled bool
	// migrationLogRunID groups the entries one invocation writes.
	migrationLogRunID string
	// migrationLogSeq orders them. It is a pointer because every With*
	// builder copies the Migrator by value, and the entries of one run have to
	// share a counter or two derived migrators would both start at one.
	migrationLogSeq *atomic.Int64
	// migrationLogReady memoizes that the table has been created, so a run
	// that writes two entries per migration sends its DDL once. A pointer for
	// the same reason the counter is one.
	migrationLogReady *atomic.Bool
	// migrationLogRefused latches a log table this connection does not own, so
	// the refusal is reported once rather than on every entry the run would
	// have written.
	migrationLogRefused *atomic.Bool
	// actor is the name the log records for this run, and actorSource says
	// what that name is worth. See [ActorSource].
	actor                    string
	actorSource              ActorSource
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

// NewMigrator creates a migrator that executes the provider's migrations over
// conn. The returned migrator uses the Ptah revision-table format in a
// schema_migrations table, linear execution order, per-file transaction mode,
// the "ptah_migrate" advisory lock name, slog.Default() logging, a no-op
// observer, and enforced pre-migration checks; the With* methods return
// adjusted copies. A nil conn supports only work that never touches the
// database, such as inspecting the provider through
// [Migrator.MigrationProvider]; every migration, status, and revision method
// requires a real connection.
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
		migrationLogEnabled: true,
		migrationLogRunID:   newMigrationLogRunID(),
		migrationLogSeq:     new(atomic.Int64),
		migrationLogReady:   new(atomic.Bool),
		migrationLogRefused: new(atomic.Bool),
		actor:               actorName,
		actorSource:         actorSource,
	}
}

// WithMigrationLog returns a copy of the migrator that writes, or does not
// write, the append-only operation log.
//
// On by default: a database that cannot say what happened to it is the gap
// this exists to close, and a log nobody turned on closes nothing. The switch
// is for the caller that must not add a table to the database it migrates --
// the Atlas-compatible surface is the one in the tree, which ships the
// revision table Atlas defines and nothing beside it.
func (m *Migrator) WithMigrationLog(enabled bool) *Migrator {
	tmp := *m
	tmp.migrationLogEnabled = enabled
	return &tmp
}

// WithActor returns a copy of the migrator that records name as the actor of
// its operations.
//
// An empty name falls back to the user the process runs as, and the two are
// recorded as different claims: see [ActorSource]. Nothing here verifies
// either, which is why the provenance travels with the name.
func (m *Migrator) WithActor(name string) *Migrator {
	tmp := *m
	tmp.actor, tmp.actorSource = resolveActor(name)
	return &tmp
}

// WithLogger returns a copy of the migrator that logs through l. A nil logger
// falls back to slog.Default().
func (m *Migrator) WithLogger(l *slog.Logger) *Migrator {
	tmp := *m
	if l == nil {
		l = slog.Default()
	}
	tmp.logger = l
	return &tmp
}

// WithObserver returns a copy of the migrator that reports tracing and metrics
// through the given observer. A nil observer falls back to [NoopObserver].
func (m *Migrator) WithObserver(observer Observer) *Migrator {
	tmp := *m
	if observer == nil {
		observer = NoopObserver{}
	}
	tmp.observer = observer
	return &tmp
}

// WithSkipChecks returns a copy of the migrator with pre-migration check
// enforcement configured: whether `-- +ptah check` assertions and Atlas txtar
// checks.sql and checks/*.sql sections are evaluated before applying up
// migrations. The default (false) enforces checks; pass true as an explicit
// emergency bypass, mirroring --allow-destructive.
func (m *Migrator) WithSkipChecks(skip bool) *Migrator {
	tmp := *m
	tmp.skipChecks = skip
	return &tmp
}

// migrationCheckGroups collects a migration's pre-migration checks: Atlas
// txtar check files first (in archive order), then `-- +ptah check` directives
// parsed from the body the given direction is about to run.
//
// The direction is explicit rather than implicitly up. With it implicit, a
// `-- +ptah check` written into a down body is parsed by nothing and ignored
// without a word -- no error, no warning, no assertion, and the rollback simply
// runs. A rollback is where a precondition is worth asserting most, and a
// safety gate that is accepted and discarded is worse than one that was never
// offered (stokaro/ptah#1715).
//
// Atlas txtar check files stay attached to the migration rather than to a
// direction: the archive carries one checks.sql for the migration, and there is
// no down half of it to read.
func (m *Migrator) migrationCheckGroups(
	migration *Migration,
	direction MigrationDirection,
) ([]checkGroup, error) {
	dialect := m.connectionDialect()
	groups := make([]checkGroup, 0, len(migration.atlasCheckFiles)+1)
	for _, file := range migration.atlasCheckFiles {
		mode := atlasCheckFileMode(file.SQL, dialect)
		checks := parseAtlasTxtarChecks(file.Name, file.SQL, dialect)
		if len(checks) == 0 && mode != checkGroupOneOf {
			continue
		}
		groups = append(groups, checkGroup{
			name:   file.Name,
			checks: checks,
			mode:   mode,
		})
	}

	body := migration.UpSQL
	if direction == MigrationDirectionDown {
		body = migration.DownSQL
	}
	parsed, err := ParseChecks(body, dialect)
	if err != nil {
		return nil, fmt.Errorf("migration %d has invalid check directives: %w", migration.Version, err)
	}
	for _, phase := range []CheckPhase{CheckPhaseBefore, CheckPhaseAfter} {
		checks := checksInPhase(parsed, phase)
		if len(checks) == 0 {
			continue
		}
		groups = append(groups, checkGroup{checks: checks, mode: checkGroupAll, phase: phase})
	}
	return groups, nil
}

// checksInPhase selects the checks a phase evaluates, keeping the order they
// were written in. A migration may declare both phases, and the two are
// separate groups because they run at different points against different
// state.
func checksInPhase(checks []Check, phase CheckPhase) []Check {
	selected := make([]Check, 0, len(checks))
	for _, check := range checks {
		if check.Phase != phase {
			continue
		}
		selected = append(selected, check)
	}
	return selected
}

// groupsInPhase selects the check groups a phase evaluates.
//
// An Atlas txtar check file names no phase and carries the zero value, so it
// belongs to the precondition phase along with every `-- +ptah check` that
// named none. Both ends of the selection read this one predicate: a second
// list would agree with the first until a phase was added.
func groupsInPhase(groups []checkGroup, phase CheckPhase) []checkGroup {
	selected := make([]checkGroup, 0, len(groups))
	for _, group := range groups {
		groupPhase := group.phase
		if groupPhase == "" {
			groupPhase = CheckPhaseBefore
		}
		if groupPhase != phase {
			continue
		}
		selected = append(selected, group)
	}
	return selected
}

// runMigrationChecks evaluates the assertion checks a migration's body
// declares for one phase (`-- +ptah check` directives and, before the body,
// Atlas txtar checks.sql sections) against conn. It is a no-op when checks are
// skipped. A malformed check directive or an unsatisfied assertion returns an
// error; what the caller does with it depends on the phase, because before the
// body nothing is applied and after it everything is.
func (m *Migrator) runMigrationChecks(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	direction MigrationDirection,
	phase CheckPhase,
) error {
	if m.skipChecks {
		return nil
	}
	groups, err := m.migrationCheckGroups(migration, direction)
	if err != nil {
		return err
	}
	selected := groupsInPhase(groups, phase)
	if len(selected) == 0 {
		return nil
	}
	info := conn.Info()
	return runCheckGroups(ctx, conn, info.Dialect, info.Version, migration.Version, selected)
}

// runPostMigrationChecks evaluates a migration's `phase=after` assertions once
// its body has run and its revision says applied, and reports whether the run
// deferred them instead.
//
// A failure here is not the migration failing. The body is committed, so the
// error says applied and the run stops rather than retrying or rolling back;
// what to do about a database that took the change without reaching the state
// it was for is an operator's decision, not a migrator's (stokaro/ptah#3405).
//
// A dry run defers every postcondition, whatever its position in the run. A
// precondition on the first migration observes the state a real apply would
// give it, which is why that one is evaluated; a postcondition asks about the
// state the body produces, and a dry run has refused to produce it.
func (m *Migrator) runPostMigrationChecks(
	ctx context.Context,
	migration *Migration,
	direction MigrationDirection,
) (bool, error) {
	if m.skipChecks {
		return false, nil
	}
	groups, err := m.migrationCheckGroups(migration, direction)
	if err != nil {
		return false, err
	}
	selected := groupsInPhase(groups, CheckPhaseAfter)
	if len(selected) == 0 {
		return false, nil
	}
	info := m.conn.Info()
	if m.conn.Writer().IsDryRun() {
		if err := validateCheckGroups(selected, info.Dialect, info.Version, migration.Version); err != nil {
			return false, &PostMigrationCheckFailedError{
				Version: migration.Version, Direction: direction, Err: err,
			}
		}
		return true, nil
	}
	if err := m.runMigrationChecks(ctx, m.conn, migration, direction, CheckPhaseAfter); err != nil {
		return false, &PostMigrationCheckFailedError{
			Version: migration.Version, Direction: direction, Err: err,
		}
	}
	return false, nil
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
	groups, err := m.migrationCheckGroups(migration, MigrationDirectionUp)
	if err != nil {
		return false, err
	}
	selected := groupsInPhase(groups, CheckPhaseBefore)
	if len(selected) == 0 {
		return false, nil
	}
	info := m.conn.Info()
	if err := validateCheckGroups(selected, info.Dialect, info.Version, migration.Version); err != nil {
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

// txModeAllExclusionAdvice is the one place the two --tx-mode all exclusions
// are explained.
//
// A timeout and a pre-migration check are both scoped to one migration, and
// `--tx-mode all` deliberately has no such scope: one transaction spans every
// migration in the run. A timeout set inside it would bound the whole batch
// rather than the file that asked for it, and a check would read committed
// pre-batch state and evaluate its precondition against a database the earlier
// migrations in the same transaction have already changed.
//
// Both refusals name the migration and the feature and point here, because a
// user who adopts tx-mode all for atomicity otherwise discovers the two
// incompatibilities one migration at a time (stokaro/ptah#1713).
const txModeAllExclusionAdvice = "tx-mode all runs every migration in one transaction, " +
	"so a per-migration timeout would bound the whole batch and a per-migration check would " +
	"read state the batch has already changed; use the default per-file transaction mode, or " +
	"remove the directive from this migration"

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
func (m *Migrator) rejectChecksUnderTxModeAll(migration *Migration, direction MigrationDirection) error {
	if m.skipChecks {
		return nil
	}
	groups, err := m.migrationCheckGroups(migration, direction)
	if err != nil {
		return err
	}
	if len(groups) > 0 {
		return fmt.Errorf(
			"migration %d declares pre-migration checks, which cannot run with tx-mode all: "+
				"%s", migration.Version, txModeAllExclusionAdvice)
	}
	return nil
}

// WithExecOrder returns a copy of the migrator that handles pending migrations
// whose version is below the current high-water mark according to execOrder.
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

// WithTransactionMode returns a copy of the migrator that wraps pending up
// migrations in transactions according to mode. An empty mode selects the
// default, [MigrationTxModeFile].
func (m *Migrator) WithTransactionMode(mode MigrationTxMode) *Migrator {
	tmp := *m
	tmp.txMode = normalizeMigrationTxMode(mode)
	return &tmp
}

// WithMigrationsEngine names the storage engine the revision table is created
// with. An empty engine leaves the dialect's own default.
//
// Only ClickHouse reads it. There a table has no engine unless one is named,
// and whether an unnamed one is even legal is decided by the server's
// `default_table_engine` -- whose own default value is `None`
// (stokaro/ptah#2234).
func (m *Migrator) WithMigrationsEngine(engine string) *Migrator {
	tmp := *m
	tmp.migrationsEngine = strings.TrimSpace(engine)
	tmp.initialized = false
	tmp.initializedDryRun = false
	return &tmp
}

// migrationsTableCreateError names the engine and the flag that chose it when a
// target has one, because the server's own message does not.
//
// A ClickHouse engine the revision table cannot use is refused as
// `code: 36, message: Engine Log doesn't support ... ORDER_BY ...`, which says
// what is wrong and not where the engine came from. On a deployment that set it
// through PTAH_MIGRATIONS_ENGINE rather than on the command line, that is the
// difference between a one-line fix and a search (stokaro/ptah#2234).
func (m *Migrator) migrationsTableCreateError(err error) error {
	clause := strings.TrimSpace(m.revisionEngineClause())
	if clause == "" {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}
	return fmt.Errorf(
		"failed to create migrations table with %s (set --migrations-engine or PTAH_MIGRATIONS_ENGINE to choose another): %w",
		clause, err)
}

// revisionEngineClause is the storage-engine clause the revision table carries.
//
// MySQL and MariaDB have always named InnoDB here, because the server default
// is a deployment setting there too. ClickHouse now names one for the same
// reason and a stronger one: an unnamed engine is not merely a different table,
// it is a statement the server may refuse outright, and on a cluster it is a
// local MergeTree holding a migration history the other replicas do not have.
//
// MergeTree is the default rather than a Log engine because the revision table
// is read back and updated, and because it is what a server with the usual
// `default_table_engine = MergeTree` was already producing -- so an existing
// deployment sees the same table it had.
func (m *Migrator) revisionEngineClause() string {
	return revisionEngineClauseFor(m.connectionDialect(), m.migrationsEngine)
}

// revisionEngineRefusal answers a named engine the revision table cannot be,
// before any statement runs.
//
// Some targets cannot take one, for opposite reasons.
//
// On the MySQL family the server accepts the statement and Ptah then refuses
// the table: requireTransactionalMetadataEngine reads the engine back and
// insists on InnoDB, because the revision table is the witness a migration was
// applied and MyISAM has no transaction to roll a failed one back with. MySQL
// DDL commits, and the create is `CREATE TABLE IF NOT EXISTS`, so refusing
// after the fact leaves a table Ptah will not use and will not recreate --
// every later verb fails until an operator drops it by hand. Refusing first
// leaves the database as it was.
//
// On SQL Server and Oracle the statement has no engine clause at all: both DDL
// builders take a branch of their own for each, so a named engine is dropped in
// silence while revisionEngineClause still reports one -- and an unrelated
// create failure would then name a clause the server never saw.
//
// ClickHouse is deliberately not in this list. Which engines a revision table
// can be is the server's judgment there, it answers with its own message, and
// the failure creates nothing.
func revisionEngineRefusal(dialect, engine string) error {
	engine = strings.TrimSpace(engine)
	if engine == "" {
		return nil
	}
	switch {
	case implicitCommitDialect(dialect) && !strings.EqualFold(engine, "InnoDB"):
		return fmt.Errorf(
			"migrations engine %q is not usable for the revision table on %s: it must be InnoDB, "+
				"which records that a migration was applied even when the statement around it fails "+
				"(unset --migrations-engine or PTAH_MIGRATIONS_ENGINE for this target)",
			engine, dialect)
	case revisionTableHasNoEngineClause(dialect):
		return fmt.Errorf(
			"migrations engine %q cannot be named on %s: the revision table there has no engine clause "+
				"(unset --migrations-engine or PTAH_MIGRATIONS_ENGINE for this target)",
			engine, dialect)
	}
	return nil
}

// revisionTableHasNoEngineClause reports whether both revision-table DDL
// builders give this target a statement of its own with no engine clause in
// it.
//
// It is one predicate because the refusal above and the two builders have to
// agree: a target listed here but rendered through the generic statement would
// refuse an engine the table could carry, and a builder branch missing from
// here would drop a named engine in silence.
// TestRevisionTableHasNoEngineClause_AgreesWithBothBuilders holds the pair.
func revisionTableHasNoEngineClause(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLServer, platform.Oracle:
		return true
	default:
		return false
	}
}

// revisionEngineClauseFor is the same decision without a Migrator, for the
// Atlas-format DDL, which is built from a bare dialect.
func revisionEngineClauseFor(dialect, engine string) string {
	engine = strings.TrimSpace(engine)
	switch {
	case engine != "":
		return " ENGINE = " + engine
	case implicitCommitDialect(dialect):
		return " ENGINE=InnoDB"
	case platform.NormalizeDialect(dialect) == platform.ClickHouse:
		return " ENGINE = MergeTree"
	default:
		return ""
	}
}

// WithMigrationsTable returns a copy of the migrator that records applied
// migrations in the named schema and table. An empty table name falls back to
// the revision-table format's default name. An empty schema puts the table in
// the connection's default schema, with one exception: Atlas's own table, the
// Atlas layout under its default name, goes where Atlas keeps it, which on the
// PostgreSQL family through a URL that pins no search_path is the
// atlas_schema_revisions schema.
func (m *Migrator) WithMigrationsTable(schema, table string) *Migrator {
	tmp := *m
	tmp.migrationsSchema = strings.TrimSpace(schema)
	tmp.atlasPlacedSchema = false
	tmp.migrationsTable = strings.TrimSpace(table)
	if tmp.migrationsTable == "" {
		tmp.migrationsTable = tmp.defaultMigrationsTable()
	}
	tmp.placeAtlasRevisionTable()
	tmp.initialized = false
	tmp.initializedDryRun = false
	tmp.metadataAvailable = false
	tmp.legacyRevisionTable = false
	return &tmp
}

// WithRevisionTableFormat returns a copy of the migrator that uses the given
// database table layout for migration revisions; a default table name follows
// the format to its own default, and so does a default schema (see
// [Migrator.WithMigrationsTable]). Both layouts retain Ptah's dirty-state
// protection; the Atlas layout encodes rollback direction in its existing
// operator_version column.
func (m *Migrator) WithRevisionTableFormat(format RevisionTableFormat) *Migrator {
	tmp := *m
	tmp.revisionTableFormat = format
	if tmp.migrationsTable == "" || tmp.migrationsTable == defaultPtahMigrationsTable {
		tmp.migrationsTable = tmp.defaultMigrationsTable()
	}
	tmp.placeAtlasRevisionTable()
	tmp.initialized = false
	tmp.initializedDryRun = false
	tmp.metadataAvailable = false
	tmp.legacyRevisionTable = false
	return &tmp
}

// placeAtlasRevisionTable puts Atlas's own revision table where Atlas keeps it
// when the caller named no schema, and takes the placement back when the
// format or the table stops being Atlas's. The rule is
// [revisiontable.Schema], the one the Atlas-compatible verbs apply to their
// own flag: the atlas_schema_revisions schema on the PostgreSQL family through
// a URL that pins no search_path, and the connection's schema otherwise.
//
// Applying it here gives every native verb and every embedder the answer the
// Atlas-compatible verbs give. Without it a history written through the native
// binary sits in the connection's schema, where Atlas does not look, and each
// tool reports the other's database as never migrated.
//
// A table under another name is left in the connection's schema. Atlas reads
// only its own table name, so moving one it cannot read gains no reader and
// surprises the caller who named it.
func (m *Migrator) placeAtlasRevisionTable() {
	if m.atlasPlacedSchema {
		m.migrationsSchema = ""
		m.atlasPlacedSchema = false
	}
	if !m.revisionTableFormat.isAtlas() || m.migrationsSchema != "" ||
		m.migrationsTable != defaultAtlasRevisionsTable || m.conn == nil {
		return
	}
	placed := revisiontable.Schema("", m.conn.Info().URL)
	if placed == "" {
		return
	}
	m.migrationsSchema = placed
	m.atlasPlacedSchema = true
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
	if platform.NormalizeDialect(m.connectionDialect()) == platform.Oracle {
		// An Oracle schema is a user, and a migrator does not create accounts.
		// The generic statement below is not Oracle's CREATE SCHEMA: measured
		// on Oracle Free 23.26.3.0.0 it answers ORA-02420, missing schema
		// authorization clause. A configured schema that does not exist fails
		// on the CREATE TABLE that names it, with the server's own message.
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
	// The dialect is a parameter for the same reason atlasRevisionsTableDDL
	// takes one: it keeps every branch reachable from a zero-value Migrator, so
	// the guard tests can assert the statement each target is sent without a
	// live database of that engine.
	return ptahRevisionsTableDDL(
		m.connectionDialect(),
		m.qualifiedMigrationsTable(),
		sqlStringLiteral(m.sqlServerObjectName()),
		m.migrationsEngine,
	)
}

func ptahRevisionsTableDDL(dialect, qualifiedTable, sqlServerObjectLiteral, engine string) string {
	if platform.NormalizeDialect(dialect) == platform.SQLServer {
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
END`, sqlServerObjectLiteral, qualifiedTable)
	}
	if platform.NormalizeDialect(dialect) == platform.Oracle {
		// Oracle has neither BIGINT nor TEXT and takes DEFAULT only before NOT
		// NULL. Measured on Oracle Free 23.26.3.0.0, one column at a time:
		//
		//	version BIGINT                                ORA-00902: invalid datatype
		//	description TEXT                              ORA-00902: invalid datatype
		//	state VARCHAR(32) NOT NULL DEFAULT 'applied'  ORA-03076: unexpected item DEFAULT
		//
		// Every text column Ptah can write as an empty string is nullable here,
		// because Oracle stores '' as NULL: a NOT NULL description would refuse
		// a migration that has none. revisionText reads the NULL back as ''.
		// There is no engine clause, which revisionTableHasNoEngineClause
		// states (stokaro/ptah#3298).
		return oracleCreateTableIfAbsent(fmt.Sprintf(`CREATE TABLE %s (
    version NUMBER(19) PRIMARY KEY,
    description CLOB NULL,
    applied_at TIMESTAMP NOT NULL,
    state VARCHAR2(32) DEFAULT 'applied' NOT NULL,
    applied NUMBER(10) DEFAULT 1 NOT NULL,
    total NUMBER(10) DEFAULT 1 NOT NULL,
    error CLOB NULL,
    error_stmt CLOB NULL,
    execution_time_ms NUMBER(19) DEFAULT 0 NOT NULL,
    checksum VARCHAR2(64) NULL
)`, qualifiedTable))
	}
	engineClause := revisionEngineClauseFor(dialect, engine)
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at %s NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied INTEGER NOT NULL DEFAULT 1,
    total INTEGER NOT NULL DEFAULT 1,
    error TEXT NULL,
    error_stmt TEXT NULL,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL DEFAULT ''
)%s`, qualifiedTable, revisionTimestampType(dialect), engineClause)
}

// oracleCreateTableIfAbsent wraps a CREATE TABLE so that a table which already
// exists is left alone, which is what IF NOT EXISTS does on the other targets.
//
// Oracle 21 has no IF NOT EXISTS -- capability.Oracle21 turns
// object_existence_guards off -- so the guard is a PL/SQL block that runs the
// statement and ignores ORA-00955, "name is already used by an existing
// object". One spelling then serves both release lines. Measured on Oracle
// Free 23.26.3.0.0: the block creates the table, and running it a second time
// is accepted and changes nothing. The statement travels as a string literal,
// so its quotes are doubled.
func oracleCreateTableIfAbsent(createTable string) string {
	return `DECLARE
    already_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(already_exists, -955);
BEGIN
    EXECUTE IMMEDIATE '` + strings.ReplaceAll(createTable, "'", "''") + `';
EXCEPTION
    WHEN already_exists THEN NULL;
END;`
}

// revisionTimestampType is the type the revision table gives its timestamp
// column.
//
// TIMESTAMP everywhere it is understood, which is every engine Ptah supports
// except one. Spanner's PostgreSQL interface does not have the type at all:
//
//	applied_at TIMESTAMP    ERROR: Type <timestamp> is not supported. (SQLSTATE P0001)
//	applied_at TIMESTAMPTZ  accepted
//
// Measured against the Cloud Spanner emulator behind PGAdapter 0.55.2. The
// column is a point in time either way -- Spanner stores timestamps in UTC and
// has no local-time type to lose -- so this is a spelling the target has,
// not a different column.
//
// It is a function rather than a branch in each DDL because both revision table
// formats have the column and both were refused (stokaro/ptah#2233).
func revisionTimestampType(dialect string) string {
	if platform.NormalizeDialect(dialect) == platform.Spanner {
		return "TIMESTAMPTZ"
	}
	return "TIMESTAMP"
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
			atlasAppliedRevisionPredicateFor(m.connectionDialect()),
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

// Initialize prepares the revision metadata this migrator reads and writes. It
// creates the metadata schema and the migrations table when they are absent,
// refuses an existing table the configured layout cannot safely use, and
// brings a table that predates the current layout up to it — so Initialize can
// execute DDL, ALTER TABLE included, against live metadata. A refusal happens
// before any statement runs, leaving the metadata untouched.
//
// Every read and write entry point calls Initialize implicitly, GetRevisions
// included; a writer in dry-run mode, which only inspects the metadata, is the
// one read-only route. Initialize is idempotent, and a migrator that later
// leaves dry-run mode still gets its table created.
func (m *Migrator) Initialize(ctx context.Context) error {
	dryRun := m.conn.Writer().IsDryRun()

	// Before the memoized return: a malformed value must not stay dormant
	// because this invocation happened to be the second one.
	if err := m.validateMetadataInputs(); err != nil {
		return err
	}

	// Skip if already initialized. The memoized result is only valid for the
	// dry-run mode it was computed under: a real Initialize records that the
	// metadata now exists, while a dry-run Initialize only records what the
	// metadata looks like, so a writer that later leaves dry-run mode must
	// still get its table created.
	if m.initialized && m.initializedDryRun == dryRun {
		return nil
	}

	// Before any statement, including the dry run's inspection: a named engine
	// the revision table cannot be is a refusal, not a table.
	if err := revisionEngineRefusal(m.connectionDialect(), m.migrationsEngine); err != nil {
		return err
	}

	// Before the dry run too: a dry run reads the existing metadata table, and
	// a foreign one can attach a policy or a default expression that runs the
	// squatter's SQL during a SELECT. A refusal that only covered writes would
	// describe a protection the read path does not have.
	if err := m.refuseForeignMetadataTable(ctx, m.migrationsTableName()); err != nil {
		return err
	}

	// Before the dry run as well: a status or a plan that read the empty
	// placed table would report a migrated database as never migrated.
	if err := m.refuseStrandedAtlasHistory(ctx); err != nil {
		return err
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
		return m.migrationsTableCreateError(err)
	}
	// Again after the create, because the check above and this statement are
	// two round trips: a role that can create objects in the metadata schema
	// can place its table between them, and IF NOT EXISTS would adopt it
	// silently. What Ptah created it owns, so the second answer is the one
	// that holds.
	if err := m.refuseForeignMetadataTable(ctx, m.migrationsTableName()); err != nil {
		return err
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
	case platform.Oracle:
		return m.oracleMigrationsColumnExists(ctx, name)
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

// oracleMigrationsColumnExists asks ALL_TAB_COLUMNS, because Oracle has no
// information_schema. The revision table's columns are created unquoted, so
// the catalog holds their upper-case fold; the table name is quoted and kept as
// given.
func (m *Migrator) oracleMigrationsColumnExists(ctx context.Context, name string) (bool, error) {
	query := sqlutil.Rebind(platform.Oracle, `SELECT COUNT(*)
FROM all_tab_columns
WHERE owner = ? AND table_name = ? AND column_name = ?`)
	var count int
	if err := m.conn.QueryRowContext(
		ctx,
		query,
		configuredOrConnectionSchema(m.metadataTableSchemaName(), m.connectionSchemaName()),
		m.migrationsTableName(),
		strings.ToUpper(name),
	).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", name, err)
	}
	return count > 0, nil
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

// isPostgresFamily reports whether this connection takes the PostgreSQL
// spelling of the metadata queries below.
//
// It deliberately answers a narrower question than
// [platform.IsPostgresFamily], which counts Spanner in. The difference is not
// an oversight and must not be closed by delegating to the exported one: the
// only thing this predicate selects is a query written around
// `current_schema()`, and Spanner's PostgreSQL interface does not have that
// function anywhere a query can call it. Measured against the Cloud Spanner
// emulator behind PGAdapter 0.55.2:
//
//	SELECT current_schema()                          public
//	SELECT ... WHERE table_schema = current_schema()  ERROR: Postgres function
//	                                                 current_schema() is not supported
//
// So Spanner belongs in the generic branch, which asks for a schema by name and
// answers correctly there -- and does so because a Spanner connection carries
// its schema, which metadataInformationSchemaName returns. Widening this
// predicate would replace a working read with one the server refuses
// (stokaro/ptah#2233).
func (m *Migrator) isPostgresFamily() bool {
	return usesPostgresMetadataQueries(m.connectionDialect())
}

// usesPostgresMetadataQueries is the decision itself, taking the dialect so it
// can be measured without a live connection of each engine.
func usesPostgresMetadataQueries(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
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

// GetRevisions returns every migration metadata row, including dirty rows.
//
// It refuses, with a *RevisionSpellingError, an Atlas-format table whose rows
// spell a migration file's version another way, such as 1 for 001_init.sql.
// Every command reads the table through here, so each one refuses that
// history rather than answering from its own reading of it.
func (m *Migrator) GetRevisions(ctx context.Context) ([]MigrationRevision, error) {
	revisions, err := queryMigrationRows(
		ctx,
		m,
		(*Migrator).getRevisionsSQL,
		m.scanRevisionRow,
		"failed to query migration revisions",
		"failed to scan migration revision",
		"error iterating migration revisions",
	)
	if err != nil {
		return nil, err
	}
	if err := m.refuseRespelledRevisions(revisions); err != nil {
		return nil, err
	}
	return revisions, nil
}

func queryMigrationRows[T any](
	ctx context.Context,
	m *Migrator,
	query func(*Migrator) string,
	scan func(rowScanner) (T, error),
	queryErr,
	scanErr,
	iterErr string,
) ([]T, error) {
	if err := m.Initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if !m.metadataAvailable {
		return make([]T, 0), nil
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
	queryErr,
	scanErr,
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

// allDigitsToken reports whether value is one or more ASCII digits, the shape
// the numeric prefix of a repeatable revision token must have.
func allDigitsToken(value string) bool {
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return value != ""
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
	if prefix, ok := strings.CutSuffix(trimmed, "R"); ok && allDigitsToken(prefix) {
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

// getPreviousMigrationVersion finds the previous migration version compared to the current one.
// Returns an error and -1 if no previous migrations exist.
func (m *Migrator) getPreviousMigrationVersion(ctx context.Context) (int64, error) {
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

// GetMigrationStatus returns the current migration status derived from the
// provider's migrations and the revision metadata rows: current version,
// applied, pending, and out-of-order versions, and the dirty revision when one
// blocks new work. It is [Migrator.GetMigrationStatusSnapshot] without the raw
// revision rows.
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

	classified, err := m.classifyAppliedChecksums(providerMigrations, revisions)
	if err != nil {
		return MigrationStatusSnapshot{}, err
	}
	// The floor, not the bootstrap. A checkpoint bootstraps a database that has
	// applied nothing, so the bootstrap is nil the moment one has -- including
	// the moment right after the bootstrap ran. What a reader needs is the
	// checkpoint that COVERS the migrations below it, which checkpointFloor
	// answers on both sides of that moment, and which the pending selection is
	// already computed from (stokaro/ptah#3356).
	status := &MigrationStatus{
		ContractVersion:         StatusContractVersion,
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
		CheckpointVersion:       floor,
		Migrations: m.migrationRecords(
			providerMigrations,
			revisions,
			pendingMigrationList,
			outOfOrderMigrations,
			floor,
			dirtyRevision,
			classified.mismatchedKeys(),
		),
		MissingMigrations: missingMigrationRecords(classified.missing),
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
	return m.MigrateUpWithOptions(ctx, MigrateUpOptions{})
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

	reconcileChecksums, err := m.verifyBeforeApply(ctx, migrations)
	if err != nil {
		return err
	}

	migrationsToApply, err := m.selectUpMigrations(opts, migrations, appliedMigrations, appliedIdentities, currentVersion)
	if err != nil {
		return err
	}
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
	if err := m.admitUpPlan(ctx, opts, plan, migrationsToApply); err != nil {
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

// upSelection is what one selection pass decided: the migrations an up run
// applies, the pending migrations the execution order left behind, and the
// order that decided both.
type upSelection struct {
	apply     []*Migration
	skipped   []int64
	execOrder ExecOrder
}

// selectUpMigrations picks the migrations one up run applies: the pending set
// the execution order allows, narrowed by the target version and then by the
// amount, and refused outright where the bound cannot be honored. Narrowing
// and refusing stay beside the selection because each reads what the step
// before it produced, and the refusal is about what was selected rather than
// about the options.
func (m *Migrator) selectUpMigrations(
	opts MigrateUpOptions,
	migrations []*Migration,
	appliedMigrations []int64,
	appliedIdentities migrationIdentitySet,
	currentVersion int64,
) ([]*Migration, error) {
	selection, err := m.migrationsToApply(migrations, appliedMigrations, appliedIdentities, opts.TargetVersion)
	if err != nil {
		return nil, err
	}
	selection.apply = limitMigrationsToApply(selection.apply, opts.Amount)
	if err := refuseUnreachableTargetVersion(opts, currentVersion, selection); err != nil {
		return nil, err
	}
	return selection.apply, nil
}

// TargetVersionPassedError reports an up run bounded at a version the recorded
// history already sits above, with no pending migration left that reaches it.
type TargetVersionPassedError struct {
	// TargetVersion is the bound the caller asked the run to stop at.
	TargetVersion int64
	// CurrentVersion is the highest version the revision table records as
	// applied.
	CurrentVersion int64
}

// Error describes the target the run was given and the version the database
// already holds.
func (e *TargetVersionPassedError) Error() string {
	return fmt.Sprintf(
		"cannot migrate up to version %d: the database already records version %d",
		e.TargetVersion,
		e.CurrentVersion,
	)
}

// TargetVersionSkippedError reports an up run bounded at a version the
// execution order leaves pending. The migration is in the directory and
// unapplied, and [ExecOrderLinearSkip] passes over it for sorting below the
// recorded version.
type TargetVersionSkippedError struct {
	// TargetVersion is the bound the caller asked the run to stop at.
	TargetVersion int64
	// CurrentVersion is the highest version the revision table records as
	// applied.
	CurrentVersion int64
	// ExecOrder is the execution order that passed over the target.
	ExecOrder ExecOrder
}

// Error names the execution order that left the target pending, which is what
// a caller can act on: the same target is reachable under
// [ExecOrderNonLinear].
func (e *TargetVersionSkippedError) Error() string {
	return fmt.Sprintf(
		"cannot migrate up to version %d: execution order %s leaves it pending because it sorts below the recorded version %d",
		e.TargetVersion,
		e.ExecOrder,
		e.CurrentVersion,
	)
}

// refuseUnreachableTargetVersion reports a bounded run that cannot arrive
// where it was sent. Without it the run selects nothing and exits 0, so an
// operator executing an approved plan is told a version was reached that the
// database never reached.
//
// The reason is read out of the selection rather than out of the recorded
// version alone. Both refusals describe a run that selected nothing while the
// history sits above the bound, and telling an operator the history moved past
// a version their directory still holds as pending sends them to look at the
// database instead of at the execution order.
func refuseUnreachableTargetVersion(opts MigrateUpOptions, currentVersion int64, selection upSelection) error {
	if !opts.RefuseTargetVersionAlreadyPassed || opts.TargetVersion <= 0 {
		return nil
	}
	if len(selection.apply) > 0 || currentVersion <= opts.TargetVersion {
		return nil
	}
	if slices.Contains(selection.skipped, opts.TargetVersion) {
		return &TargetVersionSkippedError{
			TargetVersion:  opts.TargetVersion,
			CurrentVersion: currentVersion,
			ExecOrder:      selection.execOrder,
		}
	}
	return &TargetVersionPassedError{
		TargetVersion:  opts.TargetVersion,
		CurrentVersion: currentVersion,
	}
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

func mergeAppliedVersions(applied, assumed []int64) []int64 {
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

	targetVersion, err := m.getPreviousMigrationVersion(ctx)
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
	reconcileChecksums, err := m.verifyBeforeApply(ctx, migrations)
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

	var checksDeferred []int64
	for _, migration := range migrationsToRollback {
		deferred, err := m.rollbackMigration(ctx, migration, deleteSQL)
		if err != nil {
			return err
		}
		if deferred {
			checksDeferred = append(checksDeferred, migration.Version)
		}
	}
	m.reportDeferredDownChecks(checksDeferred)
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
	reconcileChecksums, err := m.verifyBeforeApply(ctx, migrations)
	if err != nil {
		return err
	}
	selection, err := m.migrationsToApply(migrations, appliedMigrations, appliedIdentities, targetVersion)
	if err != nil {
		return err
	}
	migrationsToApply := selection.apply
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

// admitUpPlan runs everything that may refuse a selected up plan before any
// of it executes, in this order: the observer sees the plan, the guard decides
// whether it may run at all, the transaction modes are validated, and the
// pre-migration hook runs last. The observer comes first so that it still
// records a plan the steps after it refuse.
func (m *Migrator) admitUpPlan(
	ctx context.Context,
	opts MigrateUpOptions,
	plan MigrationPlan,
	migrations []*Migration,
) error {
	notifyMigrationPlanObserver(ctx, opts.PlanObserver, plan)
	if err := runMigrationPlanGuard(ctx, opts.PlanGuard, plan); err != nil {
		return err
	}
	if err := m.validateUpTransactionMode(migrations); err != nil {
		return err
	}
	return runPreMigrationHook(ctx, opts.Preflight, plan)
}

func runMigrationPlanGuard(ctx context.Context, guard MigrationPlanGuard, plan MigrationPlan) error {
	if guard == nil {
		return nil
	}
	plan.Versions = slices.Clone(plan.Versions)
	plan.VersionKeys = slices.Clone(plan.VersionKeys)
	return guard(ctx, plan)
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
		for _, misplaced := range migrationfile.MisplacedDirectives(source, dialect) {
			if misplaced.Err != nil {
				// Reported as the run's refusal by the toolkit's tx-mode
				// parsing, which names the line too. Warning about it here as
				// well would print the same line twice on a run that is about
				// to abort.
				continue
			}
			m.logger.Warn(
				"Migration directive was not honored because of where it appears in the file",
				"version", migration.Version,
				"direction", string(direction),
				"file", migrationTxModeSourceName(sourcePath, migration.Description),
				"line", misplaced.Line,
				"directive", misplaced.Text,
				"remedy", misplaced.Remedy,
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
		m.logMigrationEvent(ctx, "up", migration, MigrationLogStarted, nil)
		deferred, err := m.applyUpMigrationObserved(ctx, migration, txMode, i == 0)
		if err != nil {
			m.logMigrationEvent(ctx, "up", migration, MigrationLogFailed, err)
			return nil, err
		}
		// Before the postconditions, because the entry says what the database
		// holds: the body ran and its revision is recorded, and a check that
		// does not hold afterwards does not take that back.
		m.logMigrationEvent(ctx, "up", migration, MigrationLogApplied, nil)
		// After the body and its revision, so a failure is a statement about a
		// migration that applied.
		postDeferred, err := m.runPostMigrationChecks(ctx, migration, MigrationDirectionUp)
		if err != nil {
			return nil, err
		}
		if deferred || postDeferred {
			checksDeferred = append(checksDeferred, migration.Version)
		}
	}

	return checksDeferred, nil
}

// upSQLTexts is what the batch about to run in one transaction will execute,
// which is what decides whether that transaction needs foreign-key enforcement
// suspended for a table rebuild.
func upSQLTexts(migrations []*Migration) []string {
	texts := make([]string, 0, len(migrations))
	for _, migration := range migrations {
		texts = append(texts, migration.UpSQL)
	}
	return texts
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

	// One batch is one outcome: every migration in it applies or none does, so
	// the log opens an entry for each before the transaction starts and settles
	// all of them together. Recording a migration applied when a later one rolls
	// the whole transaction back would describe a database that never existed.
	//
	// The starts are written here rather than as each migration is reached
	// because the log writes on the pool: once the batch transaction holds the
	// write lock, SQLite refuses a second writer with SQLITE_BUSY and every
	// entry after the first would be lost.
	started := slices.Clone(migrations)
	m.logBatchStart(ctx, started)

	tx, err := sqliterebuild.BeginTransactionForAnySQL(ctx, m.conn, upSQLTexts(migrations))
	if err != nil {
		failure := fmt.Errorf("failed to begin tx-mode all transaction: %w", err)
		m.logBatchOutcome(ctx, started, MigrationLogFailed, failure)
		return nil, failure
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
			recorded := m.recordRolledBackBatchFailure(ctx, migration, startedAt[key], err, plan)
			m.logBatchOutcome(ctx, started, MigrationLogFailed, recorded)
			return nil, recorded
		}
		if err := m.recordAppliedMigrationOn(ctx, txConn, migration, startedAt[key], plan); err != nil {
			_ = tx.Rollback()
			failure := fmt.Errorf(
				"failed to record migration %d in tx-mode all transaction: %w", migration.Version, err)
			m.logBatchOutcome(ctx, started, MigrationLogFailed, failure)
			return nil, failure
		}
	}
	if err := tx.Commit(); err != nil {
		failure := fmt.Errorf("failed to commit tx-mode all transaction: %w", err)
		m.logBatchOutcome(ctx, started, MigrationLogFailed, failure)
		return nil, failure
	}
	m.logBatchOutcome(ctx, started, MigrationLogApplied, nil)
	m.logger.Info("Applied migrations in one transaction", "count", len(migrations))
	return checksDeferred, nil
}

// logBatchStart opens a log entry for every migration the batch will attempt.
func (m *Migrator) logBatchStart(ctx context.Context, started []*Migration) {
	for _, migration := range started {
		m.logMigrationEvent(ctx, "up", migration, MigrationLogStarted, nil)
	}
}

// logBatchOutcome records the same outcome for every migration the batch
// started, because one transaction gives them one.
func (m *Migrator) logBatchOutcome(
	ctx context.Context,
	started []*Migration,
	state MigrationLogState,
	failure error,
) {
	for _, migration := range started {
		m.logMigrationEvent(ctx, "up", migration, state, failure)
	}
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
	resolvedTimeouts := make(map[*Migration]migrationfile.Timeouts, len(migrations))
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

	if m.txMode != MigrationTxModeAll {
		for _, migration := range migrations {
			if err := m.refuseTimeoutsTheTargetCannotCarry(migration, resolvedTimeouts[migration]); err != nil {
				return err
			}
		}
		return nil
	}
	if err := m.validateTxModeAllDialect(); err != nil {
		return err
	}
	for _, migration := range migrations {
		if _, err := m.resolveUpMigrationTxMode(migration); err != nil {
			return err
		}
		if !resolvedTimeouts[migration].IsZero() {
			return fmt.Errorf(
				"migration %d declares timeouts, which cannot run with tx-mode all: "+
					"%s", migration.Version, txModeAllExclusionAdvice)
		}
		if err := m.rejectChecksUnderTxModeAll(migration, MigrationDirectionUp); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) effectiveUpTimeouts(migration *Migration) (migrationfile.Timeouts, error) {
	timeouts, err := migration.upTimeoutsForDialect(m.connectionDialect())
	if err != nil {
		return migrationfile.Timeouts{}, fmt.Errorf(
			"migration %d has invalid timeout directives: %w",
			migration.Version,
			err,
		)
	}
	return mergeMigrationTimeouts(m.defaultTimeouts, timeouts), nil
}

// effectiveTimeouts resolves the timeouts that bound a migration's statements
// in one direction.
func (m *Migrator) effectiveTimeouts(migration *Migration, direction MigrationDirection) (migrationfile.Timeouts, error) {
	if direction == MigrationDirectionDown {
		return m.effectiveDownTimeouts(migration)
	}
	return m.effectiveUpTimeouts(migration)
}

func (m *Migrator) effectiveDownTimeouts(migration *Migration) (migrationfile.Timeouts, error) {
	timeouts, err := migration.downTimeoutsForDialect(m.connectionDialect())
	if err != nil {
		return migrationfile.Timeouts{}, fmt.Errorf(
			"migration %d has invalid timeout directives: %w",
			migration.Version,
			err,
		)
	}
	return mergeMigrationTimeouts(m.defaultTimeouts, timeouts), nil
}

// validateTxModeAllDialect refuses --tx-mode all where the target cannot roll
// a schema change back as a unit.
//
// The decision is capability.TransactionalDDL rather than a second list of
// dialect names beside the one timeouts used. MySQL, MariaDB and ClickHouse
// commit DDL implicitly, so a failed migration leaves whatever ran before it --
// that is the engine, and the message says so rather than reporting an
// unexplained "not supported" (stokaro/ptah#1713).
func (m *Migrator) validateTxModeAllDialect() error {
	if m.conn.Info().Capabilities.Has(capability.TransactionalDDL) {
		return nil
	}
	return fmt.Errorf(
		"tx-mode all is not supported for dialect %q: this target commits schema changes as they run, "+
			"so a failed migration cannot be rolled back as a unit",
		m.conn.Info().Dialect)
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
// check was fixed. (It would not wedge outright — that flag reuses the dirty
// row instead of failing on a re-insert — but a gate that
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
func (m *Migrator) runPreMigrationChecks( //revive:disable-line:flag-parameter see above: a field would leak across derived migrators
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
	if err := m.runMigrationChecks(ctx, m.conn, migration, MigrationDirectionUp, CheckPhaseBefore); err != nil {
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
	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, timeouts, timeoutScopeTransaction)
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
	tx, err := sqliterebuild.BeginTransactionForSQL(ctx, m.conn, migration.UpSQL)
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

	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, timeouts, timeoutScopeTransaction)
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
	if rollbackErr != nil && !cancellationEndedTheTransaction(failure) {
		return fmt.Errorf("%w; additionally failed to roll back migration transaction: %v", failure, rollbackErr)
	}
	return &migrationTransactionRolledBackError{version: version, cause: failure}
}

// cancellationEndedTheTransaction reports whether a failed rollback still
// leaves the migration transaction rolled back.
//
// Every path that reaches [migrationFailureAfterRollback] is a failure undoing
// what it started, and none of them issues a COMMIT. So once the failure is the
// cancellation, the transaction ends rolled back however the rollback answered:
// database/sql rolls back a transaction whose context is canceled and reports
// sql.ErrTxDone on the attempt that follows, and where the cancellation tore
// the connection down first, the server rolls back what that connection held.
// Both answers come from one interrupted `ptah-compat migrate apply`: `sql:
// transaction has already been committed or rolled back` on one machine and
// `conn closed` on another, for the same interrupt at the same statement.
//
// Reading either as an unknown outcome records a dirty revision for a migration
// that changed nothing, and under the Atlas revision format that row survives
// the discard an ordinary rolled-back failure gets, so the next run refuses to
// continue past it.
//
// The cancellation is what makes this safe, and it is the whole condition. A
// rollback error on an ordinary failure says the transaction was committed OR
// rolled back, and a commit is the outcome nobody may guess at. What a
// dialect commits implicitly is answered before this, by the progress the
// revision recorded: a discard requires that no statement reported any.
func cancellationEndedTheTransaction(failure error) bool {
	return errors.Is(failure, context.Canceled)
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
	timeouts, err := m.effectiveUpTimeouts(migration)
	if err != nil {
		return err
	}
	return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
		restoreTimeouts, err := scoped.applySessionTimeouts(ctx, migration, timeouts)
		if err != nil {
			return err
		}
		if err := scoped.restoreNoTransactionSessionPrefix(
			ctx,
			migration,
			MigrationDirectionUp,
			plan.resumeFrom,
		); err != nil {
			failure := fmt.Errorf("failed to restore session state for migration %d: %w", migration.Version, err)
			return scoped.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, failure)
		}
		if err := scoped.recordPendingMigrationRevisionOn(ctx, scoped.conn, migration, startedAt, plan); err != nil {
			failure := fmt.Errorf("failed to record pending migration %d: %w", migration.Version, err)
			return scoped.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, failure)
		}
		if plan.resumeFrom > 1 {
			scoped.logger.Info(
				"Resuming migration after a partially applied attempt",
				"version", migration.Version,
				"resumeFromStatement", plan.resumeFrom,
			)
		}
		executionCtx := withMigrationResume(ctx, plan.resumeFrom)
		return scoped.applyUpMigrationNoTransactionOnSession(executionCtx, migration, startedAt, restoreTimeouts)
	})
}

func (m *Migrator) applyUpMigrationNoTransactionOnSession(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	restoreTimeouts restoreTimeoutsFunc,
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
		failure := m.failMigrationWithDirtyStateWithMode(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to apply migration %d", migration.Version),
			MigrationTxModeNone,
			MigrationDirectionUp,
		)
		return m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, failure)
	}
	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		return m.failMigrationWithDirtyStateWithMode(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			"",
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

// rollbackMigration rolls one migration back and reports whether its
// postconditions were deferred rather than evaluated.
func (m *Migrator) rollbackMigration(
	ctx context.Context,
	migration *Migration,
	deleteSQL string,
) (bool, error) {
	// The rollback is the reason this log exists: it deletes the revision row,
	// so without a record here a database that was on this version yesterday
	// reads exactly like one that never reached it.
	m.logMigrationEvent(ctx, "down", migration, MigrationLogStarted, nil)
	if err := m.rollbackMigrationObserved(ctx, migration, deleteSQL); err != nil {
		m.logMigrationEvent(ctx, "down", migration, MigrationLogFailed, err)
		return false, err
	}
	m.logMigrationEvent(ctx, "down", migration, MigrationLogRolledBack, nil)
	// Outside the observed span on purpose: the rollback ran and is recorded,
	// and a postcondition that does not hold must not turn that into a
	// rollback the metrics report as failed.
	return m.runPostMigrationChecks(ctx, migration, MigrationDirectionDown)
}

// reportDeferredDownChecks says which rollbacks' postconditions a dry run
// validated but did not evaluate.
//
// The down path has no options struct to hand an observer, so the report goes
// to the run's logger rather than to a caller. What must not happen is
// silence: a preview that answered fewer questions than it was asked, and said
// so on the up path only, tells an operator the rollback was previewed in full
// (stokaro/ptah#3405).
func (m *Migrator) reportDeferredDownChecks(versions []int64) {
	if len(versions) == 0 {
		return
	}
	m.logger.Warn(
		"Deferred post-migration checks: a dry run does not produce the state they assert on",
		"versions", versions,
	)
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
	// A check in the down body runs before the rollback, for the same reason
	// one in the up body runs before the migration: it is a precondition, and
	// the statement it guards is about to run. Until this existed the directive
	// was parsed by nothing and the rollback simply proceeded
	// (stokaro/ptah#1715).
	if err := m.runMigrationChecks(ctx, m.conn, migration, MigrationDirectionDown, CheckPhaseBefore); err != nil {
		return fmt.Errorf("pre-migration check failed for migration %d: %w", migration.Version, err)
	}
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
	tx, err := sqliterebuild.BeginTransactionForSQL(ctx, m.conn, migration.DownSQL)
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

	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, timeouts, timeoutScopeTransaction)
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
	timeouts, err := m.effectiveDownTimeouts(migration)
	if err != nil {
		return err
	}
	return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
		restoreTimeouts, err := scoped.applySessionTimeouts(ctx, migration, timeouts)
		if err != nil {
			return err
		}
		if err := scoped.beginRollbackRevision(ctx, migration, startedAt); err != nil {
			failure := fmt.Errorf("failed to record pending rollback %d: %w", migration.Version, err)
			return scoped.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, failure)
		}
		return scoped.rollbackMigrationNoTransactionOnSession(ctx, migration, startedAt, deleteSQL, restoreTimeouts)
	})
}

func (m *Migrator) rollbackMigrationNoTransactionOnSession(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	deleteSQL string,
	restoreTimeouts restoreTimeoutsFunc,
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
		failure := m.failRollbackWithDirtyStateWithMode(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to revert migration %d", migration.Version),
			MigrationTxModeNone,
		)
		return m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, failure)
	}
	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		return m.failRollbackWithDirtyStateWithMode(ctx, migration, startedAt, err, migration.DownSQL, "", MigrationTxModeNone)
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
	sqlText,
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
	sqlText,
	prefix string,
) error {
	return m.failRollbackWithDirtyStateWithMode(ctx, migration, startedAt, failure, sqlText, prefix, MigrationTxModeFile)
}

func (m *Migrator) failRollbackWithDirtyStateWithMode(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	sqlText,
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
	sqlText,
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

// migrationsToApply decides which pending migrations an up run may apply under
// the execution order in force, up to targetVersion.
//
// It reports what it left behind as well as what it chose. A caller that has to
// explain why a version was not applied cannot tell an out-of-order migration
// the order declined from one the recorded history covers, and deriving the
// skipped set a second time from the pending list is how one verdict becomes
// two that disagree.
func (m *Migrator) migrationsToApply(
	migrations []*Migration,
	applied []int64,
	appliedIdentities migrationIdentitySet,
	targetVersion int64,
) (upSelection, error) {
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
		err := newOutOfOrderSourceError(currentVersion, outOfOrderVersions, m.sourceVersions)
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
		return upSelection{}, err
	}

	selection := upSelection{
		apply:     make([]*Migration, 0, len(pendingMigrationList)),
		execOrder: execOrder,
	}
	for _, migration := range pendingMigrationList {
		// linear-skip leaves unapplied exactly what linear refuses, so it reads
		// the verdict computed above instead of re-deriving it. Re-deriving is
		// what let a converted directory skip nothing under linear-skip while
		// linear refused, and execute a migration the source tool leaves
		// pending.
		if execOrder == ExecOrderLinearSkip && slices.Contains(outOfOrderVersions, migration.Version) {
			m.logger.Warn("Skipping out-of-order migration", "version", migration.Version, "currentVersion", currentVersion)
			selection.skipped = append(selection.skipped, migration.Version)
			continue
		}
		selection.apply = append(selection.apply, migration)
	}

	return selection, nil
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
func checkpointRunnable(migration, bootstrap *Migration, floor int64) bool {
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
	floor,
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
		if err := m.refuseTimeoutsTheTargetCannotCarry(migration, timeouts); err != nil {
			return err
		}
		if migration.downUnavailable {
			return &AtlasDownNotImplementedError{
				Version:     migration.Version,
				Description: migration.Description,
			}
		}
		if _, err := m.resolveDownMigrationTxMode(migration); err != nil {
			return err
		}
	}
	return nil
}
