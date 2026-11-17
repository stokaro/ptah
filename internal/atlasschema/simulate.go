package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/devlock"
	"ptah.run/internal/devseed"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/providerurl"
	"ptah.run/internal/readscope"
	"ptah.run/internal/rolepassword"
	"ptah.run/internal/schemaselection"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/valuecontext"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// SimulationError reports that the generated apply plan failed to rehearse on
// the --dev-url dev database. The caller must refuse the target apply; the
// target database has not been modified.
type SimulationError struct {
	// Stage is the simulation phase that failed: "reset" (cleaning the dev
	// database), "baseline" (recreating the target's current schema), or
	// "plan" (executing the ordered plan statements).
	Stage string
	Err   error
}

func (e *SimulationError) Error() string {
	// The claim is narrow on purpose: this command did not apply the plan to
	// the target. What the rehearsed SQL did on the dev database — or through
	// it — is not something this message can speak for.
	return fmt.Sprintf(
		"dev database simulation failed during %s: %v; the plan was not applied to the target database",
		e.Stage, e.Err,
	)
}

func (e *SimulationError) Unwrap() error {
	return e.Err
}

// IsSimulationFailure reports whether err wraps a dev database simulation
// failure.
func IsSimulationFailure(err error) bool {
	var target *SimulationError
	return errors.As(err, &target)
}

// SimulateOptions configures the pre-apply dev database simulation.
type SimulateOptions struct {
	// DevURL is the dev database the plan is rehearsed on. Empty skips the
	// simulation.
	DevURL string
	// TargetURL guards against pointing the simulation at the target itself:
	// the dev database is reset destructively before the plan is rehearsed.
	TargetURL string
	// DesiredURLs are the raw --to values; a database-URL desired state must
	// not double as the dev database, because the reset would destroy the
	// desired-state source right after it was introspected.
	DesiredURLs []string
	// Statements overrides the rehearsed statements; empty rehearses the
	// prepared plan. `schema apply --edit` passes the edited statements so the
	// simulation covers exactly what would be applied. A plan that holds a
	// native operation or an approved input refuses an override.
	Statements []string
	// DevServerDisposable is the operator's declaration that the server
	// DevURL names is the run's own, as
	// [ptah.run/internal/devdocker.DisposableServerDeclared] resolved it. The
	// rehearsal then takes the realm a migration replay takes there; see
	// [ptah.run/internal/devclean.DevReplayRealm].
	DevServerDisposable bool
}

// SimulateOnDev rehearses the exact ordered apply plan on the --dev-url dev
// database before the target is touched. The dev database is reset
// deterministically, the target's introspected current schema is recreated on
// it, and the planned statements then execute in order under the same
// transaction mode as the target apply. Any failure surfaces as a
// *SimulationError and the caller must refuse the target apply; the target
// database has not been modified. Empty DevURL and empty plans are no-ops.
//
// A plan that holds a native operation, or a unit that reads an approved
// input, executes through its owner, and a native operation is bound to the
// database it was planned for. Such a plan is made again for the dev
// database, from the same desired state, after the current schema is
// recreated there; it must have the target plan's operations, in order, and
// it executes through the same executor the apply uses. See
// [nativeRehearsal].
func (p ApplyRuntimePlan) SimulateOnDev(ctx context.Context, opts SimulateOptions) error {
	if err := schemaext.RequireRuntime(ctx, p.runtime); err != nil {
		return err
	}
	// The dev database and the target are read as the plan read the target;
	// see [readscope.Qualified].
	ctx = readscope.Qualified(rolepassword.With(ctx, p.passwords))
	if strings.TrimSpace(opts.DevURL) == "" {
		return nil
	}
	if p.hasNativeExecution() {
		return p.simulateNativeOnDev(ctx, opts)
	}
	original, err := p.Statements()
	if err != nil {
		return err
	}
	statements := opts.Statements
	if len(statements) == 0 {
		statements = original
	}
	if len(statements) == 0 {
		return nil
	}
	if !slices.Equal(statements, original) && hasOwnerTransactions(p.plan.transactions) {
		return errors.New("edited SQL with owner transaction requirements needs a new owner assessment")
	}
	if err := checkOwnerTransactions(p.plan.transactions, p.txMode, p.conn); err != nil {
		return err
	}
	if p.conn == nil {
		return errors.New("schema apply simulation requires database connection")
	}

	dev, err := connectSimulationDev(ctx, opts.DevURL, opts.DevServerDisposable, p.conn, opts.TargetURL, opts.DesiredURLs)
	if err != nil {
		return err
	}
	// Registered before the close so it runs after it: a provisioned container
	// is removed only once the connection to it is gone.
	defer dev.release()
	defer dbschema.CloseAndWarn(dev.conn)
	// Registered after the close, so it runs before it: the dev database is
	// handed back with nothing the rehearsal put in it, whether the rehearsal
	// succeeded or failed.
	defer discardDevRehearsalArtifacts(ctx, dev.conn, dev.baseline)
	if err := checkDevNaming(p.rehearsal.planning.naming, p.conn.Info(), dev.conn.Info()); err != nil {
		return err
	}

	return rehearseStatementsOnDev(ctx, p.conn, dev.conn, dev.baseline, p.current, p.seed.target(ctx, p.conn), p.txMode, statements, p.runtime)
}

// discardDevRehearsalArtifacts drops what the rehearsal created in the dev
// database, on every exit path.
//
// A dev database is scratch space the tool is trusted to borrow. The pinned
// community binary v1.3.0 hands it back empty, measured 2026-08-07 against a
// live MySQL 9.7 with a freshly created target/dev pair each time: after a
// successful `schema apply` the dev database held no tables, and after a run
// that failed inside the dev database (Error 3780, incompatible foreign key
// column types) it held no tables either. Ptah matches that on both paths.
//
// Failure to clean is reported rather than returned: it must not replace the
// rehearsal's own verdict, which is what the caller acts on.
func discardDevRehearsalArtifacts(ctx context.Context, devConn *dbschema.DatabaseConnection, baseline devclean.Baseline) {
	if devConn == nil {
		return
	}
	// A canceled command still returns the dev database empty instead of
	// leaving the rehearsal behind; see [devclean.CleanupContext].
	cleanupCtx, release := devclean.CleanupContext(ctx, devclean.CleanupGrace)
	defer release()
	devConn.SchemaWriter().SetDryRun(false)
	if err := devclean.Reset(cleanupCtx, devConn, baseline); err != nil {
		slog.Warn("failed to clean the dev database after the rehearsal; it may still hold rehearsed objects",
			"error", err)
	}
}

// simulationDev is a dev database a rehearsal has claimed: the connection,
// the baseline every reset of it hands to [devclean.Reset], and the release of
// a container the connection may run in.
type simulationDev struct {
	conn     *dbschema.DatabaseConnection
	baseline devclean.Baseline
	release  func()
}

// connectSimulationDev validates the dev database URL against the target and
// opens the dev connection used to rehearse a plan.
//
// devURL is the operator's spelling, not a normalized copy: whether a value is
// a `docker://` URL at all is decided from those bytes, and normalizing first
// promotes a value the pinned binary cannot parse into a started container.
// See [devdocker.Parse]. The callers have already answered an empty one.
// disposable is the operator's declaration that the server it names is the
// run's own, which [devdocker.Resolve] records for the realm decision.
//
// The caller owns the returned [simulationDev]: its connection must be closed,
// and its release called to remove a container this call may have started.
// On an error return nothing is left open or running.
func connectSimulationDev(
	ctx context.Context,
	devURL string,
	disposable bool,
	targetConn *dbschema.DatabaseConnection,
	targetURL string,
	desiredURLs []string,
) (simulationDev, error) {
	targetInfo := targetConn.Info()
	// The dialect check reads the URL as written: a `docker://` value names its
	// engine in the text, so a mismatch is answerable before a container is
	// started and a refused run pays for none.
	if err := providerurl.ValidateDialectMatch(ctx, devURL, targetInfo.Dialect); err != nil {
		return simulationDev{}, err
	}
	// The alias checks below ask whether the dev database IS the target or the
	// desired-state database, because the dev database is reset destructively.
	// A container that does not exist yet cannot be either, and asking the
	// question of a `docker://` URL does not merely waste work -- the URL
	// comparison has no dialect to compare and answers `unsupported database
	// URL dialect`, which would refuse every docker dev database on this verb
	// with a sentence about a dialect the operator did not choose.
	// A dev server names no database, so its URL may address every database
	// of any server and a comparison of URLs would refuse each one. It is
	// compared by the server it reaches instead, once connected and before
	// anything is reset; see [claimSimulationDevServer].
	//
	// A dev URL whose provider creates dev realms is compared live for the
	// same reason: its dev database is a realm Resolve creates, so the URL
	// that names the target names a realm beside it, and only the connection
	// can say which.
	serverDev := isDevServer(devURL)
	if err := refuseDevURLAliases(ctx, devURL, targetURL, desiredURLs); err != nil {
		return simulationDev{}, err
	}
	protected := []devlock.Protected{{Conn: targetConn, Refusal: errDevURLIsTarget}}
	for _, desired := range aliasCandidates(devURL, desiredURLs) {
		if isDirectDatabaseURL(desired) {
			protected = append(protected, devlock.Protected{URL: desired, Refusal: devURLIsDesiredError(desired)})
		}
	}

	resolved, release, err := devdocker.Resolve(ctx, devURL, devdocker.Options{DeclaredDisposable: disposable})
	if err != nil {
		return simulationDev{}, err
	}

	// A dev URL naming no MySQL-family database is a whole dev server, reached
	// and claimed as one; see [claimSimulationDevServer]. The operator's
	// spelling decides it, as it does for every verb that takes a dev server.
	connect := dbschema.ConnectToDatabase
	if serverDev {
		connect = dbschema.ConnectToServer
	}
	devConn, err := connect(ctx, strings.TrimSpace(resolved))
	if err != nil {
		release()
		return simulationDev{}, fmt.Errorf("connect to --dev-url: %w", err)
	}

	devInfo := devConn.Info()
	if platform.NormalizeDialect(devInfo.Dialect) != platform.NormalizeDialect(targetInfo.Dialect) {
		dbschema.CloseAndWarn(devConn)
		release()
		return simulationDev{}, fmt.Errorf("--dev-url dialect %q does not match --url dialect %q", devInfo.Dialect, targetInfo.Dialect)
	}
	if err := checkSimulationSchemaScope(devInfo, targetInfo); err != nil {
		dbschema.CloseAndWarn(devConn)
		release()
		return simulationDev{}, err
	}
	if serverDev {
		dev, err := claimSimulationDevServer(ctx, devConn, strings.TrimSpace(resolved), targetInfo, protected)
		if err != nil {
			dbschema.CloseAndWarn(devConn)
			release()
			return simulationDev{}, err
		}
		releaseDev := dev.release
		dev.release = func() {
			releaseDev()
			release()
		}
		return dev, nil
	}
	// The URL comparisons above cannot see every alias: a connection pooler
	// can serve the target under another database name. Each server is asked
	// which database the session selected, and the dev connection is handed
	// to the caller only when that answer differs. The caller registers the
	// cleanup of the rehearsal after this returns, so a refusal here leaves
	// both databases as they were (stokaro/ptah#3769).
	if err := devlock.EnsureDistinct(ctx, devConn, protected...); err != nil {
		dbschema.CloseAndWarn(devConn)
		release()
		return simulationDev{}, err
	}
	// A dev database that holds tables, or other objects the reset drops, was
	// not left by a rehearsal: each one hands back what it claimed, over the
	// scope it claimed. The reset before the rehearsal and the cleanup after it
	// would drop them, so they are refused here, for the same reason and at the same point as the
	// identity check above. The claim records what the resets keep and which
	// scope they empty; see [devclean.Reset].
	baseline, err := devclean.Claim(ctx, devConn)
	if err != nil {
		dbschema.CloseAndWarn(devConn)
		release()
		return simulationDev{}, err
	}
	return simulationDev{conn: devConn, baseline: baseline, release: release}, nil
}

// refuseDevURLAliases refuses a dev URL whose text names the target or a
// desired-state database; see [connectSimulationDev] for the URLs it does not
// read. Every pair it skips is compared live once the dev database is
// connected.
func refuseDevURLAliases(ctx context.Context, devURL, targetURL string, desiredURLs []string) error {
	// A provider owns the grammar of its addresses, so two of them are
	// compared by the databases their sessions select, once connected.
	if _, served := providerurl.Provided(ctx, devURL); served || devdocker.ResolvedPerRun(ctx, devURL) {
		return nil
	}
	for _, target := range urlAliasCandidates(devURL, []string{targetURL}) {
		sameTarget, err := atlasurl.MayAddressSameDatabaseBeforeLiveCheck(devURL, target)
		if err != nil {
			return fmt.Errorf("compare --dev-url with target database: %w", err)
		}
		if sameTarget {
			return errDevURLIsTarget
		}
	}
	if isDevServer(devURL) {
		return nil
	}
	for _, desired := range aliasCandidates(devURL, desiredURLs) {
		sameDesired, err := sameDirectDatabaseURL(devURL, desired)
		if err != nil {
			return fmt.Errorf("compare --dev-url with --to desired-state database %q: %w", desired, err)
		}
		if sameDesired {
			return devURLIsDesiredError(desired)
		}
	}
	return nil
}

// errDevURLIsTarget is the refusal of a dev URL that names the target, by its
// URL or by what its server answers.
var errDevURLIsTarget = errors.New("--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it")

// errReplayDevURLIsTarget refuses a dev database that is the target when a
// --to migration directory is about to be replayed on it.
var errReplayDevURLIsTarget = errors.New("--dev-url must not point at the target database: the dev database is reset destructively before the migration directory is replayed on it")

// devURLIsDesiredError is the refusal of a dev URL that names a desired-state
// database, by its URL or by what its server answers.
func devURLIsDesiredError(desired string) error {
	return fmt.Errorf("--dev-url must not point at the --to desired-state database %q: the dev database is reset destructively before the plan is rehearsed on it", desired)
}

// aliasCandidates returns the URLs worth asking whether devURL already names
// them, dropping the empty ones.
//
// It returns nothing at all for a `docker://` dev URL. A container that does not
// exist yet cannot be a database the operator already named, and asking anyway
// does more than waste work: [atlasurl.MayAddressSameDatabase] has no dialect to
// compare for a docker URL and answers `unsupported database URL dialect`, which
// would refuse every docker dev database on this verb with a sentence about a
// dialect the operator never chose.
func aliasCandidates(devURL string, candidates []string) []string {
	if devdocker.IsURL(devURL) {
		return nil
	}
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate) != "" {
			out = append(out, candidate)
		}
	}
	return out
}

// urlAliasCandidates is [aliasCandidates] for the comparison of URLs, which a
// dev server skips: see [connectSimulationDev].
func urlAliasCandidates(devURL string, candidates []string) []string {
	if isDevServer(devURL) {
		return nil
	}
	return aliasCandidates(devURL, candidates)
}

// sameDirectDatabaseURL compares candidate only when its scheme names a
// directly connectable database. Desired-state files and migration directories
// share the DesiredURLs collection and are intentionally not database aliases.
func sameDirectDatabaseURL(databaseURL, candidate string) (bool, error) {
	if !isDirectDatabaseURL(candidate) {
		return false, nil
	}
	return atlasurl.MayAddressSameDatabaseBeforeLiveCheck(databaseURL, candidate)
}

// isDirectDatabaseURL reports whether a desired-state value is a database URL
// rather than a file or a migration directory.
func isDirectDatabaseURL(candidate string) bool {
	scheme, _, found := strings.Cut(strings.TrimSpace(candidate), ":")
	return found && platform.NormalizeDialect(scheme) != ""
}

// rehearseStatementsOnDev resets the dev database, recreates the target's
// introspected current schema on it, and executes the ordered statements
// under txMode — the shared rehearsal core of the pre-apply simulation and
// the plan-file desired-state verification.
//
// Every caller reaches the dev database through here, so this is where the
// escape lint runs: a dev database executes the statements for real, whether
// they came from a plan file or from a freshly computed apply.
//
// It is also where the statements are re-scoped onto the dev database. The plan
// is rendered with the target's schema name baked into every statement, and on
// MySQL, MariaDB, and ClickHouse a schema is a database, so running the plan
// verbatim on the dev connection lands it in the target — the simulation
// mutates the very database it exists to protect. See
// [rescopeStatementsForDevDatabase] and stokaro/ptah#1240.
func rehearseStatementsOnDev(
	ctx context.Context,
	targetConn,
	devConn *dbschema.DatabaseConnection,
	baseline devclean.Baseline,
	current *catalog.Database,
	outside devseed.Target,
	txMode migrator.MigrationTxMode,
	statements []string,
	runtime engine.SchemaRuntime,
) error {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return err
	}
	if targetConn == nil {
		return errors.New("schema apply simulation requires target database connection")
	}
	// A plan for a whole server names each table by its database and runs as
	// written on a whole dev server. A plan for one database is re-scoped onto
	// the dev database. Either way the guard reads what the dev server will
	// execute, and keeps it to what the cleanup of that server removes.
	if !devConn.Info().WholeServer {
		var err error
		statements, err = rescopeStatementsForDevDatabase(
			statements, devConn.Info().Dialect, targetConn.Info().Schema, devConn.Info().Schema,
		)
		if err != nil {
			return err
		}
	}
	// The lint reads exactly what the dev database will execute, so it runs on
	// the re-scoped statements rather than on the plan they came from.
	if err := checkPlanStatements(statements, devConn.Info().Dialect); err != nil {
		return err
	}
	guard := devclean.NewDevReplayGuardFor(devConn).WithSchemaStateReturned(baseline.ReturnsSchemaState())
	if err := guardPlanRehearsal(statements, guard, devConn.Info()); err != nil {
		return err
	}
	// The dev database executes these statements for real, and they came from
	// outside the operator's project, so the session carries every engine-level
	// restriction its dialect supports. Taking the session through
	// WithUntrustedSQLSession is what makes an unrestricted rehearsal
	// impossible to write; the lint above is only a lint.
	return devConn.WithUntrustedSQLSession(ctx, func(session *dbschema.DatabaseConnection) error {
		seed := rehearsalOutside{target: outside, executed: statements}
		return rehearseOnPreparedDev(ctx, session, baseline, current, seed, targetConn.Info(), txMode, statements, runtime)
	})
}

// checkPlanStatements is the escape lint as used by the rehearsal core. It is
// a variable so a test can neutralize the lint and prove that the engine-level
// restrictions — not the lint — are what stop an escape.
var checkPlanStatements = CheckPlanStatementsSandboxable

// guardPlanRehearsal is [guardRehearsedPlan] as used by the rehearsal core,
// a variable for the reason [checkPlanStatements] is one: the replay guard
// refuses an ATTACH too, and a test that proves the engine restriction holds
// has to take both screens away.
var guardPlanRehearsal = guardRehearsedPlan

func rehearseOnPreparedDev(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	baseline devclean.Baseline,
	current *catalog.Database,
	outside rehearsalOutside,
	target catalog.ServerInfo,
	txMode migrator.MigrationTxMode,
	statements []string,
	runtime engine.SchemaRuntime,
) error {
	devConn.SchemaWriter().SetDryRun(false)
	if err := devclean.Reset(ctx, devConn, baseline); err != nil {
		return &SimulationError{Stage: "reset", Err: err}
	}
	// After the reset, which empties the schema the dev session resolves names
	// in: once moved, that is the target's, which the dev database may not
	// even hold yet.
	devConn, reads, err := resolveNamesAsTarget(ctx, devConn, target)
	if err != nil {
		return &SimulationError{Stage: "baseline", Err: err}
	}
	added, err := recreateCurrentSchema(ctx, devConn, current, target, baseline, runtime, outside, reads)
	if err != nil {
		return &SimulationError{Stage: "baseline", Err: err}
	}
	if err := applyStatements(ctx, devConn, txMode, statements); err != nil {
		return &SimulationError{Stage: "plan", Err: devseed.WithAdded(err, added)}
	}
	return nil
}

// checkSimulationSchemaScope fails explicitly when the dev database's schema
// scope differs from the target's. Only dialects whose connection schema names
// a namespace inside the database participate: where it names the database
// itself the two legitimately differ, which is exactly why the plan has to be
// re-scoped before it is rehearsed.
//
// Two realm-scoped connections whose current schemas differ are not refused:
// each URL claims every schema of its database, so the rehearsal can resolve
// names on the dev session as the target does; see [resolveNamesAsTarget]. A
// URL that pins a schema claims that schema alone, and the plan's unqualified
// names would land in another one, so that pair is refused.
func checkSimulationSchemaScope(devInfo, targetInfo catalog.ServerInfo) error {
	if schemaScopeNamesDatabase(targetInfo.Dialect) {
		return nil
	}
	if devInfo.Schema == targetInfo.Schema || realmPairResolvesApart(devInfo, targetInfo) {
		return nil
	}
	return fmt.Errorf(
		"--dev-url schema scope %q does not match --url schema scope %q; point --dev-url at a dev database using the same schema",
		devInfo.Schema, targetInfo.Schema,
	)
}

// checkNativeSimulationSchemaScope refuses a realm-scoped pair whose current
// schemas differ for a plan that holds native operations or approved inputs.
// [checkSimulationSchemaScope] lets that pair through because a rehearsal of
// SQL text moves the dev session's name resolution to the target's; see
// [resolveNamesAsTarget]. The native rehearsal makes the plan again on the dev
// database and does not move it, so the dev session would build the target's
// state under its own current schema and run the plan against that.
func checkNativeSimulationSchemaScope(devInfo, targetInfo catalog.ServerInfo) error {
	if !realmPairResolvesApart(devInfo, targetInfo) {
		return nil
	}
	return fmt.Errorf(
		"--dev-url schema scope %q does not match --url schema scope %q; "+
			"a plan that holds native operations or approved inputs is rehearsed only where both resolve names alike, "+
			"so point --dev-url at a dev database using the same schema or set search_path on both URLs",
		devInfo.Schema, targetInfo.Schema,
	)
}

// resolveNamesAsTarget returns devConn, a pinned dev session, resolving
// unqualified names as the target does, and the reads the rehearsal makes of
// it.
//
// The plan and the baseline rendered from the target's read name the schema
// of every object (see [namingFor] and [readscope.Qualified]), but their SQL
// text, a view's body or a column default, names a relation as its author
// wrote it. On a realm-scoped pair whose current schemas differ, the dev
// session would resolve each such name in its own current schema: measured on
// PostgreSQL 18, a role `rebase` resolves to `rebase` in a target that holds
// that schema and to `public` in a dev database that does not. The session's
// search path is set to the target's, so the dev database builds the target's
// state under the target's names, and the plan runs against it as it will run
// on the target. A pair that resolves alike comes back as it is.
//
// The target holds the schema its session resolves to, since current_schema()
// names only a schema that exists, and the dev database is given it as well.
// A comparison need not read that schema -- one the desired schema declares
// nothing in is left out -- and without it a search path naming it resolves
// on the dev database to the next schema: a view's body naming `notes` would
// read `public.notes` where the target reads `rebase.notes`. The dev database
// claimed every schema, so the cleanup drops it with the rest.
func resolveNamesAsTarget(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	target catalog.ServerInfo,
) (*dbschema.DatabaseConnection, rehearsalReads, error) {
	if !realmPairResolvesApart(devConn.Info(), target) {
		return devConn, urlScopedReads, nil
	}
	create := "CREATE SCHEMA IF NOT EXISTS " + sqlident.Ident(target.Dialect, target.Schema)
	if _, err := devConn.ExecContext(ctx, create); err != nil {
		return nil, rehearsalReads{}, fmt.Errorf("create the target's current schema %q on the dev database: %w", target.Schema, err)
	}
	moved, err := devConn.ResolveNamesAs(ctx, target)
	if err != nil {
		return nil, rehearsalReads{}, fmt.Errorf("resolve names on the dev database as the target does: %w", err)
	}
	return moved, wholeDevReads, nil
}

// realmPairResolvesApart reports whether a dev database and a target, both at
// realm scope on an engine that resolves names along a search path, resolve
// an unqualified name to different schemas.
func realmPairResolvesApart(devInfo, targetInfo catalog.ServerInfo) bool {
	return described(targetInfo.Dialect).Schemas.Selection == descriptor.SchemaSelectionSearchPath &&
		ConnectionIsRealmScoped(devInfo) && ConnectionIsRealmScoped(targetInfo) &&
		devInfo.Schema != targetInfo.Schema
}

// rehearsalReads are the reads a rehearsal makes of its dev session: initial
// is the state the baseline is planned against, and settled the state read
// back once the baseline ran.
type rehearsalReads struct {
	initial func(context.Context, *dbschema.DatabaseConnection) (*catalog.Database, error)
	settled func(context.Context, *dbschema.DatabaseConnection) (*catalog.Database, error)
}

// urlScopedReads read the schemas the dev session's URL scope names.
var urlScopedReads = rehearsalReads{
	initial: dbschema.ReadRehearsalSchemaContext,
	settled: func(ctx context.Context, devConn *dbschema.DatabaseConnection) (*catalog.Database, error) {
		return dbschema.ReadSchemaWithSchemasContext(ctx, devConn, nil)
	},
}

// wholeDevReads read every schema of the dev database. A session that
// resolves names as the target does is read this way: its current schema is
// the target's, and a read of that schema alone would leave out the other
// schemas the target's state is built in.
var wholeDevReads = rehearsalReads{initial: readWholeDev, settled: readWholeDev}

// readWholeDev reads every schema of the dev session's database.
func readWholeDev(ctx context.Context, devConn *dbschema.DatabaseConnection) (*catalog.Database, error) {
	info := devConn.Info()
	names, err := schemaselection.RealmSchemas(ctx, info.Dialect, info.Capabilities, devConn)
	if err != nil {
		return nil, err
	}
	return dbschema.ReadSchemaWithSchemasContext(ctx, devConn, names)
}

// recreateCurrentSchema converges the freshly reset dev database to the
// target's introspected (and scope/exclude-filtered) current schema, so the plan is
// rehearsed against the same starting state it was computed for.
//
// A comparison narrowed by a selection or by a URL bound to one schema leaves
// out objects the target still holds, and the statements the dev database
// runs can name them: a kept policy that calls a function in an excluded
// schema, a planned foreign key to a table the selection does not hold. So
// the state recreated is current together with what outside.target reads of
// those objects, each whole, and what they need in turn; see
// [devseed.Closure]. The comparison the plan came from is not widened. It
// returns the objects added, and a failure after an object was added names
// them, since the dev database may be unable to build it; so does a failure of
// the plan the caller rehearses next, which runs against them.
//
// An object the recreated state holds in a schema every reset leaves as the
// claim found it is refused before anything is written; see [seedBaseline].
//
// baseline is what the dev database held before the run: its extensions, and
// for a dev database an atlas.hcl docker block provisioned its whole starting
// point. The reset left them in place, and the comparison below does not plan
// to drop the ones the target lacks: they are the dev database's environment,
// not a difference from the target. See [devclean.Baseline].
//
// target is the connection current was read through, and its URL names the
// database current was read from. An executable provider binds each value it
// reads to that database, and a dev realm binds values to the realm, whether
// it is a realm of the target's database or of a database of another name, so
// the target's values are moved to the realm before the comparison (see
// [valuecontext.Move]); without the move the provider refuses a comparison
// that holds values bound to two databases. What the realm does not read is
// left out of what it recreates (see [devclean.WithoutUnreadObjects]). The
// objects a one-database read names by the target's database are moved to the
// dev database's; see [inDevDatabase].
//
// reads are how devConn is read; see [resolveNamesAsTarget].
func recreateCurrentSchema(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	current *catalog.Database,
	target catalog.ServerInfo,
	baseline devclean.Baseline,
	runtime engine.SchemaRuntime,
	outside rehearsalOutside,
	reads rehearsalReads,
) ([]devseed.Object, error) {
	if current == nil {
		return nil, nil
	}
	devCurrent, err := reads.initial(readscope.Authored(ctx), devConn)
	if err != nil {
		return nil, fmt.Errorf("read dev database schema: %w", err)
	}
	render := func(state *catalog.Database) ([]string, error) {
		return baselineStatements(ctx, devConn, state, devCurrent, target, baseline, runtime)
	}
	dev := baselineDev{
		info:     devConn.Info(),
		guard:    devclean.NewBaselineGuardFor(devConn).WithSchemaStateReturned(baseline.ReturnsSchemaState()),
		executor: devConn.Writer(),
		read: func(ctx context.Context) (*catalog.Database, error) {
			return reads.settled(readscope.Authored(ctx), devConn)
		},
		leftAsFound:  baseline.SchemasLeftAsFound(),
		rolesAtClaim: baseline.RolesAtClaim(),
	}
	return seedBaseline(ctx, dev, current, devCurrent, outside, render)
}

// baselineDev is the dev database as [seedBaseline] writes the baseline onto
// it.
type baselineDev struct {
	info catalog.ServerInfo
	// guard is what every baseline statement passes before it is executed.
	guard    *devclean.BaselineGuard
	executor catalog.SchemaExecutor
	// read reads the dev database within the scope its URL names.
	read func(ctx context.Context) (*catalog.Database, error)
	// leftAsFound are the schemas every reset leaves as the claim found them;
	// see [devclean.Baseline.SchemasLeftAsFound].
	leftAsFound []string
	// rolesAtClaim are the roles the dev server held at the claim, nil on a
	// server the run does not own; see [devclean.Baseline.RolesAtClaim].
	rolesAtClaim []string
}

// seedBaseline writes current onto dev, with what [devseed.Closure] adds to it
// from outside the compared scope, and returns the objects added; see
// [recreateCurrentSchema]. render plans the state against devCurrent, the dev
// database as its URL reads it after the reset.
//
// Every reset leaves a schema in dev.leftAsFound as the claim found it, with
// what it holds: on a dev database whose URL pins one schema, the other
// schemas it held before the run. An object the baseline writes there outlives
// the run, and Ptah cannot remove it later without dropping or changing
// something it did not create, so a baseline that would write one is refused
// before any statement runs: an object the state holds in a schema of
// dev.leftAsFound. The objects devCurrent holds there are not counted; on
// PostgreSQL devCurrent reads the schema the URL pins and none of the others,
// so it holds none. A seeded auth.uid() in an auth schema the dev database
// already held is the case: it stays after the run, and the next run's CREATE
// FUNCTION fails on it, as it fails on a function of that name the dev
// database held. Written into a schema the claim did not find, the same object
// is the run's, and the reset drops the schema with it.
func seedBaseline(
	ctx context.Context,
	dev baselineDev,
	current, devCurrent *catalog.Database,
	outside rehearsalOutside,
	render devseed.Render,
) ([]devseed.Object, error) {
	options := devseed.Options{
		Dialect:       dev.info.Dialect,
		DefaultSchema: defaultSchemaOf(dev.info),
		SearchPath:    dev.info.IdentifierSemantics.SearchPath,
	}
	seeded, err := devseed.Closure(current, outside.executed, render, outside.target, options)
	if err != nil {
		return nil, err
	}
	seeded, err = withTargetRoles(seeded, current, outside.executed, dev.rolesAtClaim, render, options)
	if err != nil {
		return seeded.Added, devseed.WithAdded(err, seeded.Added)
	}
	if len(seeded.Statements) > 0 {
		if err := refuseWritesLeftAsFound(seeded.State, devCurrent, dev.leftAsFound, options); err != nil {
			return seeded.Added, devseed.WithAdded(err, seeded.Added)
		}
		if err := guardRehearsalBaseline(seeded.Statements, dev.guard, dev.info.Dialect); err != nil {
			return seeded.Added, devseed.WithAdded(err, seeded.Added)
		}
		if err := executeApplyStatements(ctx, dev.executor, seeded.Statements); err != nil {
			return seeded.Added, devseed.WithAdded(err, seeded.Added)
		}
	}
	return seeded.Added, devseed.WithAdded(nameColumnSequencesAsTarget(ctx, dev, seeded.State), seeded.Added)
}

// withTargetRoles adds to the state seeded holds each role of current, the
// target's state, that the statements the dev database runs name and that the
// dev server did not hold at the claim, and renders the state again; see
// [seedBaseline] and [devseed.RolesNamed].
//
// A role belongs to the server, so the target's roles outside the compared
// scope are in current.RolesOutOfScope, and a comparison plans no CREATE ROLE
// for a role the target holds. A policy `TO app_admin` planned against such a
// target names a role a dev server of its own does not hold, and fails there
// at SQLSTATE 42704 where the target applies it.
//
// held is nil on a server the run does not own, and nothing is added there:
// no reset drops a role, so the role would outlive the run. On a server the
// run owns, the reset drops every role the claim did not find, these with the
// others. A role is added with the attributes the target holds it with, and
// what renders it names in turn is read again, so a role it names is added in
// the next pass. A render that fails returns its error, and the roles of that
// pass are not among the added objects, since nothing was written.
func withTargetRoles(
	seeded devseed.Result,
	current *catalog.Database,
	executed, held []string,
	render devseed.Render,
	options devseed.Options,
) (devseed.Result, error) {
	if held == nil || current == nil || seeded.State == nil {
		return seeded, nil
	}
	for {
		lacking := rolesLacking(seeded.State, slices.Concat(current.Roles, current.RolesOutOfScope), held)
		named := devseed.RolesNamed(seeded.State, slices.Concat(executed, seeded.Statements), lacking, options)
		if len(named) == 0 {
			return seeded, nil
		}
		state := *seeded.State
		state.Roles = slices.Concat(state.Roles, named)
		statements, err := render(&state)
		if err != nil {
			return seeded, err
		}
		seeded.State, seeded.Statements = &state, statements
		for _, role := range named {
			seeded.Added = append(seeded.Added, devseed.Object{Kind: "role", Name: role.Name})
		}
	}
}

// rolesLacking returns the roles of target that neither state nor held, the
// roles the dev server held at the claim, has by name. The names compare
// exactly, as the server stores them: [devseed.RolesNamed] folds case to
// decide what a statement names, and erring there costs a role the reset
// drops, where erring here would plan a CREATE ROLE for a role the dev
// server holds.
func rolesLacking(state *catalog.Database, target []catalog.Role, held []string) []catalog.Role {
	var lacking []catalog.Role
	for _, role := range target {
		inState := slices.ContainsFunc(state.Roles, func(present catalog.Role) bool { return present.Name == role.Name })
		if !inState && !slices.Contains(held, role.Name) &&
			!slices.ContainsFunc(lacking, func(seen catalog.Role) bool { return seen.Name == role.Name }) {
			lacking = append(lacking, role)
		}
	}
	return lacking
}

// refuseWritesLeftAsFound refuses state, the baseline a rehearsal writes onto
// a dev database read as devCurrent, when it holds an object devCurrent does
// not in one of the schemas leftAsFound names; see [seedBaseline].
func refuseWritesLeftAsFound(state, devCurrent *catalog.Database, leftAsFound []string, options devseed.Options) error {
	var schemas []string
	var written []devseed.Object
	for _, schema := range leftAsFound {
		wanted, err := devseed.ObjectsIn(state, []string{schema}, options)
		if err != nil {
			return err
		}
		held, err := devseed.ObjectsIn(devCurrent, []string{schema}, options)
		if err != nil {
			return err
		}
		wanted = slices.DeleteFunc(wanted, func(object devseed.Object) bool { return slices.Contains(held, object) })
		if len(wanted) > 0 {
			schemas = append(schemas, strconv.Quote(schema))
			written = append(written, wanted...)
		}
	}
	if len(written) == 0 {
		return nil
	}
	noun, pronoun := "schema", "it"
	if len(schemas) > 1 {
		noun, pronoun = "schemas", "them"
	}
	return fmt.Errorf(
		"the dev database held %s %s before the run, and every reset leaves %s as the claim found %s, "+
			"so what the rehearsal writes there would outlive the run: %s; point --dev-url at a database that does not hold %s",
		noun, strings.Join(schemas, ", "), pronoun, pronoun, devseed.Names(written), pronoun,
	)
}

// baselineStatements returns the statements that converge the dev database,
// read as devCurrent after its reset, to state; see [recreateCurrentSchema].
func baselineStatements(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	state, devCurrent *catalog.Database,
	targetInfo catalog.ServerInfo,
	baseline devclean.Baseline,
	runtime engine.SchemaRuntime,
) ([]string, error) {
	info := devConn.Info()
	targetURL := targetInfo.URL
	state = inDevDatabase(state, targetInfo, info)
	target, err := dbschematogo.ConvertDBSchemaToGoSchema(ctx, state, info.Dialect, runtime)
	if err != nil {
		return nil, err
	}
	normalizeBaselineSerialColumns(target, info.Dialect)
	moved, err := valuecontext.Move(ctx, runtime, info.Dialect, targetURL, info.URL, target)
	if err != nil {
		return nil, fmt.Errorf("bind the target's current schema to the dev database: %w", err)
	}
	if moved {
		target = devclean.WithoutUnreadObjects(target, devCurrent)
	}
	devCurrent = baseline.WithoutEnvironment(devCurrent, devclean.ReadExtensions(state))
	devCurrent, err = baseline.WithoutStartingPoint(ctx, devCurrent, state, defaultSchemaOf(info), runtime.Selections())
	if err != nil {
		return nil, err
	}
	controlComments, err := extensionControlComments(ctx, devConn, state)
	if err != nil {
		return nil, err
	}
	if err := omitControlComments(target, devclean.ReadExtensions(devCurrent), controlComments); err != nil {
		return nil, err
	}
	omitRealmRootGrants(info, target, devCurrent)
	diff, err := schemadiff.CompareWithDatabase(ctx, devConn, target, devCurrent, nil, runtime)
	if err != nil {
		return nil, fmt.Errorf("compare current schema with dev database: %w", err)
	}
	if !diff.HasChanges() {
		return nil, nil
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		ctx, runtime,
		diff, info.Dialect, planext.Options{
			Capabilities: info.Capabilities,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("generate current schema DDL for dev database: %w", err)
	}
	return statements, nil
}

// guardRehearsalBaseline holds the whole baseline to the dev database's realm
// before any of its statements executes; see [devclean.BaselineGuard].
//
// The read the baseline is compared against includes environment the dev
// database observes outside its realm, so that an equal environment plans
// nothing. That grants no permission to write it, and neither does deriving a
// statement from the target.
func guardRehearsalBaseline(statements []string, guard *devclean.BaselineGuard, dialect string) error {
	for i, statement := range statements {
		if err := guard.ValidateStatement(statement); err != nil {
			return fmt.Errorf("baseline statement %d (%s) cannot be rehearsed: %w", i+1, statementExcerpt(statement, dialect), err)
		}
	}
	return nil
}

// statementExcerpt is the first line of a statement that is not a comment,
// cut to a length an error message can carry, so a refusal names what it
// refused. A `--` line whose first token the dialect's lexer reads as
// lexer.TokenUnknown, a setting the server reads, is not a comment, and the
// guard refuses it, so it is the excerpt. A statement holding only comments
// is excerpted by its first line.
func statementExcerpt(statement, dialect string) string {
	const limit = 80
	options := dialectlexer.Options(dialect)
	excerpt := ""
	for line := range strings.SplitSeq(statement, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if excerpt == "" {
			excerpt = line
		}
		if !strings.HasPrefix(line, "--") || readsAsSetting(line, options) {
			excerpt = line
			break
		}
	}
	if runes := []rune(excerpt); len(runes) > limit {
		return string(runes[:limit]) + "..."
	}
	return excerpt
}

// readsAsSetting reports whether the first token of line, read as the head of
// a text under options, is a TokenUnknown: a setting the server reads, rather
// than a comment. The shared rules read every `--` line as a comment or as an
// operator, so only a lexer of an engine's own answers true.
func readsAsSetting(line string, options lexer.Options) bool {
	return lexer.NewLexerWithOptions(line, options).NextToken().Type == lexer.TokenUnknown
}

// nameColumnSequencesAsTarget renames each sequence a dev column owns to the
// name the target's column owns it under.
//
// The baseline writes a serial or identity column as its shorthand, so the dev
// database creates the column's sequence under the name PostgreSQL gives it.
// The target may hold another one: renaming a table or a column leaves its
// sequence's name as it was. A plan names the target's sequence, in a grant on
// it for one, and without this rename the rehearsal of that plan fails with
// `relation "items_id_seq" does not exist` before the target is reached
// (stokaro/ptah#4064). The renames go through temporary names, so one cannot
// collide with a sequence another column holds under the name it takes.
func nameColumnSequencesAsTarget(ctx context.Context, dev baselineDev, current *catalog.Database) error {
	dialect := dev.info.Dialect
	if current == nil || !described(dialect).Conversion.OwnedSequences {
		return nil
	}
	want := make(map[string]string)
	for _, table := range current.Tables {
		for _, column := range table.Columns {
			if column.OwnedSequence != "" {
				want[columnSequenceKey(table.Schema, table.Name, column.Name)] = column.OwnedSequence
			}
		}
	}
	if len(want) == 0 {
		return nil
	}
	read, err := dev.read(ctx)
	if err != nil {
		return fmt.Errorf("read dev database sequences: %w", err)
	}
	var moves, settles []string
	for _, table := range read.Tables {
		for _, column := range table.Columns {
			target, ok := want[columnSequenceKey(table.Schema, table.Name, column.Name)]
			if !ok || column.OwnedSequence == "" || column.OwnedSequence == target {
				continue
			}
			temporary := fmt.Sprintf("ptah_rehearsal_seq_%d", len(moves))
			moves = append(moves, "ALTER SEQUENCE "+sqlident.QualifiedIdent(dialect, table.Schema, column.OwnedSequence)+
				" RENAME TO "+sqlident.Ident(dialect, temporary))
			settles = append(settles, "ALTER SEQUENCE "+sqlident.QualifiedIdent(dialect, table.Schema, temporary)+
				" RENAME TO "+sqlident.Ident(dialect, target))
		}
	}
	if err := executeApplyStatements(ctx, dev.executor, slices.Concat(moves, settles)); err != nil {
		return fmt.Errorf("name dev database sequences as the target names them: %w", err)
	}
	return nil
}

// columnSequenceKey identifies a column across the target's read and the dev
// database's, which name the same schema the same way.
func columnSequenceKey(schema, table, column string) string {
	return catalog.QualifyTableName(schema, table) + "." + column
}

// defaultSchemaOf is the schema an object a read over info leaves
// unqualified is in: the connected schema, or in realm scope the dialect's
// default.
func defaultSchemaOf(info catalog.ServerInfo) string {
	if info.Schema != "" {
		return info.Schema
	}
	return dialectDefaultSchema(info.Dialect)
}

// normalizeBaselineSerialColumns rewrites introspected SERIAL columns back to
// their SERIAL spelling for baseline recreation, on an engine whose serial
// column owns the sequence that fills it, as the PostgreSQL family's
// descriptors state with Conversion.OwnedSequences.
// Introspection deliberately omits the implicit sequences owned by SERIAL
// columns, so replaying the raw "integer DEFAULT nextval('...')" form on an
// empty dev database would reference a sequence that never gets created.
// Columns whose nextval default names an explicitly introspected sequence
// keep their default: that sequence is part of the baseline and is created.
func normalizeBaselineSerialColumns(baseline *schemamodel.Database, dialect string) {
	if !described(dialect).Conversion.OwnedSequences {
		return
	}
	sequences := make(map[string]bool, len(baseline.Sequences))
	for _, sequence := range baseline.Sequences {
		sequences[strings.ToLower(sequence.Name)] = true
	}
	for i := range baseline.Fields {
		field := &baseline.Fields[i]
		sequenceName := nextvalSequenceName(field.DefaultExpr)
		if sequenceName == "" || sequences[strings.ToLower(sequenceName)] {
			continue
		}
		serialType, ok := serialTypeForBaseline(field.Type)
		if !ok {
			continue
		}
		field.Type = serialType
		field.DefaultExpr = ""
	}
}

// nextvalSequenceName extracts the unqualified sequence name from a
// PostgreSQL nextval('<name>'::regclass) column default, or "" when the
// default is not a nextval call.
func nextvalSequenceName(defaultExpr string) string {
	trimmed := strings.TrimSpace(defaultExpr)
	if !strings.HasPrefix(strings.ToLower(trimmed), "nextval(") {
		return ""
	}
	start := strings.Index(trimmed, "'")
	if start < 0 {
		return ""
	}
	end := strings.Index(trimmed[start+1:], "'")
	if end < 0 {
		return ""
	}
	name := trimmed[start+1 : start+1+end]
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		name = name[dot+1:]
	}
	return strings.Trim(name, `"`)
}

func serialTypeForBaseline(columnType string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(columnType)) {
	case "integer", "int", "int4":
		return "SERIAL", true
	case "bigint", "int8":
		return "BIGSERIAL", true
	case "smallint", "int2":
		return "SMALLSERIAL", true
	}
	return "", false
}
