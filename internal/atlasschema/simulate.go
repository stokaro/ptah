package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/devlock"
	"ptah.run/internal/sqlident"
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
	// simulation covers exactly what would be applied.
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
func (p ApplyRuntimePlan) SimulateOnDev(ctx context.Context, opts SimulateOptions) error {
	if strings.TrimSpace(opts.DevURL) == "" {
		return nil
	}
	statements := opts.Statements
	if len(statements) == 0 {
		statements = p.plan.statements
	}
	if len(statements) == 0 {
		return nil
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

	return rehearseStatementsOnDev(ctx, p.conn, dev.conn, dev.baseline, p.current, p.txMode, statements)
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
	if err := atlasurl.ValidateDialectMatch(devURL, targetInfo.Dialect); err != nil {
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
	// A YDB dev URL is compared live for the same reason: its dev database is
	// a dev realm Resolve creates, so the URL that names the target names a
	// realm beside it, and only the connection can say which.
	serverDev := isDevServer(devURL)
	comparedByURL := !devdocker.ResolvedPerRun(devURL)
	targets := []string{targetURL}
	if !comparedByURL {
		targets = nil
	}
	for _, target := range urlAliasCandidates(devURL, targets) {
		sameTarget, err := atlasurl.MayAddressSameDatabase(devURL, target)
		if err != nil {
			return simulationDev{}, fmt.Errorf("compare --dev-url with target database: %w", err)
		}
		if sameTarget {
			return simulationDev{}, errDevURLIsTarget
		}
	}
	protected := []devlock.Protected{{Conn: targetConn, Refusal: errDevURLIsTarget}}
	for _, desired := range aliasCandidates(devURL, desiredURLs) {
		if isDirectDatabaseURL(desired) {
			protected = append(protected, devlock.Protected{URL: desired, Refusal: devURLIsDesiredError(desired)})
		}
		if serverDev || !comparedByURL {
			continue
		}
		sameDesired, err := sameDirectDatabaseURL(devURL, desired)
		if err != nil {
			return simulationDev{}, fmt.Errorf("compare --dev-url with --to desired-state database %q: %w", desired, err)
		}
		if sameDesired {
			return simulationDev{}, devURLIsDesiredError(desired)
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
	return atlasurl.MayAddressSameDatabase(databaseURL, candidate)
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
	txMode migrator.MigrationTxMode,
	statements []string,
) error {
	if targetConn == nil {
		return errors.New("schema apply simulation requires target database connection")
	}
	// A plan for a whole server names each table by its database and runs as
	// written on a whole dev server; the guard keeps it to what the reset of
	// that server removes. A plan for one database is re-scoped onto the dev
	// database.
	var err error
	if devConn.Info().WholeServer {
		err = guardServerRehearsal(statements, devConn.Info())
	} else {
		statements, err = rescopeStatementsForDevDatabase(
			statements, devConn.Info().Dialect, targetConn.Info().Schema, devConn.Info().Schema,
		)
	}
	if err != nil {
		return err
	}
	// The lint reads exactly what the dev database will execute, so it runs on
	// the re-scoped statements rather than on the plan they came from.
	if err := checkPlanStatements(statements, devConn.Info().Dialect); err != nil {
		return err
	}
	// The dev database executes these statements for real, and they came from
	// outside the operator's project, so the session carries every engine-level
	// restriction its dialect supports. Taking the session through
	// WithUntrustedSQLSession is what makes an unrestricted rehearsal
	// impossible to write; the lint above is only a lint.
	return devConn.WithUntrustedSQLSession(ctx, func(session *dbschema.DatabaseConnection) error {
		return rehearseOnPreparedDev(ctx, session, baseline, current, txMode, statements)
	})
}

// checkPlanStatements is the escape lint as used by the rehearsal core. It is
// a variable so a test can neutralize the lint and prove that the engine-level
// restrictions — not the lint — are what stop an escape.
var checkPlanStatements = CheckPlanStatementsSandboxable

func rehearseOnPreparedDev(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	baseline devclean.Baseline,
	current *catalog.Database,
	txMode migrator.MigrationTxMode,
	statements []string,
) error {
	devConn.SchemaWriter().SetDryRun(false)
	if err := devclean.Reset(ctx, devConn, baseline); err != nil {
		return &SimulationError{Stage: "reset", Err: err}
	}
	if err := recreateCurrentSchema(ctx, devConn, current, baseline); err != nil {
		return &SimulationError{Stage: "baseline", Err: err}
	}
	if err := applyStatements(ctx, devConn, txMode, statements); err != nil {
		return &SimulationError{Stage: "plan", Err: err}
	}
	return nil
}

// checkSimulationSchemaScope fails explicitly when the dev database's schema
// scope differs from the target's. Only dialects whose connection schema names
// a namespace inside the database participate: where it names the database
// itself the two legitimately differ, which is exactly why the plan has to be
// re-scoped before it is rehearsed.
func checkSimulationSchemaScope(devInfo, targetInfo catalog.ServerInfo) error {
	if schemaScopeNamesDatabase(targetInfo.Dialect) {
		return nil
	}
	if devInfo.Schema == targetInfo.Schema {
		return nil
	}
	return fmt.Errorf(
		"--dev-url schema scope %q does not match --url schema scope %q; point --dev-url at a dev database using the same schema",
		devInfo.Schema, targetInfo.Schema,
	)
}

// recreateCurrentSchema converges the freshly reset dev database to the
// target's introspected (and scope/exclude-filtered) current schema, so the plan is
// rehearsed against the same starting state it was computed for.
//
// baseline is what the dev database held before the run: its extensions, and
// for a dev database an atlas.hcl docker block provisioned its whole starting
// point. The reset left them in place, and the comparison below does not plan
// to drop the ones the target lacks: they are the dev database's environment,
// not a difference from the target. See [devclean.Baseline].
func recreateCurrentSchema(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	current *catalog.Database,
	baseline devclean.Baseline,
) error {
	if current == nil {
		return nil
	}
	target := dbschematogo.ConvertDBSchemaToGoSchema(current, devConn.Info().Dialect)
	normalizeBaselineSerialColumns(target, devConn.Info().Dialect)
	devCurrent, err := dbschema.ReadSchemaWithSchemasContext(ctx, devConn, nil)
	if err != nil {
		return fmt.Errorf("read dev database schema: %w", err)
	}
	devCurrent = baseline.WithoutEnvironment(devCurrent, catalogExtensionNames(current))
	devCurrent = baseline.WithoutStartingPoint(devCurrent, current, defaultSchemaOf(devConn.Info()))
	info := devConn.Info()
	diff, err := schemadiff.CompareWithDatabase(ctx, devConn, target, devCurrent, nil)
	if err != nil {
		return fmt.Errorf("compare current schema with dev database: %w", err)
	}
	if diff.HasChanges() {
		statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, info.Dialect, planner.Options{
			Capabilities: info.Capabilities,
		})
		if err != nil {
			return fmt.Errorf("generate current schema DDL for dev database: %w", err)
		}
		if err := executeApplyStatements(ctx, devConn.Writer(), statements); err != nil {
			return err
		}
	}
	return nameColumnSequencesAsTarget(ctx, devConn, current)
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
func nameColumnSequencesAsTarget(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	current *catalog.Database,
) error {
	dialect := devConn.Info().Dialect
	if current == nil || !platform.IsPostgresFamily(dialect) {
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
	dev, err := dbschema.ReadSchemaWithSchemasContext(ctx, devConn, nil)
	if err != nil {
		return fmt.Errorf("read dev database sequences: %w", err)
	}
	var moves, settles []string
	for _, table := range dev.Tables {
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
	if err := executeApplyStatements(ctx, devConn.Writer(), slices.Concat(moves, settles)); err != nil {
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

// catalogExtensionNames is the set of extensions a read database holds.
func catalogExtensionNames(database *catalog.Database) map[string]bool {
	names := make(map[string]bool, len(database.Extensions))
	for _, extension := range database.Extensions {
		names[extension.Name] = true
	}
	return names
}

// declaredExtensionNames is the set of extensions a desired state declares.
func declaredExtensionNames(database *schemamodel.Database) map[string]bool {
	names := make(map[string]bool)
	if database == nil {
		return names
	}
	for _, extension := range database.Extensions {
		names[extension.Name] = true
	}
	return names
}

// normalizeBaselineSerialColumns rewrites introspected PostgreSQL-family
// SERIAL columns back to their SERIAL spelling for baseline recreation.
// Introspection deliberately omits the implicit sequences owned by SERIAL
// columns, so replaying the raw "integer DEFAULT nextval('...')" form on an
// empty dev database would reference a sequence that never gets created.
// Columns whose nextval default names an explicitly introspected sequence
// keep their default: that sequence is part of the baseline and is created.
func normalizeBaselineSerialColumns(baseline *schemamodel.Database, dialect string) {
	if !platform.IsPostgresFamily(dialect) {
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
