package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/devdocker"
	"go.5x5.cz/ptah/internal/devlock"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
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

	devConn, releaseDev, err := connectSimulationDev(ctx, opts.DevURL, p.conn, opts.TargetURL, opts.DesiredURLs)
	if err != nil {
		return err
	}
	// Registered before the close so it runs after it: a provisioned container
	// is removed only once the connection to it is gone.
	defer releaseDev()
	defer dbschema.CloseAndWarn(devConn)
	// Registered after the close, so it runs before it: the dev database is
	// handed back with nothing the rehearsal put in it, whether the rehearsal
	// succeeded or failed.
	defer discardDevRehearsalArtifacts(ctx, devConn)

	return rehearseStatementsOnDev(ctx, p.conn, devConn, p.current, p.txMode, statements)
}

// devCleanupTimeout bounds the post-rehearsal cleanup so a canceled command
// still returns the dev database empty instead of leaving the rehearsal behind.
const devCleanupTimeout = 30 * time.Second

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
func discardDevRehearsalArtifacts(ctx context.Context, devConn *dbschema.DatabaseConnection) {
	if devConn == nil {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), devCleanupTimeout)
	defer cancel()
	devConn.SchemaWriter().SetDryRun(false)
	if err := devConn.SchemaWriter().DropAllTables(cleanupCtx); err != nil {
		slog.Warn("failed to clean the dev database after the rehearsal; it may still hold rehearsed objects",
			"error", err)
	}
}

// connectSimulationDev validates the dev database URL against the target and
// opens the dev connection used to rehearse a plan.
//
// devURL is the operator's spelling, not a normalized copy: whether a value is
// a `docker://` URL at all is decided from those bytes, and normalizing first
// promoted a value the pinned binary cannot parse into a started container.
// See [devdocker.Parse]. The callers have already answered an empty one.
//
// The caller owns both returned values: the connection must be closed, and the
// release must be called to remove a container this call may have started. The
// release is always non-nil, including on the error return, so a caller can
// defer it without asking whether provisioning happened.
func connectSimulationDev(
	ctx context.Context,
	devURL string,
	targetConn *dbschema.DatabaseConnection,
	targetURL string,
	desiredURLs []string,
) (*dbschema.DatabaseConnection, func(), error) {
	noRelease := func() {}
	targetInfo := targetConn.Info()
	// The dialect check reads the URL as written: a `docker://` value names its
	// engine in the text, so a mismatch is answerable before a container is
	// started and a refused run pays for none.
	if err := atlasurl.ValidateDialectMatch(devURL, targetInfo.Dialect); err != nil {
		return nil, noRelease, err
	}
	// The alias checks below ask whether the dev database IS the target or the
	// desired-state database, because the dev database is reset destructively.
	// A container that does not exist yet cannot be either, and asking the
	// question of a `docker://` URL does not merely waste work -- the URL
	// comparison has no dialect to compare and answers `unsupported database
	// URL dialect`, which would refuse every docker dev database on this verb
	// with a sentence about a dialect the operator did not choose.
	for _, target := range aliasCandidates(devURL, []string{targetURL}) {
		sameTarget, err := atlasurl.MayAddressSameDatabase(devURL, target)
		if err != nil {
			return nil, noRelease, fmt.Errorf("compare --dev-url with target database: %w", err)
		}
		if sameTarget {
			return nil, noRelease, errors.New("--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it")
		}
	}
	for _, desired := range aliasCandidates(devURL, desiredURLs) {
		sameDesired, err := sameDirectDatabaseURL(devURL, desired)
		if err != nil {
			return nil, noRelease, fmt.Errorf("compare --dev-url with --to desired-state database %q: %w", desired, err)
		}
		if sameDesired {
			return nil, noRelease, fmt.Errorf("--dev-url must not point at the --to desired-state database %q: the dev database is reset destructively before the plan is rehearsed on it", desired)
		}
	}

	resolved, release, err := devdocker.Resolve(ctx, devURL, devdocker.Options{})
	if err != nil {
		return nil, noRelease, err
	}

	devConn, err := dbschema.ConnectToDatabase(ctx, strings.TrimSpace(resolved))
	if err != nil {
		release()
		return nil, noRelease, fmt.Errorf("connect to --dev-url: %w", err)
	}

	devInfo := devConn.Info()
	if platform.NormalizeDialect(devInfo.Dialect) != platform.NormalizeDialect(targetInfo.Dialect) {
		dbschema.CloseAndWarn(devConn)
		release()
		return nil, noRelease, fmt.Errorf("--dev-url dialect %q does not match --url dialect %q", devInfo.Dialect, targetInfo.Dialect)
	}
	if err := checkSimulationSchemaScope(devInfo, targetInfo); err != nil {
		dbschema.CloseAndWarn(devConn)
		release()
		return nil, noRelease, err
	}
	return devConn, release, nil
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

// sameDirectDatabaseURL compares candidate only when its scheme names a
// directly connectable database. Desired-state files and migration directories
// share the DesiredURLs collection and are intentionally not database aliases.
func sameDirectDatabaseURL(databaseURL, candidate string) (bool, error) {
	scheme, _, found := strings.Cut(strings.TrimSpace(candidate), ":")
	if !found || platform.NormalizeDialect(scheme) == "" {
		return false, nil
	}
	return atlasurl.MayAddressSameDatabase(databaseURL, candidate)
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
	targetConn *dbschema.DatabaseConnection,
	devConn *dbschema.DatabaseConnection,
	current *dbschematypes.DBSchema,
	txMode migrator.MigrationTxMode,
	statements []string,
) error {
	if targetConn == nil {
		return errors.New("schema apply simulation requires target database connection")
	}
	statements, err := rescopeStatementsForDevDatabase(
		statements, devConn.Info().Dialect, targetConn.Info().Schema, devConn.Info().Schema)
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
		sameTargetRealm, err := devlock.SameRealm(ctx, targetConn, session)
		if err != nil {
			return fmt.Errorf("compare live --dev-url and target database realms: %w", err)
		}
		if sameTargetRealm {
			return errors.New("--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it")
		}
		return rehearseOnPreparedDev(ctx, session, current, txMode, statements)
	})
}

// checkPlanStatements is the escape lint as used by the rehearsal core. It is
// a variable so a test can neutralize the lint and prove that the engine-level
// restrictions — not the lint — are what stop an escape.
var checkPlanStatements = CheckPlanStatementsSandboxable

func rehearseOnPreparedDev(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	current *dbschematypes.DBSchema,
	txMode migrator.MigrationTxMode,
	statements []string,
) error {
	devConn.SchemaWriter().SetDryRun(false)
	if err := devConn.SchemaWriter().DropAllTables(ctx); err != nil {
		return &SimulationError{Stage: "reset", Err: err}
	}
	if err := recreateCurrentSchema(ctx, devConn, current); err != nil {
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
func checkSimulationSchemaScope(devInfo, targetInfo dbschematypes.DBInfo) error {
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
func recreateCurrentSchema(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	current *dbschematypes.DBSchema,
) error {
	if current == nil {
		return nil
	}
	baseline := dbschematogo.ConvertDBSchemaToGoSchema(current)
	normalizeBaselineSerialColumns(baseline, devConn.Info().Dialect)
	devCurrent, err := dbschema.ReadSchemaWithSchemas(devConn, nil)
	if err != nil {
		return fmt.Errorf("read dev database schema: %w", err)
	}
	info := devConn.Info()
	diff, err := schemadiff.CompareWithDatabase(ctx, devConn, baseline, devCurrent, nil)
	if err != nil {
		return fmt.Errorf("compare current schema with dev database: %w", err)
	}
	if !diff.HasChanges() {
		return nil
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, baseline, info.Dialect, planner.Options{
		Capabilities: info.Capabilities,
	})
	if err != nil {
		return fmt.Errorf("generate current schema DDL for dev database: %w", err)
	}
	return executeApplyStatements(ctx, devConn.Writer(), statements)
}

// normalizeBaselineSerialColumns rewrites introspected PostgreSQL-family
// SERIAL columns back to their SERIAL spelling for baseline recreation.
// Introspection deliberately omits the implicit sequences owned by SERIAL
// columns, so replaying the raw "integer DEFAULT nextval('...')" form on an
// empty dev database would reference a sequence that never gets created.
// Columns whose nextval default names an explicitly introspected sequence
// keep their default: that sequence is part of the baseline and is created.
func normalizeBaselineSerialColumns(baseline *goschema.Database, dialect string) {
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
