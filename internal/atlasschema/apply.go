package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devlock"
	"ptah.run/internal/dispatchwitness"
	"ptah.run/internal/errorkind"
	"ptah.run/internal/featureselect"
	"ptah.run/internal/pgsystemschema"
	"ptah.run/internal/planrefusal"
	"ptah.run/internal/planstyle"
	"ptah.run/internal/protectedtable"
	"ptah.run/internal/providerurl"
	"ptah.run/internal/readscope"
	"ptah.run/internal/rolepassword"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/schemascope"
	"ptah.run/internal/schemaselection"
	"ptah.run/internal/sqliteforeignkeys"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/internal/sqlscript"
	"ptah.run/internal/undecidednote"
	"ptah.run/internal/unmodelednote"
	"ptah.run/internal/valuecontext"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff"
)

type ApplyOptions struct {
	// Runtime selects feature services and codecs for this operation. It is required.
	Runtime engine.SchemaRuntime
	// Shape is the statement shape of the plan; see [planext.Options.Shape].
	Shape   planext.StatementShape
	ToURLs  []string
	Exclude []string
	// ProjectRoots bounds the files a desired state may make this process read.
	//
	// A declaration names the file that carries its rows, and a schema is not
	// always one the reader wrote, so the path it names is confined to the
	// project the caller is operating in rather than followed anywhere. That is
	// the boundary `file()` in atlas.hcl already has. With no value a declaration
	// that still has to read its file is refused.
	ProjectRoots []string
	// Schemas restricts both comparison sides to the named schema scopes.
	Schemas []string
	// Include restricts both comparison sides to resources matched by
	// Atlas-style include selectors.
	Include []string
	Policy  DiffPolicy
	// ProtectedTables fences declared row sets off from this plan: a plan that
	// would change one is refused rather than rated, and there is no override
	// on this path. See [protectedtable] for which entry fences which table,
	// and managedDataStatements for why the refusal has no yes.
	ProtectedTables []string
	// DevURL is the dev database used to replay migration-directory
	// desired-state sources.
	DevURL string
	// baseline is what the database held before a rehearsal claimed it: its
	// extensions, and the whole starting point of a dev database an atlas.hcl
	// docker block provisioned. The comparison leaves what the desired schema
	// does not declare out of the current state. Only the rehearsal's
	// end-state check sets it, on the dev database it rehearsed on.
	baseline devclean.Baseline
	// DevServerDisposable is the operator's declaration that the server
	// DevURL names is the run's own; see
	// [ptah.run/internal/migrationreplay.Options.DevServerDisposable].
	DevServerDisposable bool
	// ProjectEnv expands env:// desired-state references.
	ProjectEnv atlassource.ProjectEnv
	// PreparedTo carries a classified desired source whose migration-directory
	// bytes were captured and policy-checked before the target was opened.
	PreparedTo *atlassource.Set
	// LocalFilesOnly restricts ToURLs to local schema files, preserving the
	// pre-resolver loading behavior. `schema plan` sets it because a saved
	// plan fingerprints local desired-state files only.
	LocalFilesOnly bool
	// ToSources carries the same desired-state sources as ToURLs, each with the
	// variable scope its atlas.hcl `data "hcl_schema"` block put around it. It
	// is read only on the LocalFilesOnly path, which does not classify and so
	// cannot pick the scope up on its own; empty means "no source carries one"
	// and ToURLs is loaded as written.
	ToSources []schemafile.Source
	// Desired supplies a pre-loaded desired schema model. When set, ToURLs are
	// not resolved: the native command tree uses it to plan from Go-annotation
	// roots and native schema files loaded through the shared desired-source
	// loader. The Atlas-compatible callers never set it.
	Desired *schemamodel.Database
	// Vars supplies values for HCL schema-file `variable` blocks, as `--var`
	// spells them; see [ptah.run/internal/schemafile.Options].
	Vars []string
	// IgnoreUnknownHCLNames is the Atlas-compatible surface's unknown-name
	// policy; see [ptah.run/internal/atlassource.ResolveOptions].
	IgnoreUnknownHCLNames bool
	// RefuseUnrequestedSchemas holds an HCL desired state to Schemas; see
	// [ptah.run/internal/schemafile.Options.RequestedSchemas].
	RefuseUnrequestedSchemas bool
	// OmitNullBackfill plans SET NOT NULL without filling the column's NULL
	// rows from its declared default first; see
	// [ptah.run/migration/planext.Options]. The caller selects it from its
	// compatibility policy.
	OmitNullBackfill bool
	// Diagnostics receives out-of-band notices, such as an --exclude selector
	// that named no object. Nil discards them.
	Diagnostics io.Writer
	// WarnUndescribedSchemas writes a warning to Diagnostics for each schema
	// whose tables the plan removes only because the read reached a schema
	// nobody named; see [schemascope.UndescribedRemovals]. The native surface
	// sets it. The compatibility surface leaves it off: the pinned Atlas
	// community binary plans the same removal without a warning, and the
	// warning names native remedies, such as --schemas, that its flags spell
	// differently.
	WarnUndescribedSchemas bool
	// RefuseUnmatchedExclude makes an --exclude selector that named nothing in
	// either state an error rather than a notice. Only the verb that executes
	// the plan sets it: `schema apply` refuses because a selector that
	// protected nothing means the plan is free to change the object the user
	// wrote it for.
	RefuseUnmatchedExclude bool
	// ValidateDesiredSchema applies a caller-selected desired-schema policy
	// after loading and before planning. Nil accepts every modeled object.
	ValidateDesiredSchema func(*schemamodel.Database) error
	// ValidateCurrentSchema applies a caller-selected policy to the fully
	// introspected target before planning. Nil accepts every modeled object.
	ValidateCurrentSchema func(*schemamodel.Database) error
	// ValidateLiveObject applies a caller-selected policy to supplemental
	// catalog objects in the current target and in database-backed or replayed
	// desired sources. Nil performs no supplemental catalog reads.
	ValidateLiveObject func(LiveSchemaObject) error
	// ValidateMigrationSource applies a caller-selected policy to a stable
	// migration-directory desired-state snapshot before dev-database replay.
	ValidateMigrationSource func(fs.FS) error
	// ValidateLocalSchemaSource applies a caller-selected policy to each local
	// desired-schema path before parsing or dev-database work.
	ValidateLocalSchemaSource func(string) error
	// withoutData leaves the declared rows out of the plan. A rehearsal plans
	// the schema again on its dev database, which holds none of the target's
	// rows, and runs the target plan's data statements instead.
	withoutData bool
}

type ApplyPlan struct {
	target       string
	capabilities capability.Capabilities
	provider     *PlanProviderBinding
	units        []renderer.Fragment
	annotations  []renderer.Annotation
	transactions [][]plangraph.Transaction
	owned        []safety.StatementAssessment
	// columns holds, position for position with the planned statements, what a column
	// modification establishes that a statement's words cannot say; see
	// [safety.ColumnVerdicts]. It stays apart from owned because a plan marks a
	// statement an owner judged, and an edit to such a plan fails closed.
	columns []safety.StatementAssessment
	// texts is, for each unit, the text its owner writes the operation as,
	// where the unit is native and the owner has a text form for it, and
	// "" otherwise. It is a display projection, as a migration file holds the
	// operation: execution dispatches the captured unit, never this text.
	texts []string
	// changes names the change of each unit, in the words the Atlas CLI writes
	// above a statement, and is empty for a unit nothing names. It is a display
	// projection like texts, and only the compatibility surface reads it.
	changes []string
}

// ApplyRuntimeOptions configures Atlas schema apply planning and execution.
type ApplyRuntimeOptions struct {
	// Shape is the statement shape of the plan; see [planext.Options.Shape].
	Shape planext.StatementShape
	// Runtime selects feature services and codecs for this operation. It is required.
	Runtime engine.SchemaRuntime
	DevURL  string
	ToURLs  []string
	Exclude []string
	// DevServerDisposable is the operator's declaration that the server
	// DevURL names is the run's own; see
	// [ptah.run/internal/migrationreplay.Options.DevServerDisposable].
	DevServerDisposable bool
	// ProjectRoots bounds the files a desired state may make this process read;
	// see [ApplyOptions.ProjectRoots].
	ProjectRoots []string
	// Schemas restricts both comparison sides to the named schema scopes.
	Schemas []string
	// Include restricts both comparison sides to resources matched by
	// Atlas-style include selectors.
	Include []string
	Policy  DiffPolicy
	// ProtectedTables fences declared row sets off from this apply; see
	// [ApplyOptions.ProtectedTables].
	ProtectedTables []string
	TxMode          migrator.MigrationTxMode
	DryRun          bool
	// ProjectEnv expands env:// desired-state references in ToURLs.
	ProjectEnv atlassource.ProjectEnv
	// PreparedTo is the command-bound desired source prepared before target
	// connection and lock acquisition; see [ApplyOptions.PreparedTo].
	PreparedTo *atlassource.Set
	// Desired supplies a pre-loaded desired schema model; see
	// [ApplyOptions.Desired].
	Desired *schemamodel.Database
	// Diagnostics receives out-of-band notices; see [ApplyOptions.Diagnostics].
	Diagnostics io.Writer
	// WarnUndescribedSchemas is [ApplyOptions.WarnUndescribedSchemas].
	WarnUndescribedSchemas bool
	// Vars supplies values for HCL schema-file `variable` blocks, as `--var`
	// spells them; see [ptah.run/internal/schemafile.Options].
	Vars []string
	// IgnoreUnknownHCLNames is the Atlas-compatible surface's unknown-name
	// policy; see [ApplyOptions.IgnoreUnknownHCLNames].
	IgnoreUnknownHCLNames bool
	// RefuseUnrequestedSchemas is [ApplyOptions.RefuseUnrequestedSchemas].
	RefuseUnrequestedSchemas bool
	// OmitNullBackfill is [ApplyOptions.OmitNullBackfill].
	OmitNullBackfill bool
	// ValidateDesiredSchema applies a caller-selected desired-schema policy;
	// see [ApplyOptions.ValidateDesiredSchema].
	ValidateDesiredSchema func(*schemamodel.Database) error
	// ValidateCurrentSchema applies a caller-selected current-schema policy;
	// see [ApplyOptions.ValidateCurrentSchema].
	ValidateCurrentSchema func(*schemamodel.Database) error
	// ValidateLiveObject applies a caller-selected supplemental catalog policy;
	// see [ApplyOptions.ValidateLiveObject].
	ValidateLiveObject func(LiveSchemaObject) error
	// ValidateMigrationSource applies a caller-selected migration-source policy;
	// see [ApplyOptions.ValidateMigrationSource].
	ValidateMigrationSource func(fs.FS) error
	// ValidateLocalSchemaSource applies a caller-selected local-source policy;
	// see [ApplyOptions.ValidateLocalSchemaSource].
	ValidateLocalSchemaSource func(string) error
	// DesiredFor loads the sources Desired was loaded from again, bound to the
	// database databaseURL names. A rehearsal of a plan that holds a native
	// operation plans the change again on its dev database, because a native
	// operation is bound to the database it was planned for, and the desired
	// state it plans from has to be bound to the dev database. Nil plans from
	// Desired as it is, which binds to the dev database when Desired was loaded
	// without an address and is refused when it is bound to the target.
	DesiredFor func(ctx context.Context, databaseURL string) (*schemamodel.Database, error)
}

// ApplyRuntimePlan is a prepared Atlas schema apply operation for one open
// database connection.
type ApplyRuntimePlan struct {
	runtime engine.SchemaRuntime
	plan    ApplyPlan
	dryRun  bool
	conn    *dbschema.DatabaseConnection
	txMode  migrator.MigrationTxMode
	// current is the filtered (schema/include scope and exclude) introspected
	// target state the plan was computed against; the dev database simulation
	// recreates it before rehearsing the plan.
	current *catalog.Database
	// undecided is what [ApplyRuntimePlan.Undecided] returns.
	undecided schemadiff.Diagnostics
	// rehearsal is what a rehearsal of a plan that holds a native operation
	// plans again on its dev database; see [ApplyRuntimePlan.SimulateOnDev].
	rehearsal nativeRehearsal
	// passwords are the passwords the desired schema declares, by the
	// variables the plan's statements name in their place. The plan's
	// executions read them before the environment; no output holds them.
	// See [ptah.run/internal/rolepassword].
	passwords map[string]string
}

func (p ApplyPlan) HasChanges() bool {
	return len(p.units) > 0
}

// SQL projects the complete plan to SQL. Native operations refuse the whole
// projection; no executable prefix is returned.
func (p ApplyPlan) SQL() (string, error) {
	statements, err := p.Statements()
	if err != nil {
		return "", err
	}
	return annotatedSQL(statements, p.annotations), nil
}

// NamedSQL is [ApplyPlan.SQL] with the comment that names each statement's change
// above it, as the Atlas CLI writes a plan.
func (p ApplyPlan) NamedSQL() (string, error) {
	statements, err := p.Statements()
	if err != nil {
		return "", err
	}
	named := make([]string, len(statements))
	for i, statement := range statements {
		named[i] = statement
		if i < len(p.changes) {
			named[i] = atlasreport.NameStatement(p.changes[i], statement)
		}
	}
	return annotatedSQL(named, p.annotations), nil
}

// Comments names the change of each statement of [ApplyPlan.Statements], at the
// same index. Empty names no change.
func (p ApplyPlan) Comments() []string {
	return slices.Clone(p.changes)
}

// Statements returns an independent SQL projection with execution boundaries.
// A plan containing a native operation returns renderer.ErrNativeSQL.
func (p ApplyPlan) Statements() ([]string, error) {
	return (renderer.Result{Complete: true, ExecutableUnits: true, Fragments: p.units}).SQLFragments()
}

// Units returns independent captured operations in execution order.
func (p ApplyPlan) Units() ([]renderer.Fragment, error) {
	units, _, err := cloneApplyUnits(p.units)
	return units, err
}

func PlanApply(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts ApplyOptions,
) (ApplyPlan, error) {
	computation, err := computeApplyPlan(ctx, conn, opts)
	if err != nil {
		return ApplyPlan{}, err
	}
	if err := computation.undecided.Err(); err != nil {
		return ApplyPlan{}, err
	}
	return computation.executionPlan(), nil
}

// executionUnits is the DDL followed by the data. A row belongs in a
// table the same plan may be creating or altering, so the schema moves first;
// within the data, datadiff already orders inserts, updates and deletes.
func (c applyComputation) executionUnits() []renderer.Fragment {
	data := make([]renderer.Fragment, 0, len(c.dataStatements))
	for _, declared := range c.dataStatements {
		data = append(data, renderer.Fragment{SQL: declared.sql})
	}
	return slices.Insert(slices.Clone(c.units), c.dataIndex(), data...)
}

// dataIndex is where the data goes among the DDL statements: after all of
// them, except inside a SQLite rebuild's foreign-key bracket, where it goes
// ahead of the pragma that closes the bracket.
//
// After that pragma, the plan no longer ends with it, the apply opens an
// ordinary transaction in which the disabling pragma is silently ignored, and
// the rebuild's DROP either fails against a referencing row or cascades into it
// (stokaro/ptah#3282). Inside the bracket the rows run with enforcement
// suspended too, and the foreign-key check the apply runs before it commits
// still refuses a row whose reference does not resolve.
func (c applyComputation) dataIndex() int {
	statements := make([]string, 0, len(c.units))
	for _, unit := range c.units {
		if unit.Native != nil {
			return len(c.units)
		}
		statements = append(statements, unit.SQL)
	}
	return sqliteforeignkeys.AppendIndex(statements)
}

// applyComputation carries a computed schema apply plan together with the
// exclude-filtered current and desired states it was derived from, so plan
// packaging (fingerprints) reuses the exact planning inputs instead of
// re-reading the database.
type applyComputation struct {
	ApplyPlan
	// undecided are the declared objects the comparison withheld because the
	// read did not describe their kind; see [ApplyRuntimePlan.Undecided].
	undecided schemadiff.Diagnostics
	// dataStatements reconcile declared rows. They are kept apart from the DDL
	// so a plan can record the severity this package assigns them: a SQL
	// analyzer reads a DELETE of a reference row as safe, which is true about
	// the schema and false about the row.
	dataStatements []dataStatement
	current        *catalog.Database
	desired        *schemamodel.Database
	// readScope is the schema allow-list current was read at, nil when the read
	// was the connection's own default.
	readScope []string
	// schemasBeyondURL is the part of readScope the connection URL's own scope
	// does not cover when read again later; see [schemascope.BeyondURL]. A saved
	// plan records it, and [VerifyPlanTarget] reads it together with the URL's
	// scope, so the verification read covers every schema the planning read did.
	schemasBeyondURL []string
	// rowSets names the declared row sets the data stage read. A saved plan
	// records them and a digest of the rows they held, and [VerifyPlanTarget]
	// reads them again, so a row that moved after planning makes the plan stale
	// the way a moved column does.
	rowSets []PlanRowSet
}

// PreflightApplyTarget validates the target state before an apply lock is
// acquired or a desired migration directory is replayed. An explicit --schema
// remains authoritative. Without one, PostgreSQL-family targets inventory the
// user realm because desired-state replay may add schemas beyond a URL-pinned
// search_path; other dialects retain their normal URL-derived read scope. Nil
// validators return without reading the database, preserving the full/default
// compatibility path exactly.
//
// Apply planning validates the target again while the lock is held. The second
// check is deliberate: this preflight establishes before-work error ordering,
// while the locked check prevents a catalog change between preflight and plan
// construction from bypassing the caller's policy.
func PreflightApplyTarget(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schemas []string,
	validateSchema func(*schemamodel.Database) error,
	validateLiveObject func(LiveSchemaObject) error,
	runtime dbschematogo.Runtime,
) error {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return err
	}
	if validateSchema == nil && validateLiveObject == nil {
		return nil
	}
	if conn == nil {
		return errors.New("schema apply target preflight requires database connection")
	}
	readScope, err := preflightApplyReadScope(ctx, conn, schemas)
	if err != nil {
		return fmt.Errorf("read database schema: %w", err)
	}
	current, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, readScope)
	if err != nil {
		return fmt.Errorf("read database schema: %w", err)
	}
	if err := validateCurrentApplySchema(ctx, current, conn.Info().Dialect, validateSchema, runtime); err != nil {
		return err
	}
	return ValidateLiveObjects(conn, readScope, validateLiveObject)
}

func preflightApplyReadScope(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schemas []string,
) ([]string, error) {
	if requested := schemascope.SplitNames(schemas); len(requested) > 0 {
		return requested, nil
	}
	// A search-path engine's realm is every schema of the database, which
	// its catalog lists; another engine reads the schemas its URL scope names.
	if described(conn.Info().Dialect).Schemas.Selection == descriptor.SchemaSelectionSearchPath {
		return schemaselection.RealmSchemas(ctx, conn.Info().Dialect, conn.Info().Capabilities, conn)
	}
	return schemascope.ReadNames(ctx, conn.Info(), nil, conn)
}

func computeApplyPlan(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts ApplyOptions,
) (applyComputation, error) {
	if err := schemaext.RequireRuntime(ctx, opts.Runtime); err != nil {
		return applyComputation{}, err
	}
	if err := validateApplyPlanningInputs(conn, opts); err != nil {
		return applyComputation{}, err
	}
	ctx = readscope.Without(ctx, atlasfilter.KindsLeftOut(opts.Runtime.Selections(), opts.Exclude, opts.Include)...)
	allowUnmatched, err := resolveApplyAllowUnmatched(opts)
	if err != nil {
		return applyComputation{}, err
	}

	scope := atlasfilter.Scope{
		Schemas:               opts.Schemas,
		Include:               opts.Include,
		Exclude:               opts.Exclude,
		DefaultSchema:         conn.Info().Schema,
		RealmRelativePatterns: ConnectionIsRealmScoped(conn.Info()),
		Selections:            opts.Runtime.Selections(),
	}
	desired, err := loadAndValidateDesiredApplySchema(ctx, conn, opts)
	if err != nil {
		return applyComputation{}, err
	}
	read, err := applyCurrentState(ctx, conn, opts.Schemas, desired)
	if err != nil {
		return applyComputation{}, err
	}
	current := opts.baseline.WithoutEnvironment(read.current, devclean.DeclaredExtensions(desired))
	declared, err := goschematodb.ToDBSchema(ctx, desired, conn.Info().Dialect, opts.Runtime)
	if err != nil {
		return applyComputation{}, err
	}
	current, err = opts.baseline.WithoutStartingPoint(ctx, current, declared, defaultSchemaOf(conn.Info()), opts.Runtime.Selections())
	if err != nil {
		return applyComputation{}, err
	}
	if err := validateCurrentApplyState(ctx, conn, current, read.readScope, opts); err != nil {
		return applyComputation{}, err
	}
	selectionOpts := opts
	selectionOpts.RefuseUnmatchedExclude = opts.RefuseUnmatchedExclude && !allowUnmatched
	current, desired, err = selectApplyStates(ctx, conn.Info().Dialect, current, desired, scope, selectionOpts)
	if err != nil {
		return applyComputation{}, err
	}

	basePlan, err := captureApplyTarget(conn.Info())
	if err != nil {
		return applyComputation{}, err
	}
	computation := applyComputation{
		ApplyPlan:        basePlan,
		current:          current,
		desired:          desired,
		readScope:        read.readScope,
		schemasBeyondURL: read.schemasBeyondURL,
	}
	info := conn.Info()
	// The comparison is told what the diff policy will do to its answer; see
	// [DiffPolicy.CompareOptions].
	compareOpts := opts.Policy.CompareOptions()
	withSecretRotation(compareOpts, opts.Policy)
	compared, undecided, err := schemadiff.CompareWithDatabaseReportingUndecidedAdditions(ctx, conn, desired, current, compareOpts, opts.Runtime)
	if err != nil {
		return applyComputation{}, fmt.Errorf("compare database schema: %w", comparisonRefusal(err))
	}
	// No statement is planned for a withheld object, so a plan that says
	// nothing about it quietly does less than the desired schema asks for. The
	// wording names neither surface's flags: native `schema apply` and the
	// compatibility one both come through here.
	undecidednote.Report(ctx, opts.Diagnostics, undecided, "the database", "the desired schema")
	// The plan leaves an object of a kind Ptah does not model where it is, so an
	// empty plan says nothing about it.
	unmodelednote.Report(ctx, opts.Diagnostics, "the database", current)
	computation.undecided = undecided
	diff, omitted := ApplyDiffPolicy(compared, opts.Policy, info.Dialect)
	// After the policy, which deletes the table drops a configuration skips:
	// a drop the plan will not run is not one to warn about.
	if opts.WarnUndescribedSchemas {
		undescribed, err := schemascope.UndescribedRemovals(info, opts.Schemas, desired, diff)
		if err != nil {
			return applyComputation{}, fmt.Errorf("compare database schema: %w", err)
		}
		schemascope.WarnUndescribed(opts.Diagnostics, undescribed)
	}
	// A skipped object is not a change, so a plan with nothing else to run
	// has no statement to carry its comment; the line goes beside the
	// result instead, so "synced" never stands alone over it. A change the
	// diff policy left out is reported the same way.
	if !diff.HasChanges() {
		ReportOmitted(opts.Diagnostics, omitted)
		reportSkipped(opts.Diagnostics, diff.SkippedFeatures)
	}
	if diff.HasChanges() {
		// The planner is handed the comparison as it came back, with the
		// kinds the policy skips: it removes the same changes the filter
		// above did, and the plan opens with a comment for each of them, as
		// a generated migration does. Everything above decided on that
		// filter's answer.
		planOptions := planext.Options{
			Capabilities:         info.Capabilities,
			ConcurrentIndexes:    opts.Policy.ConcurrentIndexCreate,
			OnlineAlter:          opts.Policy.OnlineAlter,
			ConcurrentIndexDrops: opts.Policy.ConcurrentIndexDrop,
			ConcurrentIndexRefs: declaredConcurrentIndexRefs(
				opts.Policy, diff, desired, current, info.Dialect, info.Capabilities,
			),
			OmitNullBackfill:    opts.OmitNullBackfill,
			AllowTableRebuild:   opts.Policy.AllowTableRebuild,
			TableRebuildRequest: opts.Policy.TableRebuildRequest,
			SkipChangeKinds:     opts.Policy.SkipChangeKinds,
			Statements:          planstyle.Statements,
			Shape:               opts.Shape,
		}
		plan, err := planner.GenerateSchemaDiffRenderedPlan(
			ctx, opts.Runtime,
			compared, info.Dialect, planOptions,
		)
		if err != nil {
			return applyComputation{}, fmt.Errorf("generate schema apply operations: %w", err)
		}
		if err := computation.captureRenderedUnits(ctx, opts.Runtime, info, plan); err != nil {
			return applyComputation{}, err
		}
	}
	// The data stage runs whether or not the schema changed. A release that
	// only edits a reference row changes no DDL, and a planner that returned
	// here would report an empty plan for a change its author made.
	computation.dataStatements, computation.rowSets, err = plannedData(ctx, conn, opts, desired, current)
	if err != nil {
		return applyComputation{}, err
	}
	return computation, nil
}

// plannedData is the data stage of a plan: the statements that reconcile the
// declared rows, and the row sets they read, or none for a plan computed
// [ApplyOptions.withoutData].
func plannedData(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts ApplyOptions,
	desired *schemamodel.Database,
	current *catalog.Database,
) ([]dataStatement, []PlanRowSet, error) {
	if opts.withoutData {
		return nil, nil, nil
	}
	return managedDataStatements(ctx, conn, opts.Runtime, desired, current, opts.ProjectRoots, protectedtable.New(opts.ProtectedTables))
}

// selectApplyStates checks selectors against both sides before planning. One
// empty side is a create or removal; two empty selections cannot prove agreement.
// A standalone feature object is kept or left out whole by the tables it binds
// on either side, and a selection that would split one is refused.
func selectApplyStates(ctx context.Context, dialect string, current *catalog.Database, desired *schemamodel.Database, scope atlasfilter.Scope, opts ApplyOptions) (*catalog.Database, *schemamodel.Database, error) {
	scoped, err := projectLiveStates(ctx, opts.Runtime, dialect, current, desired, scope)
	if err != nil {
		return nil, nil, err
	}
	current, currentReports, currentErr := scoped.current, scoped.currentReports, scoped.currentErr
	desired, desiredReports, desiredErr := scoped.desired, scoped.desiredReports, scoped.desiredErr
	// An --exclude selector that named nothing in the database and nothing in
	// the desired state protected nothing, and apply is the verb that carries
	// the plan out. Refusing is the safe answer there; the opt-in named in the
	// message restores the permissive one. Callers that only compute a plan
	// say so instead.
	unmatched := atlasfilter.UnmatchedAcrossStates(currentReports.Exclude, desiredReports.Exclude)
	if opts.RefuseUnmatchedExclude {
		if err := refuseUnmatchedExclude(unmatched); err != nil {
			return nil, nil, err
		}
	} else {
		reportUnmatchedExclude(opts.Diagnostics, unmatched)
	}
	// An --include selection that matched neither the database nor the desired
	// state leaves nothing to apply. Reported as a synced schema it is a verb
	// claiming success for work it did not do, with the target untouched, so
	// schema apply refuses instead. One empty side is left alone: that is what
	// a pure create or a pure drop looks like.
	if emptySelection(currentErr) && emptySelection(desiredErr) {
		return nil, nil, fmt.Errorf(
			"%w; schema apply would change nothing",
			currentErr,
		)
	}
	applyExtensionSupportCoverage(desired, currentReports.Selection, desiredReports.Selection)

	return current, desired, nil
}

func validateApplyPlanningInputs(conn *dbschema.DatabaseConnection, opts ApplyOptions) error {
	if conn == nil {
		return errors.New("schema apply planning requires database connection")
	}
	if len(opts.ToURLs) == 0 && opts.Desired == nil {
		return errors.New("schema apply planning requires desired schema URLs")
	}
	// Resolve this before the desired state is loaded and before the target is
	// read. A malformed value therefore runs nothing and writes nothing.
	return sqlitevirtual.ValidateToggle(conn.Info().Dialect)
}

type scopedApplyStates struct {
	current        *catalog.Database
	desired        *schemamodel.Database
	currentReports atlasfilter.ScopeReports
	desiredReports atlasfilter.ScopeReports
	currentErr     error
	desiredErr     error
}

// projectLiveStates captures the bindings of both sides of a comparison with a
// live read and projects both through scope. An empty selection is not an error
// here: it stays in the result, for the caller that can see both sides.
func projectLiveStates(
	ctx context.Context,
	runtime any,
	dialect string,
	current *catalog.Database,
	desired *schemamodel.Database,
	scope atlasfilter.Scope,
) (scopedApplyStates, error) {
	var err error
	scope.Bindings, err = scopeBindings(ctx, runtime, dialect, scope, databaseSide(current), generatedSide(desired))
	if err != nil {
		return scopedApplyStates{}, err
	}
	scoped := scopeApplyStates(current, desired, scope)
	if scoped.currentErr != nil && !emptySelection(scoped.currentErr) {
		return scopedApplyStates{}, scoped.currentErr
	}
	if scoped.desiredErr != nil && !emptySelection(scoped.desiredErr) {
		return scopedApplyStates{}, scoped.desiredErr
	}
	return scoped, nil
}

func scopeApplyStates(
	current *catalog.Database,
	desired *schemamodel.Database,
	scope atlasfilter.Scope,
) scopedApplyStates {
	project := func(scope atlasfilter.Scope) scopedApplyStates {
		projectedCurrent, currentReports, currentErr := scopeDatabaseSide(current, scope, "current schema")
		projectedDesired, desiredReports, desiredErr := scopeGeneratedSide(desired, scope, "desired schema")
		return scopedApplyStates{
			current:        projectedCurrent,
			desired:        projectedDesired,
			currentReports: currentReports,
			desiredReports: desiredReports,
			currentErr:     currentErr,
			desiredErr:     desiredErr,
		}
	}

	result := project(scope)
	if result.currentErr != nil && !emptySelection(result.currentErr) ||
		result.desiredErr != nil && !emptySelection(result.desiredErr) {
		return result
	}
	supportScope, changed := extensionSupportScope(
		scope,
		result.currentReports.Selection,
		result.desiredReports.Selection,
	)
	if !changed {
		return result
	}
	return project(supportScope)
}

func validateCurrentApplyState(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	current *catalog.Database,
	readScope []string,
	opts ApplyOptions,
) error {
	if err := validateCurrentApplySchema(ctx, current, conn.Info().Dialect, opts.ValidateCurrentSchema, opts.Runtime); err != nil {
		return err
	}
	return ValidateLiveObjects(conn, readScope, opts.ValidateLiveObject)
}

func resolveApplyAllowUnmatched(opts ApplyOptions) (bool, error) {
	if !opts.RefuseUnmatchedExclude {
		return false, nil
	}
	return atlasfilter.AllowUnmatchedExclude()
}

func validateCurrentApplySchema(
	ctx context.Context,
	current *catalog.Database,
	dialect string,
	validate func(*schemamodel.Database) error,
	runtime dbschematogo.Runtime,
) error {
	if validate == nil {
		return nil
	}
	desired, err := dbschematogo.ConvertDBSchemaToGoSchema(ctx, current, dialect, runtime)
	if err != nil {
		return err
	}
	return validate(desired)
}

// applyRead is the database side of an apply together with the scopes it was
// read at. [planSourceSchema] needs both scopes in order to fingerprint a state
// [VerifyPlanTarget] can recompute.
type applyRead struct {
	current *catalog.Database
	// readScope is the schema allow-list current was read at.
	readScope []string
	// schemasBeyondURL is the part of readScope the URL alone will not cover
	// when read again; see [schemascope.BeyondURL].
	schemasBeyondURL []string
}

// applyCurrentState reads the database side of an apply.
func applyCurrentState(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	requested []string,
	desired *schemamodel.Database,
) (applyRead, error) {
	urlScope, err := schemascope.ReadNames(ctx, conn.Info(), nil, conn)
	if err != nil {
		return applyRead{}, fmt.Errorf("read database schema: %w", err)
	}
	readScope, err := schemascope.ComparisonScope(conn.Info(), requested, urlScope, desired)
	if err != nil {
		return applyRead{}, fmt.Errorf("read database schema: %w", err)
	}
	current, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, readScope)
	if err != nil {
		return applyRead{}, fmt.Errorf("read database schema: %w", errorkind.TargetRead(err))
	}
	return applyRead{
		current:          current,
		readScope:        readScope,
		schemasBeyondURL: schemascope.BeyondURL(conn.Info(), urlScope, readScope),
	}, nil
}

// comparisonRefusal marks a refusal the comparison completed as a refused
// declaration, and returns any other error as it came.
//
// The mark belongs here because this comparison runs after the target was read,
// which is what a refusal code promises. Both types below are raised elsewhere,
// and `schema diff` raises them over two documents with no target at all, so a
// report that read them directly could break that promise. An error the
// comparison could not complete, such as a provider that failed to answer, is
// neither type.
func comparisonRefusal(err error) error {
	_, declaration := errors.AsType[*schemadiff.RefusalError](err)
	_, validation := errors.AsType[*schemavalidation.RefusalError](err)
	if declaration || validation {
		return &planrefusal.DeclarationError{Err: err}
	}
	return err
}

// loadDesiredApplySchema materializes the desired schema for apply planning.
// The resolver accepts local schema files (unchanged pre-resolver behavior),
// database URLs, migration directories replayed on the dev database, and
// env:// references; LocalFilesOnly pins the legacy local-file-only path.
func loadDesiredApplySchema(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts ApplyOptions,
) (*schemamodel.Database, error) {
	if opts.Desired != nil {
		bound, err := valuecontext.Source(ctx, opts.Runtime, conn.Info().Dialect, conn.Info().URL, opts.Desired)
		if err != nil {
			return nil, err
		}
		return validateDesiredApplySchema(bound, opts.ValidateDesiredSchema)
	}
	// Both the dev database and the target can limit an apply to one schema, and
	// the target's URL is the one this connection was opened from.
	schemaScope, schemaScopeFlag := schemafile.ScopeFromURLs(opts.DevURL, conn.Info().URL, "url")
	if opts.LocalFilesOnly {
		desired, err := schemafile.LoadSources(ctx, localApplySources(opts), schemafile.Options{
			Contexts:                 opts.Runtime,
			YAML:                     opts.Runtime.YAML(),
			SQL:                      opts.Runtime.SQL(),
			HCL:                      opts.Runtime.HCL(),
			CoverageVocabulary:       opts.Runtime.CoverageVocabulary(),
			Relations:                featureselect.RelationsOf(opts.Runtime),
			DatabaseURL:              conn.Info().URL,
			Dialect:                  conn.Info().Dialect,
			IgnoreUnknownHCLNames:    opts.IgnoreUnknownHCLNames,
			ReportIgnored:            opts.Diagnostics,
			SchemaScope:              schemaScope,
			SchemaScopeFlag:          schemaScopeFlag,
			RequestedSchemas:         opts.Schemas,
			RefuseUnrequestedSchemas: opts.RefuseUnrequestedSchemas,
			Vars:                     opts.Vars,
		})
		if err != nil {
			return nil, err
		}
		return validateDesiredApplySchema(desired, opts.ValidateDesiredSchema)
	}
	var set atlassource.Set
	if opts.PreparedTo != nil {
		set = *opts.PreparedTo
	} else {
		var err error
		set, err = atlassource.ClassifySet("--to", opts.ToURLs, opts.ProjectEnv, dbschema.NativeSources(ctx))
		if err != nil {
			return nil, err
		}
	}
	// Before the set is resolved, because a migration directory replays on the
	// dev server as it resolves.
	if err := refuseSQLBesideOneDatabaseOnDevServer(conn.Info(), opts.DevURL, set); err != nil {
		return nil, err
	}
	state, err := set.Resolve(ctx, atlassource.ResolveOptions{
		Runtime:           opts.Runtime,
		DatabaseURL:       conn.Info().URL,
		Dialect:           conn.Info().Dialect,
		DialectFlag:       "--url",
		DialectFromServer: true,
		// A --to database naming no database is read as the whole server, so
		// that a server applies to a server; against one database it is
		// refused below.
		ServerScope: true,
		DevURL:      opts.DevURL,
		// A --to migration directory replays on the dev database, and the
		// replay resets it first: the dev database must not be the target, or
		// a database --to also names.
		Protected: append(
			[]devlock.Protected{{Conn: conn, Refusal: errReplayDevURLIsTarget}},
			set.DevProtected()...,
		),
		DevServerDisposable:       opts.DevServerDisposable,
		SchemaScope:               schemaScope,
		SchemaScopeFlag:           schemaScopeFlag,
		RequestedSchemas:          opts.Schemas,
		RefuseUnrequestedSchemas:  opts.RefuseUnrequestedSchemas,
		IgnoreUnknownHCLNames:     opts.IgnoreUnknownHCLNames,
		ReportIgnored:             opts.Diagnostics,
		Vars:                      opts.Vars,
		ValidateSchema:            opts.ValidateDesiredSchema,
		ValidateInspectedSchema:   opts.ValidateCurrentSchema,
		ValidateInspectedDatabase: LiveDatabaseValidator(opts.ValidateLiveObject),
		ValidateMigrationSource:   opts.ValidateMigrationSource,
		ValidateLocalSchemaSource: opts.ValidateLocalSchemaSource,
	})
	if err != nil {
		return nil, err
	}
	if err := refuseServerScopeMismatch(connectionSide(conn.Info()), stateSide(state)); err != nil {
		return nil, err
	}
	if len(state.EnvironmentExtensions) > 0 {
		installed, err := installedExtensionNames(ctx, conn)
		if err != nil {
			return nil, err
		}
		// A directory replayed on the dev database reads back that
		// database's own extensions too. One the target lacks is not the
		// directory's to create there.
		state = state.WithoutEnvironment(installed)
	}
	if state.EnvironmentState != nil {
		// So is a docker block's starting point: what the target holds of it
		// matches, and the rest is not the directory's to create there.
		target, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, catalogSchemaNames(state.EnvironmentState))
		if err != nil {
			return nil, fmt.Errorf("read target schema: %w", err)
		}
		state, err = state.WithoutStartingPoint(ctx, atlassource.State{DB: target}, conn.Info().Dialect, opts.Runtime)
		if err != nil {
			return nil, err
		}
	}
	return state.Schema, nil
}

// catalogSchemaNames returns the names of the schemas database holds.
func catalogSchemaNames(database *catalog.Database) []string {
	names := make([]string, 0, len(database.Schemas))
	for _, schema := range database.Schemas {
		names = append(names, schema.Name)
	}
	return names
}

// extensionInstaller is the writer that names the extensions its database
// holds.
type extensionInstaller interface {
	InstalledExtensions(context.Context) ([]string, error)
}

// installedExtensionNames is the set of extensions conn's database holds, or
// none on a dialect without extensions.
func installedExtensionNames(ctx context.Context, conn *dbschema.DatabaseConnection) (map[string]bool, error) {
	names := make(map[string]bool)
	lister, ok := conn.SchemaWriter().(extensionInstaller)
	if !ok {
		return names, nil
	}
	installed, err := lister.InstalledExtensions(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the target's extensions: %w", err)
	}
	for _, name := range installed {
		names[name] = true
	}
	return names, nil
}

// localApplySources is the desired state of the LocalFilesOnly path, as
// sources rather than URLs.
//
// ToSources wins when the caller supplied it, because it is the same list with
// each source's atlas.hcl variable scope attached; ToURLs is the fallback for
// every caller that never had a project file. The two are never merged: a
// caller that knows about scopes hands over the whole list.
func localApplySources(opts ApplyOptions) []schemafile.Source {
	if len(opts.ToSources) > 0 {
		return opts.ToSources
	}
	sources := make([]schemafile.Source, 0, len(opts.ToURLs))
	for _, rawURL := range opts.ToURLs {
		sources = append(sources, schemafile.Source{URL: rawURL})
	}
	return sources
}

func loadAndValidateDesiredApplySchema(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts ApplyOptions,
) (*schemamodel.Database, error) {
	desired, err := loadDesiredApplySchema(ctx, conn, opts)
	if _, mismatch := errors.AsType[*ServerScopeMismatchError](err); mismatch {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("load --to schema: %w", err)
	}
	if err := schemascope.RefuseOutsideTargetDatabase(conn.Info(), desired); err != nil {
		return nil, err
	}
	if err := pgsystemschema.ValidateDeclaredPostgresSystemSchemas(
		conn.Info().Dialect,
		desired.Schemas,
	); err != nil {
		return nil, fmt.Errorf("validate --to schema: %w", err)
	}
	return desired, nil
}

// desiredApplySet classifies the --to sources the way the load will, for a
// refusal that has to be decided before anything is contacted. A source the
// load would refuse classifies as nothing, and the load reports it.
func desiredApplySet(ctx context.Context, opts ApplyRuntimeOptions) atlassource.Set {
	if opts.PreparedTo != nil {
		return *opts.PreparedTo
	}
	if opts.Desired != nil {
		return atlassource.Set{}
	}
	set, err := atlassource.ClassifySet("--to", opts.ToURLs, opts.ProjectEnv, dbschema.NativeSources(ctx))
	if err != nil {
		return atlassource.Set{}
	}
	return set
}

// PrepareApply validates Atlas schema apply runtime inputs and builds the
// executable apply plan for the already-open target database connection.
func PrepareApply(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts ApplyRuntimeOptions,
) (ApplyRuntimePlan, error) {
	if conn == nil {
		return ApplyRuntimePlan{}, errors.New("schema apply requires database connection")
	}
	if err := providerurl.ValidateDialectMatch(ctx, opts.DevURL, conn.Info().Dialect); err != nil {
		return ApplyRuntimePlan{}, err
	}
	if err := RefuseServerDevDatabase(ctx, conn.Info(), opts.DevURL, desiredApplySet(ctx, opts), opts.Runtime.HCL()); err != nil {
		return ApplyRuntimePlan{}, err
	}

	planning := ApplyOptions{
		Runtime:                   opts.Runtime,
		Shape:                     opts.Shape,
		ProjectRoots:              opts.ProjectRoots,
		ToURLs:                    opts.ToURLs,
		Exclude:                   opts.Exclude,
		Schemas:                   opts.Schemas,
		Include:                   opts.Include,
		Policy:                    opts.Policy,
		ProtectedTables:           opts.ProtectedTables,
		DevURL:                    opts.DevURL,
		DevServerDisposable:       opts.DevServerDisposable,
		ProjectEnv:                opts.ProjectEnv,
		PreparedTo:                opts.PreparedTo,
		Desired:                   opts.Desired,
		ValidateDesiredSchema:     opts.ValidateDesiredSchema,
		ValidateCurrentSchema:     opts.ValidateCurrentSchema,
		ValidateLiveObject:        opts.ValidateLiveObject,
		ValidateMigrationSource:   opts.ValidateMigrationSource,
		ValidateLocalSchemaSource: opts.ValidateLocalSchemaSource,
		OmitNullBackfill:          opts.OmitNullBackfill,

		Diagnostics:              opts.Diagnostics,
		WarnUndescribedSchemas:   opts.WarnUndescribedSchemas,
		RefuseUnmatchedExclude:   true,
		IgnoreUnknownHCLNames:    opts.IgnoreUnknownHCLNames,
		RefuseUnrequestedSchemas: opts.RefuseUnrequestedSchemas,
		Vars:                     opts.Vars,
	}
	computation, err := computeApplyPlan(ctx, conn, planning)
	if err != nil {
		return ApplyRuntimePlan{}, err
	}
	if err := refuseOnlineAlterInOneTransaction(opts.Policy, opts.TxMode, conn.Info().Dialect); err != nil {
		return ApplyRuntimePlan{}, err
	}
	passwords, err := rolepassword.Declared(computation.desired)
	if err != nil {
		return ApplyRuntimePlan{}, err
	}
	return ApplyRuntimePlan{
		passwords: passwords,
		runtime:   opts.Runtime,
		plan:      computation.executionPlan(),
		dryRun:    opts.DryRun,
		conn:      conn,
		txMode:    opts.TxMode,
		current:   computation.current,
		undecided: computation.undecided,
		rehearsal: nativeRehearsal{
			planning:   planning,
			desiredFor: opts.DesiredFor,
			schema:     computation.ApplyPlan,
			data:       computation.dataStatements,
		},
	}, nil
}

// refuseOnlineAlterInOneTransaction refuses an online apply whose statements
// would share a transaction on the PostgreSQL family.
//
// The pair this policy generates -- ADD CONSTRAINT ... NOT VALID and the
// VALIDATE CONSTRAINT that completes it -- buys its weaker lock only when the
// two statements commit separately. In one transaction the ACCESS EXCLUSIVE
// lock the addition takes is held to commit, so the validation's scan runs
// behind it and writers wait for the whole scan: the apply would do more work
// than the plain statement and block for just as long.
//
// Refused rather than silently downgraded, because the policy is a promise
// about what the apply does to a live database and an apply that quietly did
// not keep it is the outcome the policy exists to prevent.
func refuseOnlineAlterInOneTransaction(
	policy DiffPolicy,
	txMode migrator.MigrationTxMode,
	dialect string,
) error {
	if !policy.OnlineAlter || described(dialect).OnlineAlter != descriptor.OnlineAlterNotValid {
		return nil
	}
	if txMode == migrator.MigrationTxModeNone {
		return nil
	}
	return fmt.Errorf(
		"online alter needs each statement to commit on its own on %s: a constraint added NOT VALID "+
			"and validated in the same transaction holds the addition's ACCESS EXCLUSIVE lock through "+
			"the validation scan. Apply with --tx-mode none, or turn diff.online_alter off",
		dialect,
	)
}

func validateDesiredApplySchema(
	desired *schemamodel.Database,
	validator func(*schemamodel.Database) error,
) (*schemamodel.Database, error) {
	if validator != nil {
		if err := validator(desired); err != nil {
			return nil, err
		}
	}
	return desired, nil
}

func (p ApplyRuntimePlan) HasChanges() bool {
	return p.plan.HasChanges()
}

func (p ApplyRuntimePlan) SQL() (string, error) {
	return p.plan.SQL()
}

// NamedSQL is [ApplyPlan.NamedSQL].
func (p ApplyRuntimePlan) NamedSQL() (string, error) {
	return p.plan.NamedSQL()
}

// Comments is [ApplyPlan.Comments].
func (p ApplyRuntimePlan) Comments() []string {
	return p.plan.Comments()
}

func (p ApplyRuntimePlan) Statements() ([]string, error) {
	return p.plan.Statements()
}

// Undecided returns the declared objects the comparison withheld: the read of
// the target did not describe their kind, so nothing checked whether they
// exist, and no statement in the plan creates them. They are sorted by kind and
// then name. A plan with no statements and a non-empty Undecided has not shown
// that the target matches the desired schema.
func (p ApplyRuntimePlan) Undecided() schemadiff.Diagnostics {
	return p.undecided.Clone()
}

// Units returns the complete prepared batch with independent native payloads.
func (p ApplyRuntimePlan) Units() ([]renderer.Fragment, error) { return p.plan.Units() }

// Assessments judges the captured execution units without another rendering
// pass. Owner facts remain aligned with managed-data operations and SQL.
func (p ApplyRuntimePlan) Assessments(ctx context.Context, runtime safety.Runtime) ([]safety.StatementAssessment, error) {
	if p.conn == nil {
		return nil, errors.New("schema apply assessment requires database connection")
	}
	return safety.AssessUnits(ctx, runtime, p.conn.Info().Dialect, p.plan.units, p.plan.owned)
}

// HasNativeUnits reports whether SQL projection would discard an operation.
func (p ApplyRuntimePlan) HasNativeUnits() bool {
	return slices.ContainsFunc(p.plan.units, func(unit renderer.Fragment) bool { return unit.Native != nil })
}

// HasInputReferences reports whether the captured plan approves inputs whose
// references must remain attached to their execution units.
func (p ApplyRuntimePlan) HasInputReferences() bool {
	return slices.ContainsFunc(p.plan.units, func(unit renderer.Fragment) bool { return len(unit.Inputs) != 0 })
}

// Preflight checks the complete captured batch and its transaction requirements
// without opening a transaction or writing to the database.
func (p ApplyRuntimePlan) Preflight(ctx context.Context) error {
	return p.plan.Preflight(ctx, p.conn, p.txMode)
}

// Execute applies the prepared operations under the captured transaction mode.
// Dry runs validate the whole batch without changing the database.
func (p ApplyRuntimePlan) Execute(ctx context.Context) error {
	ctx = rolepassword.With(ctx, p.passwords)
	if err := p.Preflight(ctx); err != nil {
		return err
	}
	if !p.HasChanges() || p.dryRun {
		return nil
	}
	p.conn.SchemaWriter().SetDryRun(false)
	return p.plan.Execute(ctx, p.conn, p.txMode)
}

func ApplySQL(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	txMode migrator.MigrationTxMode,
	sqlText string,
) error {
	if conn == nil {
		return errors.New("schema apply execution requires database connection")
	}

	return applyStatements(ctx, conn, txMode, SplitApplyStatements(sqlText, conn.Info().Dialect))
}

// ApplyStatements executes an already-split ordered statement list under
// txMode. Callers that verified a specific statement list must execute that
// same list instead of re-splitting SQL text, so what was checked is what
// runs.
func ApplyStatements(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	txMode migrator.MigrationTxMode,
	statements []string,
) error {
	if conn == nil {
		return errors.New("schema apply execution requires database connection")
	}

	return applyStatements(ctx, conn, txMode, statements)
}

// applyStatements executes the ordered statements on conn under txMode. It is
// shared by the target apply and the dev database simulation, so both run the
// exact same ordered plan through the same execution path.
func applyStatements(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	txMode migrator.MigrationTxMode,
	statements []string,
) error {
	executable, positions := executableStatements(statements, conn.Info().Dialect)
	if err := applyUnits(ctx, conn, txMode, renderer.SQLFragments(executable)); err != nil {
		return &listedStatementsError{err: err, positions: positions, total: len(statements)}
	}
	return nil
}

// executableStatements returns the statements that hold something to send, and
// the 1-based position of each among the statements it was given. A plan file
// written by hand can list a statement that holds only comments.
func executableStatements(statements []string, dialect string) ([]string, []int) {
	filtered := make([]string, 0, len(statements))
	positions := make([]int, 0, len(statements))
	for i, statement := range statements {
		if strings.TrimSpace(sqlutil.StripCommentsForDialect(statement, dialect)) != "" {
			filtered = append(filtered, statement)
			positions = append(positions, i+1)
		}
	}
	return filtered, positions
}

// listedStatementsError is a failure of a run over the list of statements it was
// given, some of which it may have left out because they hold only comments. The
// report lists every statement, so a position it gives has to count them. It adds
// nothing to the message.
type listedStatementsError struct {
	err       error
	positions []int
	total     int
}

func (e *listedStatementsError) Error() string { return e.err.Error() }

func (e *listedStatementsError) Unwrap() error { return e.err }

// position is the position among the listed statements of the statement at the
// 1-based position executed among the ones that ran. Zero names no statement.
func (e *listedStatementsError) position(executed int) int {
	if executed < 1 || executed > len(e.positions) {
		return 0
	}
	return e.positions[executed-1]
}

func SplitApplyStatements(sqlText, dialect string) []string {
	statements := sqlutil.SplitSQLStatementsForDialect(sqlText, dialect)
	filtered := statements[:0]
	for _, stmt := range statements {
		stmt = strings.TrimSpace(sqlutil.StripCommentsForDialect(stmt, dialect))
		if stmt != "" {
			filtered = append(filtered, stmt)
		}
	}
	return filtered
}

func FormatMigrationSQL(statements []string) string {
	var out strings.Builder
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmt = strings.TrimSuffix(stmt, ";")
		out.WriteString(stmt)
		out.WriteString(sqlscript.Terminator(stmt))
		out.WriteString("\n")
	}
	return out.String()
}

func executeApplyStatements(ctx context.Context, executor catalog.SchemaExecutor, statements []string) error {
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		err := checkStatement(ctx, executor, stmt)
		if err == nil {
			dispatchwitness.Sending(ctx)
			err = executor.ExecuteSQL(ctx, stmt)
		}
		if err != nil {
			return statementFailure(stmt, i, len(statements), err)
		}
	}
	return nil
}

// statementChecker is an executor that can refuse a statement without sending
// it, the way its ExecuteSQL would before anything reached the server.
type statementChecker interface {
	CheckStatement(ctx context.Context, statement string) error
}

// CheckApplyStatements answers whether a run of the statements would stop at one of
// them before sending it, in the words the run would use, and sends nothing. A dry
// run asks it because it never reaches the executor that asks for itself. ctx carries
// what the statements read, as the run's own does.
func CheckApplyStatements(ctx context.Context, conn *dbschema.DatabaseConnection, statements []string) error {
	if conn == nil {
		return errors.New("schema apply execution requires database connection")
	}
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := checkStatement(ctx, conn.Writer(), stmt); err != nil {
			return statementFailure(stmt, i, len(statements), err)
		}
	}
	return nil
}

// statementFailure is the error of the statement at index i of total.
func statementFailure(stmt string, i, total int, err error) error {
	return &migrator.MigrationExecutionError{
		Err:            fmt.Errorf("failed to execute SQL statement: %w", err),
		Statement:      stmt,
		StatementIndex: i + 1,
		Total:          total,
	}
}

// checkStatement asks an executor that can answer whether it would refuse stmt
// before sending it, so a refusal of the first statement does not read as a run
// that reached the database.
func checkStatement(ctx context.Context, executor catalog.SchemaExecutor, stmt string) error {
	if checked, ok := executor.(statementChecker); ok {
		return checked.CheckStatement(ctx, stmt)
	}
	return nil
}

// reportSkipped writes one line for each object the comparison left out
// because the target does not serve its model and its owner skips it there,
// as a plan writes it.
func reportSkipped(diagnostics io.Writer, skipped []schemaext.SkippedObject) {
	if diagnostics == nil {
		return
	}
	for _, object := range skipped {
		_, _ = fmt.Fprintln(diagnostics, strings.ToUpper(object.Target)+": "+object.Note())
	}
}

func (c applyComputation) executionPlan() ApplyPlan {
	plan := c.ApplyPlan
	plan.units, plan.transactions, plan.owned = c.executionUnits(), c.executionTransactions(), c.executionAssessments()
	plan.annotations = c.executionAnnotations()
	plan.texts = c.executionTexts()
	plan.changes = c.executionChanges()
	return plan
}

// executionChanges aligns the change names with executionUnits: a data statement
// names no change.
func (c applyComputation) executionChanges() []string {
	if len(c.changes) == 0 {
		return nil
	}
	return slices.Insert(slices.Clone(c.changes), c.dataIndex(), make([]string, len(c.dataStatements))...)
}

// executionTexts aligns the native units' texts with executionUnits: the
// data statements are SQL and have none.
func (c applyComputation) executionTexts() []string {
	if len(c.texts) == 0 {
		return nil
	}
	return slices.Insert(slices.Clone(c.texts), c.dataIndex(), make([]string, len(c.dataStatements))...)
}
