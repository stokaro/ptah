package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/internal/schemascope"
	"go.5x5.cz/ptah/internal/schemaselection"
	"go.5x5.cz/ptah/internal/sqliterebuild"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

type ApplyOptions struct {
	ToURLs  []string
	Exclude []string
	// Schemas restricts both comparison sides to the named schema scopes.
	Schemas []string
	// Include restricts both comparison sides to resources matched by
	// Atlas-style include selectors.
	Include []string
	Policy  DiffPolicy
	// DevURL is the dev database used to replay migration-directory
	// desired-state sources.
	DevURL string
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
	Desired *goschema.Database
	// Vars supplies values for HCL schema-file `variable` blocks, as `--var`
	// spells them; see [go.5x5.cz/ptah/internal/schemafile.Options].
	Vars []string
	// IgnoreUnknownHCLNames is the Atlas-compatible surface's unknown-name
	// policy; see [go.5x5.cz/ptah/internal/atlassource.ResolveOptions].
	IgnoreUnknownHCLNames bool
	// Diagnostics receives out-of-band notices, such as an --exclude selector
	// that named no object. Nil discards them.
	Diagnostics io.Writer
	// RefuseUnmatchedExclude makes an --exclude selector that named nothing in
	// either state an error rather than a notice. Only the verb that executes
	// the plan sets it: `schema apply` refuses because a selector that
	// protected nothing means the plan is free to change the object the user
	// wrote it for.
	RefuseUnmatchedExclude bool
	// ValidateDesiredSchema applies a caller-selected desired-schema policy
	// after loading and before planning. Nil accepts every modeled object.
	ValidateDesiredSchema func(*goschema.Database) error
	// ValidateCurrentSchema applies a caller-selected policy to the fully
	// introspected target before planning. Nil accepts every modeled object.
	ValidateCurrentSchema func(*goschema.Database) error
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
}

type ApplyPlan struct {
	statements []string
}

// ApplyRuntimeOptions configures Atlas schema apply planning and execution.
type ApplyRuntimeOptions struct {
	DevURL  string
	ToURLs  []string
	Exclude []string
	// Schemas restricts both comparison sides to the named schema scopes.
	Schemas []string
	// Include restricts both comparison sides to resources matched by
	// Atlas-style include selectors.
	Include []string
	Policy  DiffPolicy
	TxMode  migrator.MigrationTxMode
	DryRun  bool
	// ProjectEnv expands env:// desired-state references in ToURLs.
	ProjectEnv atlassource.ProjectEnv
	// PreparedTo is the command-bound desired source prepared before target
	// connection and lock acquisition; see [ApplyOptions.PreparedTo].
	PreparedTo *atlassource.Set
	// Desired supplies a pre-loaded desired schema model; see
	// [ApplyOptions.Desired].
	Desired *goschema.Database
	// Diagnostics receives out-of-band notices; see [ApplyOptions.Diagnostics].
	Diagnostics io.Writer
	// Vars supplies values for HCL schema-file `variable` blocks, as `--var`
	// spells them; see [go.5x5.cz/ptah/internal/schemafile.Options].
	Vars []string
	// IgnoreUnknownHCLNames is the Atlas-compatible surface's unknown-name
	// policy; see [ApplyOptions.IgnoreUnknownHCLNames].
	IgnoreUnknownHCLNames bool
	// ValidateDesiredSchema applies a caller-selected desired-schema policy;
	// see [ApplyOptions.ValidateDesiredSchema].
	ValidateDesiredSchema func(*goschema.Database) error
	// ValidateCurrentSchema applies a caller-selected current-schema policy;
	// see [ApplyOptions.ValidateCurrentSchema].
	ValidateCurrentSchema func(*goschema.Database) error
	// ValidateLiveObject applies a caller-selected supplemental catalog policy;
	// see [ApplyOptions.ValidateLiveObject].
	ValidateLiveObject func(LiveSchemaObject) error
	// ValidateMigrationSource applies a caller-selected migration-source policy;
	// see [ApplyOptions.ValidateMigrationSource].
	ValidateMigrationSource func(fs.FS) error
	// ValidateLocalSchemaSource applies a caller-selected local-source policy;
	// see [ApplyOptions.ValidateLocalSchemaSource].
	ValidateLocalSchemaSource func(string) error
}

// ApplyRuntimePlan is a prepared Atlas schema apply operation for one open
// database connection.
type ApplyRuntimePlan struct {
	plan   ApplyPlan
	dryRun bool
	conn   *dbschema.DatabaseConnection
	txMode migrator.MigrationTxMode
	// current is the filtered (schema/include scope and exclude) introspected
	// target state the plan was computed against; the dev database simulation
	// recreates it before rehearsing the plan.
	current *types.DBSchema
}

func (p ApplyPlan) HasChanges() bool {
	return len(p.statements) > 0
}

func (p ApplyPlan) SQL() string {
	return FormatMigrationSQL(p.statements)
}

func (p ApplyPlan) Statements() []string {
	return slices.Clone(p.statements)
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
	return ApplyPlan{statements: computation.statements}, nil
}

// applyComputation carries a computed schema apply plan together with the
// exclude-filtered current and desired states it was derived from, so plan
// packaging (fingerprints) reuses the exact planning inputs instead of
// re-reading the database.
type applyComputation struct {
	statements []string
	current    *types.DBSchema
	desired    *goschema.Database
	// readScope is the schema allow-list current was read at, nil when the read
	// was the connection's own default. A saved plan records no schema scope, so
	// [VerifyPlanTarget] re-reads at that default; a caller fingerprinting
	// current has to know whether the two would be the same read.
	readScope []string
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
	validateSchema func(*goschema.Database) error,
	validateLiveObject func(LiveSchemaObject) error,
) error {
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
	current, err := dbschema.ReadSchemaWithSchemas(conn, readScope)
	if err != nil {
		return fmt.Errorf("read database schema: %w", err)
	}
	if err := validateCurrentApplySchema(current, validateSchema); err != nil {
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
	if platform.IsPostgresFamily(conn.Info().Dialect) {
		return schemaselection.RealmSchemas(ctx, conn.Info().Dialect, conn)
	}
	return schemascope.ReadNames(ctx, conn.Info(), nil, conn)
}

func computeApplyPlan(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts ApplyOptions,
) (applyComputation, error) {
	if err := validateApplyPlanningInputs(conn, opts); err != nil {
		return applyComputation{}, err
	}
	allowUnmatched, err := resolveApplyAllowUnmatched(opts)
	if err != nil {
		return applyComputation{}, err
	}

	scope := atlasfilter.Scope{
		Schemas:       opts.Schemas,
		Include:       opts.Include,
		Exclude:       opts.Exclude,
		DefaultSchema: conn.Info().Schema,
	}
	desired, err := loadAndValidateDesiredApplySchema(ctx, conn, opts)
	if err != nil {
		return applyComputation{}, err
	}
	current, readScope, err := applyCurrentState(ctx, conn, opts.Schemas, desired)
	if err != nil {
		return applyComputation{}, err
	}
	if err := validateCurrentApplyState(conn, current, readScope, opts); err != nil {
		return applyComputation{}, err
	}
	scoped := scopeApplyStates(current, desired, scope)
	current, currentReports, currentErr := scoped.current, scoped.currentReports, scoped.currentErr
	if currentErr != nil && !emptySelection(currentErr) {
		return applyComputation{}, currentErr
	}
	desired, desiredReports, desiredErr := scoped.desired, scoped.desiredReports, scoped.desiredErr
	if desiredErr != nil && !emptySelection(desiredErr) {
		return applyComputation{}, desiredErr
	}
	// An --exclude selector that named nothing in the database and nothing in
	// the desired state protected nothing, and apply is the verb that carries
	// the plan out. Refusing is the safe answer there; the opt-in named in the
	// message restores the permissive one. Callers that only compute a plan
	// say so instead.
	unmatched := atlasfilter.UnmatchedAcrossStates(currentReports.Exclude, desiredReports.Exclude)
	if opts.RefuseUnmatchedExclude && !allowUnmatched {
		if err := refuseUnmatchedExclude(unmatched); err != nil {
			return applyComputation{}, err
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
		return applyComputation{}, fmt.Errorf(
			"%w; schema apply would change nothing",
			currentErr)
	}
	applyExtensionSupportCoverage(desired, currentReports.Selection, desiredReports.Selection)

	computation := applyComputation{
		current:   current,
		desired:   desired,
		readScope: readScope,
	}
	info := conn.Info()
	// The comparison is told what applyDiffPolicy will do to its answer. The
	// SQLite virtual-table guard runs inside CompareWithDatabase and refuses on
	// the statements it predicts, so without this a plan whose every drop the
	// policy deletes was refused before the policy could delete it
	// (stokaro/ptah#1028).
	compareOpts := config.DefaultCompareOptions()
	compareOpts.SkipTableDrops = opts.Policy.SkipDropTable
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, desired, current, compareOpts)
	if err != nil {
		return applyComputation{}, fmt.Errorf("compare database schema: %w", err)
	}
	diff = applyDiffPolicy(diff, opts.Policy)
	if !diff.HasChanges() {
		return computation, nil
	}

	computation.statements, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, desired, info.Dialect, planner.Options{
		Capabilities:         info.Capabilities,
		ConcurrentIndexes:    opts.Policy.ConcurrentIndexCreate,
		ConcurrentIndexDrops: opts.Policy.ConcurrentIndexDrop,
	})
	if err != nil {
		return applyComputation{}, fmt.Errorf("generate schema apply SQL: %w", err)
	}
	return computation, nil
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
	current        *types.DBSchema
	desired        *goschema.Database
	currentReports atlasfilter.ScopeReports
	desiredReports atlasfilter.ScopeReports
	currentErr     error
	desiredErr     error
}

func scopeApplyStates(
	current *types.DBSchema,
	desired *goschema.Database,
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
	conn *dbschema.DatabaseConnection,
	current *types.DBSchema,
	readScope []string,
	opts ApplyOptions,
) error {
	if err := validateCurrentApplySchema(current, opts.ValidateCurrentSchema); err != nil {
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
	current *types.DBSchema,
	validate func(*goschema.Database) error,
) error {
	if validate == nil {
		return nil
	}
	return validate(dbschematogo.ConvertDBSchemaToGoSchema(current))
}

// applyCurrentState reads the database side of an apply and reports the scope
// it was read at, which [planSourceSchema] needs in order to fingerprint a
// state [VerifyPlanTarget] can recompute.
func applyCurrentState(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	requested []string,
	desired *goschema.Database,
) (current *types.DBSchema, readScope []string, err error) {
	urlScope, err := schemascope.ReadNames(ctx, conn.Info(), nil, conn)
	if err != nil {
		return nil, nil, fmt.Errorf("read database schema: %w", err)
	}
	readScope = applyReadScope(requested, urlScope, desired)
	current, err = dbschema.ReadSchemaWithSchemas(conn, readScope)
	if err != nil {
		return nil, nil, fmt.Errorf("read database schema: %w", err)
	}
	return current, readScope, nil
}

// applyReadScope resolves the schemas the DATABASE side of an apply is read at.
//
// The two sides of a comparison have to be read at the same scope or silence on
// one of them is mistaken for absence. `schema inspect` on a URL that pins no
// schema describes every schema of the realm (stokaro/ptah#1264); applying that
// description back to the database it came from used to read only the
// connection's own schema, find no `extra` there, and plan `CREATE SCHEMA
// extra` and `CREATE TABLE "extra"."b"` for a schema and a table the database
// already has. Measured on PostgreSQL 17.10: the plan never converged, and
// executing it failed at SQLSTATE 42P07.
//
// base is what the URL says this run covers, resolved once by
// [schemascope.ReadNames] so that this verb, `schema diff`, `schema inspect`
// and `migrate diff` cannot disagree about it. An earlier fix derived the scope
// here from the DESIRED state instead, to keep a document describing one schema
// of a multi-schema database from reaching the others. That is the wrong
// protection at the wrong layer: it makes a whole schema silently unmanaged
// rather than authoritatively absent, and it makes this verb disagree with
// `schema diff` over the same two inputs. The pinned Atlas community binary
// v1.3.0 plans `DROP SCHEMA "extra" CASCADE` for that document on both verbs,
// measured on PostgreSQL 17.10, and a desired state that means to keep a schema
// it does not describe says so with a `ptah:not-described schema` record --
// which the comparator honors and no scope trick can express.
//
// The desired state's own schemas are still added on top, because a URL pinned
// to one schema by `search_path` covers less than a document may name, and a
// creation planned for an object that exists fails the run.
//
// An explicit `--schema` outranks both: it is the operator naming the scope.
func applyReadScope(requested, base []string, desired *goschema.Database) []string {
	if names := SplitSchemaNames(requested); len(names) > 0 {
		return names
	}
	scope := slices.Clone(base)
	scope = append(scope, desiredSchemaNames(desired)...)
	scope = slices.DeleteFunc(scope, func(name string) bool { return strings.TrimSpace(name) == "" })
	slices.Sort(scope)
	scope = slices.Compact(scope)
	if len(scope) == 0 {
		return nil
	}
	return scope
}

// desiredSchemaNames is every schema a desired state names, over the
// declarations that carry one. A document may name a schema by declaring a
// block for it or by qualifying an object with it, and both have to count: an
// inspected document does the first, a hand-written one often only the second.
func desiredSchemaNames(desired *goschema.Database) []string {
	if desired == nil {
		return nil
	}
	var names []string
	add := func(name string) {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			names = append(names, trimmed)
		}
	}
	for _, schema := range desired.Schemas {
		add(schema.Name)
	}
	for _, table := range desired.Tables {
		add(table.Schema)
	}
	for _, sequence := range desired.Sequences {
		add(sequence.Schema)
	}
	for _, domain := range desired.Domains {
		add(domain.Schema)
	}
	for _, composite := range desired.CompositeTypes {
		add(composite.Schema)
	}
	for _, rangeType := range desired.Ranges {
		add(rangeType.Schema)
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// loadDesiredApplySchema materializes the desired schema for apply planning.
// The resolver accepts local schema files (unchanged pre-resolver behavior),
// database URLs, migration directories replayed on the dev database, and
// env:// references; LocalFilesOnly pins the legacy local-file-only path.
func loadDesiredApplySchema(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts ApplyOptions,
) (*goschema.Database, error) {
	if opts.Desired != nil {
		return validateDesiredApplySchema(opts.Desired, opts.ValidateDesiredSchema)
	}
	// Both the dev database and the target can limit an apply to one schema, and
	// the target's URL is the one this connection was opened from.
	schemaScope, schemaScopeFlag := schemafile.ScopeFromURLs(opts.DevURL, conn.Info().URL, "url")
	if opts.LocalFilesOnly {
		desired, err := schemafile.LoadSources(localApplySources(opts), schemafile.Options{
			Dialect:               conn.Info().Dialect,
			IgnoreUnknownHCLNames: opts.IgnoreUnknownHCLNames,
			SchemaScope:           schemaScope,
			SchemaScopeFlag:       schemaScopeFlag,
			Vars:                  opts.Vars,
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
		set, err = atlassource.ClassifySet("--to", opts.ToURLs, opts.ProjectEnv)
		if err != nil {
			return nil, err
		}
	}
	state, err := set.Resolve(ctx, atlassource.ResolveOptions{
		Dialect:                   conn.Info().Dialect,
		DialectFlag:               "--url",
		DevURL:                    opts.DevURL,
		SchemaScope:               schemaScope,
		SchemaScopeFlag:           schemaScopeFlag,
		IgnoreUnknownHCLNames:     opts.IgnoreUnknownHCLNames,
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
	return state.Schema, nil
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
) (*goschema.Database, error) {
	desired, err := loadDesiredApplySchema(ctx, conn, opts)
	if err != nil {
		return nil, fmt.Errorf("load --to schema: %w", err)
	}
	if err := schemaselection.ValidateDeclaredPostgresSystemSchemas(
		conn.Info().Dialect,
		desired.Schemas,
	); err != nil {
		return nil, fmt.Errorf("validate --to schema: %w", err)
	}
	return desired, nil
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
	if err := atlasurl.ValidateDialectMatch(opts.DevURL, conn.Info().Dialect); err != nil {
		return ApplyRuntimePlan{}, err
	}

	computation, err := computeApplyPlan(ctx, conn, ApplyOptions{
		ToURLs:                    opts.ToURLs,
		Exclude:                   opts.Exclude,
		Schemas:                   opts.Schemas,
		Include:                   opts.Include,
		Policy:                    opts.Policy,
		DevURL:                    opts.DevURL,
		ProjectEnv:                opts.ProjectEnv,
		PreparedTo:                opts.PreparedTo,
		Desired:                   opts.Desired,
		ValidateDesiredSchema:     opts.ValidateDesiredSchema,
		ValidateCurrentSchema:     opts.ValidateCurrentSchema,
		ValidateLiveObject:        opts.ValidateLiveObject,
		ValidateMigrationSource:   opts.ValidateMigrationSource,
		ValidateLocalSchemaSource: opts.ValidateLocalSchemaSource,

		Diagnostics:            opts.Diagnostics,
		RefuseUnmatchedExclude: true,
		IgnoreUnknownHCLNames:  opts.IgnoreUnknownHCLNames,
		Vars:                   opts.Vars,
	})
	if err != nil {
		return ApplyRuntimePlan{}, err
	}
	return ApplyRuntimePlan{
		plan:    ApplyPlan{statements: computation.statements},
		dryRun:  opts.DryRun,
		conn:    conn,
		txMode:  opts.TxMode,
		current: computation.current,
	}, nil
}

func validateDesiredApplySchema(
	desired *goschema.Database,
	validator func(*goschema.Database) error,
) (*goschema.Database, error) {
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

func (p ApplyRuntimePlan) SQL() string {
	return p.plan.SQL()
}

func (p ApplyRuntimePlan) Statements() []string {
	return p.plan.Statements()
}

// Execute applies the prepared schema diff. Dry-run and no-op plans return
// without modifying schema state.
func (p ApplyRuntimePlan) Execute(ctx context.Context) error {
	if !p.HasChanges() || p.dryRun {
		return nil
	}
	if p.conn == nil {
		return errors.New("schema apply execution requires database connection")
	}

	p.conn.SchemaWriter().SetDryRun(false)
	return ApplySQL(ctx, p.conn, p.txMode, p.SQL())
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
	statements = executableStatements(statements, conn.Info().Dialect)
	switch txMode {
	case migrator.MigrationTxModeNone:
		return executeApplyStatements(ctx, conn.Writer(), statements)
	case migrator.MigrationTxModeFile, migrator.MigrationTxModeAll:
		tx, err := sqliterebuild.BeginTransaction(ctx, conn, statements)
		if err != nil {
			return fmt.Errorf("begin schema apply transaction: %w", err)
		}
		if err := executeApplyStatements(ctx, tx, statements); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit schema apply transaction: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("invalid tx-mode %q", txMode)
	}
}

func executableStatements(statements []string, dialect string) []string {
	filtered := make([]string, 0, len(statements))
	for _, statement := range statements {
		if strings.TrimSpace(sqlutil.StripCommentsForDialect(statement, dialect)) != "" {
			filtered = append(filtered, statement)
		}
	}
	return filtered
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
		out.WriteString(strings.TrimSuffix(stmt, ";"))
		out.WriteString(";\n")
	}
	return out.String()
}

func executeApplyStatements(ctx context.Context, executor types.SchemaExecutor, statements []string) error {
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := executor.ExecuteSQL(ctx, stmt); err != nil {
			return &migrator.MigrationExecutionError{
				Err:            fmt.Errorf("failed to execute SQL statement: %w", err),
				Statement:      stmt,
				StatementIndex: i + 1,
				Total:          len(statements),
			}
		}
	}
	return nil
}
