package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/schemafile"
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
	// LocalFilesOnly restricts ToURLs to local schema files, preserving the
	// pre-resolver loading behavior. `schema plan` sets it because a saved
	// plan fingerprints local desired-state files only.
	LocalFilesOnly bool
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
	// Desired supplies a pre-loaded desired schema model; see
	// [ApplyOptions.Desired].
	Desired *goschema.Database
	// Vars supplies values for HCL schema-file `variable` blocks, as `--var`
	// spells them; see [go.5x5.cz/ptah/internal/schemafile.Options].
	Vars []string
	// IgnoreUnknownHCLNames is the Atlas-compatible surface's unknown-name
	// policy; see [ApplyOptions.IgnoreUnknownHCLNames].
	IgnoreUnknownHCLNames bool
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

func computeApplyPlan(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts ApplyOptions,
) (applyComputation, error) {
	if conn == nil {
		return applyComputation{}, errors.New("schema apply planning requires database connection")
	}
	if len(opts.ToURLs) == 0 && opts.Desired == nil {
		return applyComputation{}, errors.New("schema apply planning requires desired schema URLs")
	}

	scope := atlasfilter.Scope{
		Schemas:       opts.Schemas,
		Include:       opts.Include,
		Exclude:       opts.Exclude,
		DefaultSchema: conn.Info().Schema,
	}
	desired, err := loadDesiredApplySchema(ctx, conn, opts)
	if err != nil {
		return applyComputation{}, fmt.Errorf("load --to schema: %w", err)
	}
	readScope := applyReadScope(opts.Schemas, conn.Info().Schema, desired)
	current, err := dbschema.ReadSchemaWithSchemas(conn, readScope)
	if err != nil {
		return applyComputation{}, fmt.Errorf("read database schema: %w", err)
	}
	current, currentErr := scopeDatabaseSide(current, scope, "current schema")
	if currentErr != nil && !emptySelection(currentErr) {
		return applyComputation{}, currentErr
	}
	desired, desiredErr := scopeGeneratedSide(desired, scope, "desired schema")
	if desiredErr != nil && !emptySelection(desiredErr) {
		return applyComputation{}, desiredErr
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

	computation := applyComputation{current: current, desired: desired, readScope: readScope}
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, desired, current, nil)
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

// applyReadScope resolves the schemas the DATABASE side of an apply is read at.
//
// The two sides of a comparison have to be read at the same scope or silence on
// one of them is mistaken for absence. `schema inspect` on a URL that pins no
// schema now describes every schema of the realm (stokaro/ptah#1264); applying
// that description back to the database it came from used to read only the
// connection's own schema, find no `extra` there, and plan `CREATE SCHEMA
// extra` and `CREATE TABLE "extra"."b"` for a schema and a table the database
// already has. Measured on PostgreSQL 17.10: the plan never converged, and
// executing it failed at SQLSTATE 42P07.
//
// The scope is taken from the DESIRED state rather than from the URL, and that
// choice is the safety property. A URL-derived realm scope would widen the read
// for every document, including one that describes a single schema of a
// multi-schema database -- and a schema the database has that the document does
// not describe is planned for removal. Measured on the same database, a
// document declaring only `public` read at two-schema scope plans `DROP TABLE
// IF EXISTS "extra"."b" CASCADE`. Reading exactly the schemas the document
// names cannot reach an object no document mentions.
//
// An explicit `--schema` outranks both: it is the operator naming the scope.
//
// nil is returned when the desired state names nothing beyond the schema the
// connection is already on, so the common case reads exactly what it read
// before.
func applyReadScope(requested []string, connectedSchema string, desired *goschema.Database) []string {
	if names := SplitSchemaNames(requested); len(names) > 0 {
		return names
	}
	connected := strings.TrimSpace(connectedSchema)
	named := desiredSchemaNames(desired)
	beyond := slices.DeleteFunc(named, func(name string) bool { return name == connected })
	if len(beyond) == 0 {
		return nil
	}
	if connected != "" {
		beyond = append(beyond, connected)
	}
	slices.Sort(beyond)
	return slices.Compact(beyond)
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
		return opts.Desired, nil
	}
	if opts.LocalFilesOnly {
		return schemafile.LoadAll(opts.ToURLs, schemafile.Options{
			Dialect:               conn.Info().Dialect,
			IgnoreUnknownHCLNames: opts.IgnoreUnknownHCLNames,
			Vars:                  opts.Vars,
		})
	}
	set, err := atlassource.ClassifySet("--to", opts.ToURLs, opts.ProjectEnv)
	if err != nil {
		return nil, err
	}
	state, err := set.Resolve(ctx, atlassource.ResolveOptions{
		Dialect:               conn.Info().Dialect,
		DialectFlag:           "--url",
		DevURL:                opts.DevURL,
		IgnoreUnknownHCLNames: opts.IgnoreUnknownHCLNames,
		Vars:                  opts.Vars,
	})
	if err != nil {
		return nil, err
	}
	return state.Schema, nil
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
		ToURLs:     opts.ToURLs,
		Exclude:    opts.Exclude,
		Schemas:    opts.Schemas,
		Include:    opts.Include,
		Policy:     opts.Policy,
		DevURL:     opts.DevURL,
		ProjectEnv: opts.ProjectEnv,
		Desired:    opts.Desired,

		IgnoreUnknownHCLNames: opts.IgnoreUnknownHCLNames,
		Vars:                  opts.Vars,
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
	switch txMode {
	case migrator.MigrationTxModeNone:
		return executeApplyStatements(ctx, conn.Writer(), statements)
	case migrator.MigrationTxModeFile, migrator.MigrationTxModeAll:
		tx, err := conn.SchemaWriter().BeginTransaction(ctx)
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
