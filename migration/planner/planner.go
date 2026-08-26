package planner

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/planner/dialects/clickhouse"
	"go.5x5.cz/ptah/internal/planner/dialects/mssql"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	"go.5x5.cz/ptah/internal/planner/dialects/oracle"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/internal/planner/dialects/sqlite"
	"go.5x5.cz/ptah/internal/txrequire"
	"go.5x5.cz/ptah/migration/diffpolicy"
	"go.5x5.cz/ptah/migration/internal/identifiervalidation"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

var builtInPlannerRegistration struct {
	once sync.Once
	err  error
}

var plannerRegistry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

// Planner defines the interface for database-specific migration planning.
//
// Implementations of this interface are responsible for converting schema differences
// into Abstract Syntax Tree (AST) nodes that represent the SQL operations needed to
// migrate from the current database schema to the target schema.
//
// The interface is designed to be dialect-agnostic at the contract level while
// allowing implementations to handle database-specific features, constraints,
// and optimization strategies.
//
// # Implementation Requirements
//
// Implementations must:
//   - Generate AST nodes in dependency-aware order (e.g., create tables before foreign keys)
//   - Handle dialect-specific data types and constraints appropriately
//   - Provide safe migration paths that minimize data loss risks
//   - Support rollback scenarios where applicable
//
// # Parameters
//
//   - diff: Contains the differences between target and current schemas
//   - generated: The target schema derived from Go struct annotations
//
// # Return Value
//
// Returns a slice of AST nodes representing the SQL operations needed for migration.
// The nodes are ordered to respect database dependencies and constraints.
//
// # Example Implementation Pattern
//
//	func (p *PostgresPlanner) GenerateMigrationAST(
//		diff *difftypes.SchemaDiff,
//		generated *schemamodel.Database,
//	) ([]ast.Node, error) {
//		var nodes []ast.Node
//
//		// 1. Create enum types first (PostgreSQL-specific)
//		nodes = append(nodes, p.generateEnumCreations(diff, generated)...)
//
//		// 2. Create tables in dependency order
//		nodes = append(nodes, p.generateTableCreations(diff, generated)...)
//
//		// 3. Add indexes and constraints
//		nodes = append(nodes, p.generateIndexCreations(diff, generated)...)
//
//		return nodes, nil
//	}
type Planner interface {
	GenerateMigrationAST(diff *difftypes.SchemaDiff, desired *schemamodel.Database) ([]ast.Node, error)
}

// Options configures high-level planner helpers.
type Options struct {
	// Capabilities describes the concrete target server. Nil means the
	// dialect's default capability preset.
	Capabilities capability.Capabilities
	// ConcurrentIndexes requests PostgreSQL CREATE INDEX CONCURRENTLY for all
	// newly added indexes when the target supports it.
	ConcurrentIndexes bool
	// ConcurrentIndexRefs requests PostgreSQL CREATE INDEX CONCURRENTLY for
	// exactly these table-qualified newly added indexes when the target
	// supports it.
	ConcurrentIndexRefs []difftypes.IndexRef
	// ConcurrentIndexDrops requests PostgreSQL DROP INDEX CONCURRENTLY for all
	// standalone index removals when the target supports it. It is separate
	// from ConcurrentIndexes so enabling concurrent index builds never silently
	// rewrites a drop the caller did not ask for.
	ConcurrentIndexDrops bool
	// ConcurrentIndexDropRefs requests PostgreSQL DROP INDEX CONCURRENTLY for
	// exactly these table-qualified removed indexes when the target supports it.
	ConcurrentIndexDropRefs []difftypes.IndexRef
	// SkipChangeKinds lists destructive change kinds the planner must omit from
	// the plan (emitting a clearly-marked comment in their place) instead of
	// deferring to the coarse destructive gate. Currently honored by the
	// PostgreSQL-family planner.
	SkipChangeKinds []diffpolicy.ChangeKind
}

// CapabilitiesFor returns the configured capability set, falling back to the
// default preset for dialect when no explicit set was provided.
func (o Options) CapabilitiesFor(dialect string) capability.Capabilities {
	if o.Capabilities != nil {
		return o.Capabilities
	}
	return capability.ForDialect(dialect)
}

// Factory creates a planner for a dialect from construction options.
type Factory func(Options) Planner

// Register registers a planner factory for a dialect. Third-party dialects can
// call this from init and then use the standard planner helpers.
func Register(dialect string, factory Factory) error {
	if err := ensureBuiltInPlannersRegistered(); err != nil {
		return err
	}
	return registerPlannerFactory(dialect, factory)
}

// RegisteredDialects returns the registered planner dialect names, or the
// error that kept the built-in planners from registering.
func RegisteredDialects() ([]string, error) {
	if err := ensureBuiltInPlannersRegistered(); err != nil {
		return nil, err
	}
	plannerRegistry.mu.RLock()
	defer plannerRegistry.mu.RUnlock()

	dialects := make([]string, 0, len(plannerRegistry.factories))
	for dialect := range plannerRegistry.factories {
		dialects = append(dialects, dialect)
	}
	slices.Sort(dialects)
	return dialects, nil
}

// GetPlanner returns a dialect-specific migration planner for the given database dialect.
//
// This registry lookup creates and returns the appropriate planner
// implementation based on the specified database dialect. Each planner handles
// dialect-specific features, SQL syntax variations, and optimization
// strategies.
//
// # Supported Dialects
//
// Every dialect this package registers a built-in planner for is accepted:
// postgres, cockroachdb, yugabytedb, spanner, mysql, mariadb, sqlserver,
// oracle, clickhouse, and sqlite. See the package documentation for what each
// planner does, and RegisteredDialects for the set at runtime, which a
// third-party Register call can extend.
//
// # Parameters
//
//   - dialect: Database dialect identifier (use constants from platform package)
//
// # Return Value
//
// Returns a Planner implementation specific to the requested dialect, or an
// error for unknown, unsupported, empty, or invalid dialect strings.
//
// # Usage Example
//
//	import "go.5x5.cz/ptah/core/platform"
//
//	// Get PostgreSQL planner
//	pgPlanner, err := planner.GetPlanner(platform.Postgres)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Get MySQL planner
//	mysqlPlanner, err := planner.GetPlanner(platform.MySQL)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Generate migration AST
//	nodes, err := pgPlanner.GenerateMigrationAST(diff, generated)
//	if err != nil {
//		log.Fatal(err)
//	}
//
// # Design Rationale
//
// The registry pattern is used here to:
//   - Provide a clean, consistent interface for planner creation
//   - Allow third-party extension for new database dialects
//   - Centralize dialect validation and error handling
//   - Enable dependency injection and testing scenarios
func GetPlanner(dialect string) (Planner, error) {
	return GetPlannerWithOptions(dialect, Options{Capabilities: capability.ForDialect(dialect)})
}

// GetPlannerWithOptions returns a dialect-specific migration planner with
// explicit high-level generation policy. Live database paths should set
// Options.Capabilities from DBInfo.Capabilities so planning uses the same
// server-version preset as readers and renderers; offline callers can leave
// it nil to fall back to the dialect's default preset.
func GetPlannerWithOptions(dialect string, opts Options) (Planner, error) {
	if err := ensureBuiltInPlannersRegistered(); err != nil {
		return nil, err
	}
	return getRegisteredPlanner(dialect, opts)
}

func ensureBuiltInPlannersRegistered() error {
	builtInPlannerRegistration.once.Do(func() {
		builtInPlannerRegistration.err = registerBuiltInPlanners()
	})
	return builtInPlannerRegistration.err
}

func registerBuiltInPlanners() error {
	for _, dialect := range []string{
		platform.Postgres,
		platform.CockroachDB,
		platform.YugabyteDB,
		platform.Spanner,
	} {
		if err := registerPostgresFamilyPlanner(dialect); err != nil {
			return err
		}
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		if err := registerMySQLFamilyPlanner(dialect); err != nil {
			return err
		}
	}

	if err := registerPlannerFactory(platform.SQLServer, func(opts Options) Planner {
		return mssql.NewWithCapabilities(opts.CapabilitiesFor(platform.SQLServer))
	}); err != nil {
		return err
	}

	if err := registerPlannerFactory(platform.Oracle, func(opts Options) Planner {
		return oracle.NewWithCapabilities(opts.CapabilitiesFor(platform.Oracle))
	}); err != nil {
		return err
	}

	if err := registerPlannerFactory(platform.ClickHouse, func(opts Options) Planner {
		return clickhouse.NewWithCapabilities(opts.CapabilitiesFor(platform.ClickHouse))
	}); err != nil {
		return err
	}
	return registerPlannerFactory(platform.SQLite, func(opts Options) Planner {
		return sqlite.NewWithCapabilities(opts.CapabilitiesFor(platform.SQLite))
	})
}

func registerPostgresFamilyPlanner(dialect string) error {
	return registerPlannerFactory(dialect, func(opts Options) Planner {
		plan := postgres.NewForDialect(dialect, opts.CapabilitiesFor(dialect)).
			WithConcurrentIndexRefs(opts.ConcurrentIndexRefs...).
			WithConcurrentIndexDropRefs(opts.ConcurrentIndexDropRefs...).
			WithSkipChangeKinds(opts.SkipChangeKinds...)
		if opts.ConcurrentIndexes {
			plan = plan.WithConcurrentIndexes()
		}
		if opts.ConcurrentIndexDrops {
			plan = plan.WithConcurrentIndexDrops()
		}
		return plan
	})
}

func registerMySQLFamilyPlanner(dialect string) error {
	return registerPlannerFactory(dialect, func(opts Options) Planner {
		return mysql.NewForDialect(dialect, opts.CapabilitiesFor(dialect))
	})
}

func registerPlannerFactory(dialect string, factory Factory) error {
	normalized := normalizeRegistryDialect(dialect)
	if normalized == "" {
		return fmt.Errorf("planner registry: dialect must not be empty")
	}
	if factory == nil {
		return fmt.Errorf("planner registry: factory for dialect %q must not be nil", normalized)
	}

	plannerRegistry.mu.Lock()
	defer plannerRegistry.mu.Unlock()

	if plannerRegistry.factories == nil {
		plannerRegistry.factories = make(map[string]Factory)
	}
	if _, exists := plannerRegistry.factories[normalized]; exists {
		return fmt.Errorf("planner registry: dialect %q is already registered", normalized)
	}
	plannerRegistry.factories[normalized] = factory
	return nil
}

func getRegisteredPlanner(dialect string, opts Options) (Planner, error) {
	normalized := normalizeRegistryDialect(dialect)
	if normalized == "" {
		return nil, unsupportedDialectPlanError(dialect)
	}

	plannerRegistry.mu.RLock()
	factory, exists := plannerRegistry.factories[normalized]
	plannerRegistry.mu.RUnlock()
	if !exists {
		return nil, unsupportedDialectPlanError(dialect)
	}
	planner := factory(opts)
	if planner == nil {
		return nil, fmt.Errorf("planner registry: factory for dialect %q returned nil", normalized)
	}
	return planner, nil
}

func unsupportedDialectPlanError(dialect string) error {
	return &ptaherr.PlanError{
		Dialect: dialect,
		Err:     ptaherr.ErrUnsupportedDialect,
		Message: fmt.Sprintf("unsupported database dialect: %s", dialect),
	}
}

func normalizeRegistryDialect(dialect string) string {
	normalized := platform.NormalizeDialect(dialect)
	if normalized != "" {
		return normalized
	}
	return strings.ToLower(strings.TrimSpace(dialect))
}

// GenerateSchemaDiffAST generates AST nodes for schema differences using the specified dialect.
//
// This is a convenience function that combines planner creation and AST generation
// into a single call. It internally uses GetPlanner to obtain the appropriate
// dialect-specific planner and then calls GenerateMigrationAST on it.
//
// # Parameters
//
//   - diff: Schema differences identified by the schemadiff package
//   - generated: Target schema parsed from Go struct annotations
//   - dialect: Database dialect identifier (use constants from platform package)
//
// # Return Value
//
// Returns a slice of AST nodes representing the SQL operations needed for migration.
// The nodes are ordered to respect database dependencies and constraints.
//
// # Usage Example
//
//	import "go.5x5.cz/ptah/core/platform"
//
//	// Generate AST nodes for PostgreSQL
//	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.Postgres)
//	if err != nil {
//		return err
//	}
//
//	// Process nodes for custom validation or transformation
//	for _, node := range nodes {
//		// Custom processing logic
//	}
//
// # See Also
//
//   - GenerateSchemaDiffSQL: For complete SQL string generation
//   - GenerateSchemaDiffSQLStatements: For individual SQL statements
//   - GetPlanner: For direct planner access
func GenerateSchemaDiffAST(diff *difftypes.SchemaDiff, desired *schemamodel.Database, dialect string) ([]ast.Node, error) {
	return GenerateSchemaDiffASTWithOptions(diff, desired, dialect, Options{
		Capabilities: capability.ForDialect(dialect),
	})
}

// GenerateSchemaDiffASTWithOptions generates AST nodes with explicit planning
// options.
func GenerateSchemaDiffASTWithOptions(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dialect string,
	opts Options,
) ([]ast.Node, error) {
	caps := opts.CapabilitiesFor(dialect)
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	// Both normalizations belong here rather than in the dialect planners: a
	// column does not carry the schema of the user type it names, only the
	// declaration does, and every dialect planner reads its columns out of this
	// one value (stokaro/ptah#1138).
	preparedGenerated := fromschema.QualifyDeclaredUserTypes(
		fromschema.AssignDefaultForeignKeyNames(desired, dialect),
		dialect,
	)
	if diff != nil &&
		diff.IdentifierSemantics != nil &&
		!diff.IdentifierSemantics.IsZero() &&
		!diff.IdentifierSemantics.Equal(semantics) {
		return nil, wrapPlanError(
			dialect,
			fmt.Errorf(
				"%w: invalid identifier semantics snapshot",
				ptaherr.ErrInvalidSchemaDiff,
			),
		)
	}
	if err := identifiervalidation.ValidateTarget(
		preparedGenerated,
		dialect,
		semantics,
	); err != nil {
		return nil, wrapPlanError(dialect, err)
	}
	if preparedGenerated != nil {
		if err := renderer.ValidateSchemaWithCapabilities(preparedGenerated, dialect, caps); err != nil {
			return nil, wrapPlanError(dialect, err)
		}
	}
	planner, err := GetPlannerWithOptions(dialect, opts)
	if err != nil {
		return nil, wrapPlanError(dialect, err)
	}
	nodes, err := planner.GenerateMigrationAST(diff, preparedGenerated)
	if err != nil {
		return nil, wrapPlanError(dialect, err)
	}
	return nodes, nil
}

// NodeRequiresNoTransaction reports whether a single planned AST node must run
// outside the migrator's per-migration transaction.
//
// The rule lives in [txrequire], which also answers the authored-file question
// migration/lint and the migrator ask. There used to be two implementations
// and they disagreed: this one counted `ALTER TYPE ... ADD VALUE` and lint's
// counted concurrent indexes only, so the enum file lint did not call a mix
// was exactly the file that failed at apply (stokaro/ptah#996).
func NodeRequiresNoTransaction(dialect string, node ast.Node) bool {
	return txrequire.NodeRequiresAutocommit(dialect, node)
}

// RequiresNoTransaction reports whether the planned migration contains
// statements that must be applied outside the migrator's per-migration
// transaction. Keep this conservative: it should only return true for DDL that
// is known to be rejected or unusable in a PostgreSQL-family transaction.
func RequiresNoTransaction(dialect string, nodes []ast.Node) bool {
	for _, node := range nodes {
		if NodeRequiresNoTransaction(dialect, node) {
			return true
		}
	}
	return false
}

// GenerateSchemaDiffSQLStatements generates individual SQL statements for schema differences.
//
// This high-level convenience function provides the most commonly used output format:
// a slice of individual SQL statements that can be executed sequentially to perform
// the migration. It combines AST generation, SQL rendering, and statement splitting
// into a single operation.
//
// # Parameters
//
//   - diff: Schema differences identified by the schemadiff package
//   - generated: Target schema parsed from Go struct annotations
//   - dialect: Database dialect identifier (use constants from platform package)
//
// # Return Value
//
// Returns a slice of individual SQL statements, each ending with a semicolon.
// The statements are ordered to respect database dependencies and can be executed
// sequentially to perform the migration.
//
// # Statement Processing
//
// The function performs the following processing steps:
//  1. Generate AST nodes using GenerateSchemaDiffAST
//  2. Render AST nodes to complete SQL using the renderer package
//  3. Split the SQL into individual statements using sqlutil.SplitSQLStatements
//  4. Return the statements as a string slice
//
// # Usage Example
//
//	import "go.5x5.cz/ptah/core/platform"
//
//	// Generate SQL statements for MySQL
//	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, platform.MySQL)
//	if err != nil {
//		return err
//	}
//
//	// Execute statements sequentially
//	for _, stmt := range statements {
//		if err := db.Exec(stmt); err != nil {
//			log.Fatalf("Failed to execute statement: %v", err)
//		}
//	}
//
// # See Also
//
//   - GenerateSchemaDiffSQL: For complete SQL string without splitting
//   - GenerateSchemaDiffAST: For AST nodes without rendering
func GenerateSchemaDiffSQLStatements(diff *difftypes.SchemaDiff, desired *schemamodel.Database, dialect string) ([]string, error) {
	output, err := GenerateSchemaDiffSQLWithOptions(diff, desired, dialect, Options{
		Capabilities: capability.ForDialect(dialect),
	})
	if err != nil {
		return nil, err
	}
	statements := sqlutil.SplitSQLStatementsForDialect(output, dialect)
	return statements, nil
}

// GenerateSchemaDiffSQLStatementsWithOptions generates individual SQL
// statements using explicit planning options.
func GenerateSchemaDiffSQLStatementsWithOptions(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dialect string,
	opts Options,
) ([]string, error) {
	output, err := GenerateSchemaDiffSQLWithOptions(diff, desired, dialect, opts)
	if err != nil {
		return nil, err
	}
	// The dialect decides where one statement ends. The blind splitter treats
	// every semicolon outside a BEGIN block as a boundary, which is right for
	// most targets and wrong for the one whose routine body is opened by IS:
	// an Oracle function with a declaration section came out of here as four
	// fragments, each of which the server refuses on its own.
	statements := sqlutil.SplitSQLStatementsForDialect(output, dialect)
	return statements, nil
}

// GenerateSchemaDiffSQL generates complete SQL for schema differences as a single string.
//
// This function provides a mid-level interface that generates a complete SQL script
// containing all the statements needed to perform the migration. The output is a
// single string with multiple SQL statements separated by semicolons and newlines.
//
// # Parameters
//
//   - diff: Schema differences identified by the schemadiff package
//   - generated: Target schema parsed from Go struct annotations
//   - dialect: Database dialect identifier (use constants from platform package)
//
// # Return Value
//
// Returns a complete SQL script as a single string. The script contains all
// statements needed for the migration, properly formatted and ordered.
//
// # SQL Generation Process
//
// The function performs the following steps:
//  1. Generate AST nodes using GenerateSchemaDiffAST
//  2. Render all AST nodes to SQL using the dialect-specific renderer
//  3. Return the complete SQL as a single string
//
// # Output Format
//
// The generated SQL includes:
//   - Proper statement termination with semicolons
//   - Appropriate line breaks and formatting
//   - Comments for complex operations (dialect-dependent)
//   - Dependency-ordered statements
//
// # Usage Example
//
//	import "go.5x5.cz/ptah/core/platform"
//
//	// Generate complete SQL script for PostgreSQL
//	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.Postgres)
//	if err != nil {
//		return err
//	}
//
//	// Write to migration file
//	if err := os.WriteFile("migration.sql", []byte(sql), 0644); err != nil {
//		log.Fatalf("Failed to write migration file: %v", err)
//	}
//
//	// Or execute as a single transaction
//	if _, err := db.Exec(sql); err != nil {
//		log.Fatalf("Migration failed: %v", err)
//	}
//
// # See Also
//
//   - GenerateSchemaDiffSQLStatements: For individual SQL statements
//   - GenerateSchemaDiffAST: For AST nodes without rendering
func GenerateSchemaDiffSQL(diff *difftypes.SchemaDiff, desired *schemamodel.Database, dialect string) (string, error) {
	return GenerateSchemaDiffSQLWithOptions(diff, desired, dialect, Options{
		Capabilities: capability.ForDialect(dialect),
	})
}

// GenerateSchemaDiffSQLWithOptions generates complete SQL using explicit
// planning options.
func GenerateSchemaDiffSQLWithOptions(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dialect string,
	opts Options,
) (string, error) {
	// An extension the DESIRED schema declares is one the plan installs, and it
	// is installed before the statements that need it. A connection opened
	// before the extension existed answers about the past, so its capability
	// set alone would emit `CREATE EXTENSION "timescaledb"` and then skip the
	// create_hypertable that needs it (stokaro/ptah#1026).
	caps := capability.WithDeclaredExtensions(
		opts.CapabilitiesFor(dialect), declaredExtensionNames(desired))
	astNodes, err := GenerateSchemaDiffASTWithOptions(diff, desired, dialect, opts)
	if err != nil {
		return "", err
	}
	output, err := renderer.RenderSQLWithCapabilities(dialect, caps, astNodes...)
	if err != nil {
		return "", wrapRenderError(dialect, err)
	}
	return output, nil
}

// declaredExtensionNames is what the desired schema says the target will have
// by the time these statements run.
func declaredExtensionNames(desired *schemamodel.Database) []string {
	if desired == nil {
		return nil
	}
	names := make([]string, 0, len(desired.Extensions))
	for _, extension := range desired.Extensions {
		names = append(names, extension.Name)
	}
	return names
}

func wrapPlanError(dialect string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*ptaherr.PlanError](err); ok {
		return err
	}
	return &ptaherr.PlanError{
		Dialect: dialect,
		Err:     err,
		Message: err.Error(),
	}
}

func wrapRenderError(dialect string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*ptaherr.RenderError](err); ok {
		return err
	}
	return &ptaherr.RenderError{
		Dialect: dialect,
		Err:     err,
		Message: err.Error(),
	}
}
