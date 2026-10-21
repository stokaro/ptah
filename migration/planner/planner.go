package planner

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/txrequire"
)

// ErrInvalidPlan reports a plan a planner built in breach of the planning
// contract, such as an owner operation planned beside other work in one node.
// It names a defect in the planner, not in the diff it was given, which is
// why it is not [ptaherr.ErrInvalidSchemaDiff].
var ErrInvalidPlan = errors.New("invalid plan")

// GenerateSchemaDiffAST generates AST nodes for schema differences using the specified dialect.
//
// This is a convenience function that combines planner creation and AST generation
// into a single call. It obtains the target planner from the selected runtime
// and then calls GenerateMigrationAST on it.
//
// # Parameters
//
//   - ctx: Caller context, required even for an empty change set
//   - runtime: Selected feature planning and rendering services
//   - diff: Schema differences identified by the schemadiff package
//   - dialect: Database dialect identifier (use constants from platform package)
//
// # Return Value
//
// Returns a slice of AST nodes representing the SQL operations needed for migration.
// The nodes are ordered to respect database dependencies and constraints.
//
// # Usage Example
//
//	import "ptah.run/core/platform"
//
//	// Generate AST nodes for PostgreSQL
//	nodes, err := planner.GenerateSchemaDiffAST(ctx, runtime, diff, platform.Postgres)
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
//   - planext.Resolver: For direct planner access
func GenerateSchemaDiffAST(ctx context.Context, runtime PlanningRuntime, diff *difftypes.SchemaDiff, dialect string) ([]ast.Node, error) {
	return GenerateSchemaDiffASTWithOptions(
		ctx, runtime,
		diff, dialect, planext.Options{
			Capabilities: capability.ForDialect(dialect),
		},
	)
}

// GenerateSchemaDiffASTWithOptions generates AST nodes with explicit planning
// options. The context and runtime are required; errors and cancellation return
// no partial plan. The refusals below hold for every package-level generation helper
// in this package.
//
// The diff carries the schema operands. Context and runtime select the services
// and cancellation boundary. The desired schema is prepared and validated
// where it is supplied, in the schemadiff package, which puts on the diff what
// planning needs, its Declared* carries included; a caller assembling a diff
// by hand can run schemadiff.ValidateDesiredSchema itself. Planning refuses
// rather than guesses:
//
//   - a diff carrying an IdentifierSemantics snapshot that is not valid for
//     the dialect fails with an error satisfying errors.Is against
//     ptaherr.ErrInvalidSchemaDiff, and a nil diff is refused the same way by
//     every built-in planner rather than planned as an empty migration
//   - a dialect no planner is registered for fails with an error satisfying
//     errors.Is against ptaherr.ErrUnsupportedDialect
//   - a plan in which an owner operation shares a node with other work fails
//     with an error satisfying errors.Is against [ErrInvalidPlan]: each owner
//     operation is planned as a node of its own, so its verdict belongs to
//     the statements that node renders
//
// Every failure comes back as a *ptaherr.PlanError carrying the dialect, so
// errors.As selects the structured form and errors.Is the sentinel.
func GenerateSchemaDiffASTWithOptions(
	ctx context.Context, runtime PlanningRuntime,
	diff *difftypes.SchemaDiff,
	dialect string,
	opts planext.Options,
) ([]ast.Node, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, wrapPlanError(dialect, err)
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
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
	planner, err := runtime.MigrationPlanner(dialect, opts)
	if err != nil {
		return nil, wrapPlanError(dialect, err)
	}
	diff, err = scopeCreatedObjects(runtime, diff, dialect)
	if err != nil {
		return nil, wrapPlanError(dialect, err)
	}
	nodes, err := planner.GenerateMigrationAST(
		ctx, runtime,
		diff,
	)
	if err != nil {
		return nil, wrapPlanError(dialect, err)
	}
	if opts.OnlineAlter {
		// After the dialect planner rather than inside it: every ALTER TABLE
		// the plan emits carries the request, and the dialect planners build
		// them in a dozen places. A pass over the result cannot miss one.
		nodes = requestOnlineAlter(nodes, dialect, opts.CapabilitiesFor(dialect))
	}
	if opts.OmitNullBackfill {
		// After the dialect planner for the reason the online request is:
		// a pass over the result reaches every column modification.
		omitNullBackfill(nodes)
	}
	if err := requireIsolatedOwnerOperations(nodes); err != nil {
		return nil, wrapPlanError(dialect, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, wrapPlanError(dialect, err)
	}
	return append(skippedComments(diff), nodes...), nil
}

// skippedComments writes one comment ahead of the plan for each object the
// comparison left out because the target does not serve its model and its
// owner answers [schemaext.OffTargetSkip], as the whole-schema render does:
// the plan has nothing to run for it, and a reader is told why.
func skippedComments(diff *difftypes.SchemaDiff) []ast.Node {
	if diff == nil || len(diff.SkippedFeatures) == 0 {
		return nil
	}
	comments := make([]ast.Node, 0, len(diff.SkippedFeatures))
	for _, object := range diff.SkippedFeatures {
		comments = append(comments, ast.NewComment(strings.ToUpper(object.Target)+": "+object.Note()))
	}
	return comments
}

// requireIsolatedOwnerOperations refuses a plan in which an owner operation
// shares a node with anything else. Safety reports and saved plans give each
// statement the verdict of the owner operation that rendered it, and they can
// tell which one that is only when the operation is a node of its own:
// otherwise an ADD COLUMN beside a policy would carry the policy's access
// verdict, or the policy the DROP TABLE's. The planners build owner operations
// that way already, so this guards the boundary rather than reshaping a plan.
func requireIsolatedOwnerOperations(nodes []ast.Node) error {
	for index, node := range nodes {
		if ast.PlacementOf(node) == ast.MixedExtension {
			return fmt.Errorf("%w: planned node %d (%T) carries an owner operation beside other operations; "+
				"each owner operation must be planned as a node of its own", ErrInvalidPlan, index+1, node)
		}
	}
	return nil
}

// NodeRequiresNoTransaction reports whether a single planned AST node must run
// outside the migrator's per-migration transaction.
//
// The rule lives in [txrequire], which also answers the authored-file question
// migration/lint and the migrator ask. Two implementations disagree: one
// counting `ALTER TYPE ... ADD VALUE` and lint's counting concurrent indexes
// only makes the enum file lint does not call a mix exactly the file that fails
// at apply (stokaro/ptah#996).
func NodeRequiresNoTransaction(dialect string, node ast.Node) bool {
	return txrequire.NodeRequiresAutocommit(dialect, node)
}

// RequiresNoTransaction reports whether the planned migration contains
// statements that must be applied outside the migrator's per-migration
// transaction. An explicit owner requirement or a built-in descriptor rule
// establishes this restriction; unknown metadata does not.
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
//   - ctx: Caller context, required even for an empty change set
//   - runtime: Selected feature planning and rendering services
//   - diff: Schema differences identified by the schemadiff package
//   - dialect: Database dialect identifier (use constants from platform package)
//
// # Return Value
//
// Returns a slice of individual SQL statements. A statement carries no
// trailing semicolon and may open with the comment lines the planner attached
// to it, so each element is handed to the driver as-is. The statements are
// ordered to respect database dependencies and are executed in order, on one
// database session, to perform the migration: a MySQL or MariaDB plan that
// replaces a trigger holds LOCK TABLES from one statement to a later one.
//
// # Statement Processing
//
// The function performs the following processing steps:
//  1. Generate AST nodes using GenerateSchemaDiffAST
//  2. Render each AST node to its SQL fragment using the renderer package
//  3. Split each fragment under the dialect's statement grammar, so no
//     statement runs into the next node's (see [RenderedPlan.PlannedStatements])
//  4. Return the statements as a string slice
//
// # Usage Example
//
//	import "ptah.run/core/platform"
//
//	// Generate SQL statements for MySQL
//	statements, err := planner.GenerateSchemaDiffSQLStatements(ctx, runtime, diff, platform.MySQL)
//	if err != nil {
//		return err
//	}
//
//	// Execute statements in order on one session
//	session, err := db.Conn(ctx)
//	if err != nil {
//		return err
//	}
//	defer session.Close()
//	for _, stmt := range statements {
//		if _, err := session.ExecContext(ctx, stmt); err != nil {
//			log.Fatalf("Failed to execute statement: %v", err)
//		}
//	}
//
// # See Also
//
//   - GenerateSchemaDiffSQL: For complete SQL string without splitting
//   - GenerateSchemaDiffAST: For AST nodes without rendering
func GenerateSchemaDiffSQLStatements(ctx context.Context, runtime Runtime, diff *difftypes.SchemaDiff, dialect string) ([]string, error) {
	return GenerateSchemaDiffSQLStatementsWithOptions(
		ctx, runtime,
		diff, dialect, planext.Options{
			Capabilities: capability.ForDialect(dialect),
		},
	)
}

// GenerateSchemaDiffSQLStatementsWithOptions generates individual SQL
// statements using explicit planning options.
func GenerateSchemaDiffSQLStatementsWithOptions(
	ctx context.Context, runtime Runtime,
	diff *difftypes.SchemaDiff,
	dialect string,
	opts planext.Options,
) ([]string, error) {
	plan, err := GenerateSchemaDiffRenderedPlan(ctx, runtime, diff, dialect, opts)
	if err != nil {
		return nil, err
	}
	return plan.Statements(), nil
}

// RenderedPlan is a schema diff planned and rendered once. Request is the
// rendering request the planned AST nodes went out in, and Result holds one
// fragment per node, so a statement can be traced to the node that rendered it
// without planning again. Its SQL and Statements are
// what [GenerateSchemaDiffSQLWithOptions] and
// [GenerateSchemaDiffSQLStatementsWithOptions] return for the same input.
type RenderedPlan struct {
	// Request holds the planned nodes in Request.Nodes.
	Request renderer.Request
	Result  renderer.Result
	// Dialect is the caller's spelling of the target, which decides where one
	// statement ends.
	Dialect string
}

// SQL returns the rendered plan as one script.
func (p RenderedPlan) SQL() string { return p.Result.SQL() }

// PlannedStatement is one statement of a rendered plan and the index of the
// planned node, in Request.Nodes, whose fragment rendered it. Node is -1 for a
// comment-only piece at the end of the plan, which no statement follows.
type PlannedStatement struct {
	SQL  string
	Node int
}

// PlannedStatements splits the rendered plan into the statements it executes,
// one fragment at a time, so each statement keeps the node that rendered it.
// A comment-only piece of a fragment, such as an owner's note, is joined to the
// next statement and takes that statement's node, as splitting the joined
// script would join it. A fragment's statements never run into the next
// fragment's: a fragment that leaves its last statement unterminated, or ends
// a SQL Server routine whose body would otherwise reach the end of the batch,
// still ends where its node ends.
func (p RenderedPlan) PlannedStatements() []PlannedStatement {
	if p.Result.ExecutableUnits {
		units := make([]PlannedStatement, 0, len(p.Result.Fragments))
		for node, sql := range p.Result.Fragments {
			if sql != "" {
				units = append(units, PlannedStatement{SQL: sql, Node: node})
			}
		}
		return units
	}
	var statements []PlannedStatement
	carry := ""
	for node, fragment := range p.Result.Fragments {
		// The dialect decides where one statement ends. The blind splitter
		// treats every semicolon outside a BEGIN block as a boundary, which is
		// right for most targets and wrong for the one whose routine body is
		// opened by IS: an Oracle function with a declaration section came out
		// of here as four fragments, each of which the server refuses on its own.
		text := carry + fragment
		pieces := sqlutil.SplitSQLStatementsForDialect(text, p.Dialect)
		executable := len(pieces)
		for executable > 0 && strings.TrimSpace(sqlutil.StripCommentsForDialect(pieces[executable-1], p.Dialect)) == "" {
			executable--
		}
		for _, piece := range pieces[:executable] {
			statements = append(statements, PlannedStatement{SQL: piece, Node: node})
		}
		carry = trailingPieces(text, pieces, executable)
	}
	for _, piece := range sqlutil.SplitSQLStatementsForDialect(carry, p.Dialect) {
		statements = append(statements, PlannedStatement{SQL: piece, Node: -1})
	}
	return statements
}

// trailingPieces is text from the first of pieces[from:] to its end: the
// comment-only pieces a fragment ends with, as they were written, so the next
// fragment's first statement takes them byte for byte as a split of the joined
// plan would, trailing spaces inside a comment included. A piece the splitter
// rewrote, which a client delimiter directive can do, is not found in text; the
// pieces are then joined by line instead.
func trailingPieces(text string, pieces []string, from int) string {
	if from == len(pieces) {
		return ""
	}
	cursor := 0
	for i, piece := range pieces {
		at := strings.Index(text[cursor:], piece)
		if at < 0 {
			break
		}
		if i == from {
			return text[cursor+at:]
		}
		cursor += at + len(piece)
	}
	return strings.Join(pieces[from:], "\n") + "\n"
}

// Statements returns the SQL of [RenderedPlan.PlannedStatements].
func (p RenderedPlan) Statements() []string {
	planned := p.PlannedStatements()
	statements := make([]string, 0, len(planned))
	for _, statement := range planned {
		statements = append(statements, statement.SQL)
	}
	return statements
}

// GenerateSchemaDiffRenderedPlan plans diff and renders the planned nodes
// once, keeping the nodes, the request, and the per-node result together. It
// fails with the errors [GenerateSchemaDiffSQLWithOptions] documents, and
// returns the zero plan on any failure or cancellation.
func GenerateSchemaDiffRenderedPlan(
	ctx context.Context, runtime Runtime,
	diff *difftypes.SchemaDiff,
	dialect string,
	opts planext.Options,
) (RenderedPlan, error) {
	// An extension this plan installs is installed before the statements that
	// need it. A connection opened before the extension existed answers about
	// the past, so its capability set alone would emit
	// `CREATE EXTENSION "timescaledb"` and then skip the create_hypertable that
	// needs it (stokaro/ptah#1026).
	//
	// The extensions come from the owner's changes rather than from the
	// desired schema because installing is what makes them available: an
	// extension the schema declares and the target already has is already in
	// the capability set the connection reported (stokaro/ptah#2315). An
	// owner's change answers for its own direction; see
	// [schemaext.CapabilityProvider].
	astNodes, err := GenerateSchemaDiffASTWithOptions(
		ctx, runtime,
		diff, dialect, opts,
	)
	if err != nil {
		return RenderedPlan{}, err
	}
	caps := schemaext.ProvidedByChanges(opts.CapabilitiesFor(dialect), diff.FeatureChanges)
	// The planner resolves built-in transport spellings before selecting a
	// target. Rendering must use that same target, not reinterpret the spelling.
	target := platform.NormalizeDialect(dialect)
	if target == "" {
		target = dialect
	}
	request := renderer.Request{Target: target, Capabilities: caps, Nodes: astNodes}
	output, err := renderer.Render(ctx, runtime, request)
	if err != nil {
		return RenderedPlan{}, wrapRenderError(dialect, err)
	}
	if err := ctx.Err(); err != nil {
		return RenderedPlan{}, wrapRenderError(dialect, err)
	}
	return RenderedPlan{Request: request, Result: output, Dialect: dialect}, nil
}

// GenerateSchemaDiffSQL generates complete SQL for schema differences as a single string.
//
// This function provides a mid-level interface that generates a complete SQL script
// containing all the statements needed to perform the migration. The output is a
// single string with multiple SQL statements separated by semicolons and newlines.
//
// # Parameters
//
//   - ctx: Caller context, required even for an empty change set
//   - runtime: Selected feature planning and rendering services
//   - diff: Schema differences identified by the schemadiff package
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
//	import "ptah.run/core/platform"
//
//	// Generate complete SQL script for PostgreSQL
//	sql, err := planner.GenerateSchemaDiffSQL(ctx, runtime, diff, platform.Postgres)
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
func GenerateSchemaDiffSQL(ctx context.Context, runtime Runtime, diff *difftypes.SchemaDiff, dialect string) (string, error) {
	return GenerateSchemaDiffSQLWithOptions(
		ctx, runtime,
		diff, dialect, planext.Options{
			Capabilities: capability.ForDialect(dialect),
		},
	)
}

// GenerateSchemaDiffSQLWithOptions generates complete SQL using explicit
// planning options.
//
// Rendering capabilities are widened with the extensions this plan itself
// installs, read from the owner's changes in diff.FeatureChanges: such an
// extension is in place before the statements that need it, so those
// statements render even when the capability set was resolved from a server
// that does not have the extension yet (stokaro/ptah#1026).
//
// Planning failures come back as *ptaherr.PlanError (see
// GenerateSchemaDiffASTWithOptions); rendering failures as
// *ptaherr.RenderError. Both carry the dialect and answer errors.As.
func GenerateSchemaDiffSQLWithOptions(
	ctx context.Context, runtime Runtime,
	diff *difftypes.SchemaDiff,
	dialect string,
	opts planext.Options,
) (string, error) {
	plan, err := GenerateSchemaDiffRenderedPlan(ctx, runtime, diff, dialect, opts)
	if err != nil {
		return "", err
	}
	return plan.SQL(), nil
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
