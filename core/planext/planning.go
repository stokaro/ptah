// Package planext defines common migration planning contracts. The selected
// runtime supplies implementations; the contract links no planner or comparator.
package planext

import (
	"context"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/platform/capability"
)

// ChangeKind identifies a destructive schema change kind that a diff policy can
// skip. The string values are the stable identifiers accepted in project config
// (ptah.yaml diff.skip) and are safe to render in migration comments.
type ChangeKind string

const (
	// DropTable skips dropping tables that exist in the database but not in the
	// target schema. Dependent object removals (indexes, constraints, triggers,
	// RLS policies, grants) for a kept table are skipped with it.
	DropTable ChangeKind = "drop_table"
	// DropColumn skips dropping columns from tables that exist in both schemas.
	DropColumn ChangeKind = "drop_column"
	// DropIndex skips dropping standalone indexes. Indexes that are being
	// replaced (dropped and recreated under the same name) are not affected.
	DropIndex ChangeKind = "drop_index"
	// DropEnum skips dropping enum types that exist in the database but not in
	// the target schema.
	DropEnum ChangeKind = "drop_enum"
)

// StatementStyle selects how a plan spells what its statements carry beyond
// the change itself: comments, existence guards and CASCADE. The style never
// changes which objects a plan creates, changes or drops on a database in the
// state the plan was made against.
type StatementStyle uint8

const (
	// StatementStyleAnnotated is the zero value and Ptah's own style. A plan
	// writes comments that describe its statements, guards a drop with IF
	// EXISTS and a new index with IF NOT EXISTS, and drops with CASCADE where
	// the target accepts it.
	StatementStyleAnnotated StatementStyle = iota
	// StatementStyleExact writes a plan that runs only against the database it
	// was made against. It leaves out the comments that only describe the
	// statements beside them (see [ast.CommentNode]), the IF EXISTS and IF
	// NOT EXISTS guards no earlier statement of the plan makes necessary, and
	// every CASCADE the plan does not rely on to remove a dependent it does
	// not drop itself. A statement run against a database in another state
	// then fails, where a guard would make it do nothing and CASCADE would
	// remove an object nobody named.
	//
	// A comment that reports what the plan left out or cannot do is written
	// in this style too. The PostgreSQL-family and MySQL-family planners mark
	// their descriptive comments, and the PostgreSQL-family planner also
	// settles guards and CASCADE; the other built-in planners plan as in the
	// annotated style. Feature owners that negotiate planning.statement-style
	// receive this choice and set their own guards and CASCADE accordingly.
	StatementStyleExact
)

// Planner defines the interface for database-specific migration planning.
//
// Implementations of this interface are responsible for converting schema differences
// into Abstract Syntax Tree (AST) nodes that represent the SQL operations needed to
// migrate from the current database schema to the desired schema.
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
//   - Refuse a diff they cannot plan into applicable SQL rather than emitting
//     statements the target would reject
//
// # Parameters
//
// The diff carries the schema operands. Context and runtime select the services
// and cancellation boundary. Each change entry carries its own operands
// (the difftypes Changes elements and Desired fields), and schema-wide
// vocabulary travels on the diff's Declared* carries, so an implementation
// reads everything it plans from the diff. The desired schema itself stays
// with the schemadiff package, which prepares and validates it where it is
// supplied and puts on the diff what planning needs; see
// GenerateSchemaDiffASTWithOptions.
//
// # Return Value
//
// Returns a slice of AST nodes representing the SQL operations needed for migration.
// The nodes are ordered to respect database dependencies and constraints.
//
// # Example Implementation Pattern
//
//	func (p *AcmePlanner) GenerateMigrationAST(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff) ([]ast.Node, error) {
//		var nodes []ast.Node
//
//		// 1. Create enum types first (PostgreSQL-style dialects)
//		nodes = append(nodes, p.planEnumAdditions(diff)...)
//
//		// 2. Create tables in dependency order
//		nodes = append(nodes, p.planTableAdditions(diff)...)
//
//		// 3. Add indexes and constraints
//		nodes = append(nodes, p.planIndexAdditions(diff)...)
//
//		return nodes, nil
//	}
type Planner interface {
	GenerateMigrationAST(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff) ([]ast.Node, error)
}

// Options configures high-level planner helpers.
type Options struct {
	// Capabilities describes the concrete target server. Built-in planners use
	// the dialect preset when it is empty. External providers receive the set
	// unchanged, so missing facts never select a built-in preset.
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
	// OnlineAlter asks the server to apply the plan without blocking the
	// writes already running against its tables, in whichever grammar the
	// target has: the MySQL-family ALGORITHM and LOCK clauses, or PostgreSQL's
	// NOT VALID constraint followed by its own validation.
	//
	// It is a request, not a prediction. A MySQL-family server refuses a
	// statement it cannot apply that way, so a plan built with this either
	// runs online or fails at the statement rather than under a table copy
	// nobody expected. A target without the grammar is planned as it would be
	// without this.
	OnlineAlter bool
	// SkipChangeKinds lists destructive change kinds the planner must omit from
	// the plan (emitting a clearly-marked comment in their place) instead of
	// deferring to the coarse destructive gate. Currently honored by the
	// PostgreSQL-family planner.
	SkipChangeKinds []ChangeKind
	// OmitNullBackfill plans a column made NOT NULL without first filling its
	// NULL rows from the column's declared default, so SET NOT NULL fails on a
	// NULL row whether or not the column declares one.
	//
	// The zero value keeps the fill, which only a PostgreSQL-family plan
	// writes: the value is the author's own, and the UPDATE is in the plan for
	// review. Atlas CE v1.3.0 writes no fill, and ptah-compat sets this under
	// its strict Community Edition policy to plan what that binary plans.
	OmitNullBackfill bool
	// AllowTableRebuild plans a change the target cannot make in place as an
	// explicit rebuild of the table: a new table written from the declaration,
	// a copy of the rows, and a swap of the two. On YDB it covers a changed
	// primary key, a column type change and a column made NOT NULL, and
	// without it each of those is refused with an error naming the way to ask,
	// [Options.TableRebuildRequest].
	//
	// The steps of a YDB rebuild are not atomic, and rows written to the table
	// between the copy and the swap are lost, which the plan says above the
	// steps. A table with a Serial column, or with a setting the read of the
	// database did not describe, is refused even when this is set. A target
	// without such a rebuild plans as it would without this; SQLite rebuilds a
	// table inside one transaction and needs no request.
	AllowTableRebuild bool
	// TableRebuildRequest is how the caller asks for AllowTableRebuild, and a
	// refusal of a change only a rebuild can make names it. Empty names the
	// native flag, [DefaultTableRebuildRequest]; a surface that has no such
	// flag, such as ptah-compat, passes the spelling it reads instead.
	TableRebuildRequest string
	// Statements selects the statement style. The zero value is
	// [StatementStyleAnnotated]. ptah-compat `migrate diff` selects
	// [StatementStyleExact], which is how the Atlas CLI writes a migration
	// file.
	Statements StatementStyle
}

// DefaultTableRebuildRequest is how the native commands ask for
// [Options.AllowTableRebuild]: the --allow-table-rebuild flag. An empty
// [Options.TableRebuildRequest] means it.
const DefaultTableRebuildRequest = "--allow-table-rebuild"

// TableRebuildWords returns how the caller asks for AllowTableRebuild, for a
// refusal to name: [Options.TableRebuildRequest], or
// [DefaultTableRebuildRequest] when it is empty.
func (o Options) TableRebuildWords() string {
	if o.TableRebuildRequest != "" {
		return o.TableRebuildRequest
	}
	return DefaultTableRebuildRequest
}

// CapabilitiesFor returns the configured capability set, falling back to the
// default preset for dialect when no explicit set was provided.
func (o Options) CapabilitiesFor(dialect string) capability.Capabilities {
	if o.Capabilities.Len() != 0 {
		return o.Capabilities
	}
	return capability.ForDialect(dialect)
}

// Factory creates a planner for a dialect from construction options.
type Factory func(Options) Planner

// Resolver selects a common planner from one frozen provider selection.
// Missing targets or services are refused without consulting a global registry.
type Resolver interface {
	MigrationPlanner(string, Options) (Planner, error)
}
