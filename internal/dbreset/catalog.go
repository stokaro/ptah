package dbreset

import (
	"context"
	"database/sql"
)

// CleanupCatalog is what an engine's catalog answers a cleanup plan where the
// schema reader cannot: the objects the writer's cleanup destroys that the
// reader does not describe, how the views a plan drops read through each
// other, and the locks a scoped plan holds before it drops anything. A writer
// offers one for the engines whose catalog it was measured against;
// internal/schemaclean asks the writer and names no engine of its own.
//
// Each read runs through the querier the caller hands it, so it runs on the
// caller's session, inside the caller's transaction where there is one.
type CleanupCatalog interface {
	// Objects lists the objects of schema that the writer's cleanup destroys
	// and the schema reader does not describe, each with the statement that
	// drops it where the engine names one.
	Objects(ctx context.Context, q Querier, schema string) ([]CleanupObject, error)
	// ViewDepths answers, for each view and materialized view of schema, how
	// many views it reads through, so a plan drops the deepest first. Nil
	// means the engine orders nothing beyond the plan's kinds.
	ViewDepths(ctx context.Context, q Querier, schema string) ([]ViewDepth, error)
	// LockRelations takes, through executor, the locks a scoped cleanup holds
	// on the tables it drops before it drops anything. It does nothing for
	// an empty list, and an engine that takes no lock does nothing at all.
	LockRelations(ctx context.Context, executor Executor, relations []Relation) error
}

// CleanupObject is an object a cleanup plan lists from the catalog.
type CleanupObject struct {
	// Kind is the plan's name for the kind, such as "sequence",
	// "foreign_table" or "event".
	Kind string
	// Schema is the schema the object is in, empty where the engine's plan
	// names none.
	Schema string
	// Table is the table that owns the object, such as the table whose
	// column a sequence serves, and empty for none.
	Table string
	// Name is the object's name, a routine's with its argument list.
	Name string
	// SelectorName is the name a selection matches the object by, a
	// routine's without its argument list. Empty means Name.
	SelectorName string
	// Implicit reports that the object goes with the one that owns it, such
	// as a sequence an identity or serial column created.
	Implicit bool
	// Statement drops the object. Empty leaves the plan to render one.
	Statement string
}

// ViewDepth is how many views one view reads through.
type ViewDepth struct {
	// Kind is "view" or "materialized_view".
	Kind  string
	Name  string
	Depth int
}

// Relation is a table a scoped cleanup drops.
type Relation struct {
	Schema string
	Name   string
}

// Dependent is a relation defined in terms of a relation a scoped cleanup
// selects, such as a view that reads a selected table.
type Dependent struct {
	// Source is the selected relation the dependent reads.
	Source string
	// Name is the dependent's name.
	Name string
	// Kind spells the dependent's kind for a diagnostic, such as "view" or
	// "materialized view".
	Kind string
}

// Querier runs a catalog read.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Executor runs a statement inside the scoped cleanup's transaction.
type Executor interface {
	ExecuteSQL(ctx context.Context, statement string, args ...any) error
}
