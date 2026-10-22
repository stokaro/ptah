package migratorext

import (
	"context"
	"database/sql"
)

// RowsQueryer is what an index guard reads the catalog through.
type RowsQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// IndexRef is one index a migration's statement names, spelled the way the
// engine's catalog stores it. Schema is the schema of the target table, empty
// when the statement left the table unqualified.
type IndexRef struct {
	Schema      string
	Table       string
	Name        string
	IfNotExists bool
}

// IndexName names an index by its schema and its own name.
type IndexName struct {
	Schema string
	Name   string
}

// IndexName returns the schema-level name of the index ref names.
func (r IndexRef) IndexName() IndexName {
	return IndexName{Schema: r.Schema, Name: r.Name}
}

// IndexState is the catalog identity behind one index a statement creates:
// the table it targets, and the relation that holds its name. The kinds are
// the engine's own catalog codes, which only its guard interprets.
type IndexState struct {
	TargetFound        bool
	TargetSchema       string
	TargetTable        string
	TargetKind         string
	Name               string
	RelationExists     bool
	RelationKind       string
	IndexedTableSchema string
	IndexedTable       string
	Valid              bool
	Ready              bool
}

// IndexName returns the schema-level name of the index the state is about.
func (s IndexState) IndexName() IndexName {
	return IndexName{Schema: s.TargetSchema, Name: s.Name}
}

// IndexAttachment is an index a statement attaches to a constraint, which
// renames the index to the constraint's name.
type IndexAttachment struct {
	Index IndexRef
	Name  string
}

// IndexRefusal words a refusal to record a migration over an index the
// engine cannot use.
type IndexRefusal struct {
	// UnusableOutcome says what recording would do when every offending
	// index exists and is unusable.
	UnusableOutcome string
	// RetryAction says what to do once the indexes are rebuilt or dropped.
	RetryAction string
	// UnsafeOutcome says what recording would do when a relation is
	// missing or is not the intended index.
	UnsafeOutcome string
}

// IndexGuard keeps the migrator from recording a migration applied while an
// index one of its conditional CREATE INDEX statements names is not usable
// on its target, which happens on an engine where a failed build leaves an
// unusable index that still holds the name.
type IndexGuard interface {
	// ConditionalCreates returns the indexes the CREATE INDEX ... IF NOT
	// EXISTS statements of sqlText name, in order and without duplicates.
	ConditionalCreates(sqlText string) []IndexRef
	// Drops returns the indexes the DROP INDEX statements of sqlText name.
	Drops(sqlText string) []IndexRef
	// Attachments returns the indexes statement attaches to a constraint.
	Attachments(statement string) []IndexAttachment
	// SearchPath reports whether a replayed statement changes how an
	// unqualified name resolves, and whether the value it leaves is known.
	SearchPath(statement string) (changed, known bool)
	// ResolveTarget resolves the table ref targets as an unqualified name
	// resolves now. The state's TargetFound reports whether there is one.
	ResolveTarget(ctx context.Context, q RowsQueryer, ref IndexRef) (IndexState, error)
	// State reads ref's target and the relation that holds its name.
	State(ctx context.Context, q RowsQueryer, ref IndexRef) (IndexState, error)
	// TargetsAcrossSchemas returns every table in a user schema ref's
	// unqualified target could be.
	TargetsAcrossSchemas(ctx context.Context, q RowsQueryer, ref IndexRef) ([]IndexState, error)
	// ResolveDropped resolves the index a DROP INDEX names, reporting false
	// when nothing holds the name.
	ResolveDropped(ctx context.Context, q RowsQueryer, ref IndexRef) (IndexName, bool, error)
	// Usable reports whether state is an acceptable index on its target.
	Usable(state IndexState) bool
	// SameDefinition reports whether the usable index state names has the
	// definition the CREATE INDEX statement declares.
	SameDefinition(ctx context.Context, q RowsQueryer, statement string, state IndexState) (bool, error)
	// ApplyError refuses to apply migration version over unsafe.
	ApplyError(version int64, unsafe []IndexState) error
	// RepairError refuses to record migration version over unsafe.
	RepairError(version int64, unsafe []IndexState, refusal IndexRefusal) error
}
