package generator

import (
	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// ReverseSchemaDiff returns the diff that undoes diff: the plan input a caller
// renders to obtain rollback SQL for a forward migration it has already
// planned.
//
// It is the rule this package has always applied to its own `.down.sql` half,
// exported so that a second writer of migration files can reach it. There is
// exactly one caller outside this package today —
// [go.5x5.cz/ptah/internal/atlasmigrate.DiffOptions.PlanReverse], which the
// Atlas-compatible `migrate diff` fills in — and the reason it cannot call the
// unexported original is an import edge rather than taste: `migration/generator`
// imports `internal/atlasmigrate` in ten places (the migration-directory lock,
// the publication primitives, the qualifier), so the dependency cannot be
// pointed the other way without moving those primitives out first
// (stokaro/ptah#1013).
//
// # Arguments
//
// diff is the FORWARD comparison, desired the schema it was computed against,
// and current the introspected database state it was computed FROM. All three
// are what a diff run already holds; nothing here needs recomputing.
//
// current is what makes the reverse restorable rather than merely opposite. A
// forward diff that widens a column records the new type only, so the reverse
// of it has to read the old one back off the pre-change database state; the
// same is true of a constraint the forward direction replaced. desired resolves
// object names the forward diff carries in reference form, such as an RLS
// policy's owning table.
//
// # What the caller still owes
//
// The result is a diff, not SQL. Rendering it is the caller's step, and the
// schema to render it AGAINST is the pre-change one — `current` converted to a
// [goschema.Database] — because the reverse restores that state rather than the
// desired one.
//
// A table the reverse RE-CREATES is restored by that CREATE TABLE, so the
// result deliberately does not also list the constraints the re-created body
// brings back with it: its primary key, and the field-level foreign keys the
// planner's new-table pass re-adds. Repeating them is not merely redundant —
// PostgreSQL refuses the second primary key outright, so a rollback carrying
// both aborts part-applied (stokaro/ptah#1013). Everything the re-created body
// does NOT restore, a CHECK above all, is still listed and still rendered.
//
// Two refinements this package applies to its own down direction are NOT
// included, because neither is expressible without the dialect, which this
// signature deliberately does not take:
//
//   - the MySQL-family backing index a dropped FOREIGN KEY leaves behind, and
//   - the concurrent-index refs a down file inherits from the up file's policy.
//
// A caller that needs either has to add it, and a caller on a dialect where
// neither applies — PostgreSQL, SQLite — needs neither.
//
// The returned diff is freshly allocated, but it shares slices with diff: the
// reversal is largely a swap of the added and removed lists. Callers must treat
// both as read-only.
func ReverseSchemaDiff(
	diff *types.SchemaDiff,
	desired *goschema.Database,
	current *dbschematypes.DBSchema,
) *types.SchemaDiff {
	if diff == nil {
		return nil
	}
	return reverseSchemaDiffWithSchema(diff, desired, current)
}
