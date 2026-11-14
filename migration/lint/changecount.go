package lint

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"

	"ptah.run/core/ast"
	"ptah.run/internal/lintkit"
)

// ChangeCount selects how [File.Changes] counts the schema changes of an up
// file. The zero value is [ChangeCountStatements].
type ChangeCount string

const (
	// ChangeCountStatements counts the changes each statement makes, to every
	// kind of object Ptah models. A statement the starting state shows to
	// change nothing counts nothing where the run supplied that state.
	ChangeCountStatements ChangeCount = ""
	// ChangeCountAtlas counts the way the community Atlas binary's
	// `migrate lint` summary does, for every kind of object Ptah models. It
	// differs from [ChangeCountStatements] on a directory's first file when
	// that file holds more than ten statements: such a file counts one change
	// per object it leaves behind, rather than one per change each statement
	// makes. It also asks for the starting state of every version holding a
	// statement that state may show to change nothing, through
	// [Analysis.BaselineVersions].
	ChangeCountAtlas ChangeCount = "atlas"
	// ChangeCountAtlasCommunity counts as [ChangeCountAtlas] does, and only
	// the changes to kinds of object the community Atlas binary reads off the
	// dev database: tables with their columns, constraints, indexes and
	// comments, schemas, and enum types. A change to a view, a materialized
	// view, a trigger, a routine, a sequence, a policy, a role, a privilege,
	// row-level security, or an object an engine owner's statement adds
	// counts nothing.
	ChangeCountAtlasCommunity ChangeCount = "atlas-community"
)

// validateChangeCount refuses a count the package does not define, rather than
// counting some other way than the caller asked.
func validateChangeCount(count ChangeCount) error {
	switch count {
	case ChangeCountStatements, ChangeCountAtlas, ChangeCountAtlasCommunity:
		return nil
	default:
		return fmt.Errorf("unsupported schema change count %q", count)
	}
}

// readsStartingState reports a count that asks for the starting state of the
// statements whose change it decides.
func (c ChangeCount) readsStartingState() bool {
	return c != ChangeCountStatements
}

// objectKind is the kind of object a schema change lands on. The counts read
// it, and [SchemaChange] does not carry it.
type objectKind uint8

const (
	// kindTablePart is a part of a table other than a column or an index: a
	// constraint, an ALTER TABLE action Ptah models no further, an engine
	// owner's operation inside ALTER TABLE, or a comment on the table or one
	// of its columns.
	kindTablePart objectKind = iota
	kindTable
	kindColumn
	kindIndex
	kindSchema
	kindType
	kindSequence
	kindView
	kindMaterializedView
	kindTrigger
	kindRoutine
	kindPolicy
	kindRole
	kindPrivilege
	kindRowSecurity
	// kindOwnerStatement is an object an engine owner's own statement adds,
	// drops or changes, such as a PostgreSQL extension, composite type,
	// domain or policy: the owner reports the change and not the kind of
	// object it lands on.
	kindOwnerStatement
	// kindComment is a comment on anything but a table or a column.
	kindComment
)

// standalone reports a kind whose objects exist on their own. A count of what
// a file leaves behind counts each such object once; an object of any other
// kind is part of one of them, its table most often, and is counted with it.
func (k objectKind) standalone() bool {
	switch k {
	case kindTable, kindSchema, kindType, kindSequence, kindView, kindMaterializedView,
		kindTrigger, kindRoutine, kindPolicy, kindRole, kindOwnerStatement:
		return true
	default:
		return false
	}
}

// communityInspects reports a kind of object the community binary reads off
// the dev database, so a change to one counts in its summary. Measured against
// the pinned community binary v1.3.0, one statement per version after a
// version that creates a table: on SQLite, a CREATE TRIGGER and a CREATE VIEW
// count no change; on PostgreSQL, CREATE VIEW, CREATE MATERIALIZED VIEW,
// CREATE TRIGGER, CREATE FUNCTION, CREATE PROCEDURE, CREATE SEQUENCE, CREATE
// POLICY, a composite type, a domain, CREATE EXTENSION, GRANT, REVOKE, ALTER
// TABLE ... ENABLE ROW LEVEL SECURITY, a COMMENT ON VIEW and a COMMENT ON
// FUNCTION count none, where an enum type, a COMMENT ON TABLE, a COMMENT ON
// COLUMN, a COMMENT ON INDEX and an exclusion constraint count one; on MySQL,
// a view, a trigger and a procedure count none.
func (k objectKind) communityInspects() bool {
	switch k {
	case kindTable, kindColumn, kindTablePart, kindIndex, kindSchema, kindType:
		return true
	default:
		return false
	}
}

// countedChange is a schema change and what the counts read about the object
// it lands on.
type countedChange struct {
	SchemaChange
	// on is the kind of object the change lands on.
	on objectKind
	// parent is the folded reference of the table an index or a part of a
	// table belongs to, empty where the statement names none.
	parent string
	// guarded reports a change written IF EXISTS or IF NOT EXISTS, which the
	// engine skips when the object is already absent or present.
	guarded bool
}

// alterOperationObject is the kind of object an ALTER TABLE action other than
// a table rename lands on.
func alterOperationObject(op ast.AlterOperation) objectKind {
	switch op.(type) {
	case *ast.AddColumnOperation, *ast.DropColumnOperation, *ast.ModifyColumnOperation,
		*ast.AlterGeneratedColumnExpressionOperation, *ast.RenameColumnOperation:
		return kindColumn
	default:
		return kindTablePart
	}
}

// alterOperationGuarded reports an ALTER TABLE action written IF EXISTS or IF
// NOT EXISTS: an ADD COLUMN IF NOT EXISTS or a DROP COLUMN IF EXISTS.
func alterOperationGuarded(op ast.AlterOperation) bool {
	switch o := op.(type) {
	case *ast.AddColumnOperation:
		return o.IfNotExists
	case *ast.DropColumnOperation:
		return o.IfExists
	default:
		return false
	}
}

// commentObject is the kind of object a COMMENT ON statement lands on, read
// off its words: a comment on a table or a column is part of the table, a
// comment on an index part of the index, and any other comment
// [kindComment].
func commentObject(w []string) objectKind {
	if len(w) < 3 || w[0] != "COMMENT" || w[1] != "ON" {
		return kindComment
	}
	switch w[2] {
	case "TABLE", "COLUMN":
		return kindTablePart
	case "INDEX":
		return kindIndex
	default:
		return kindComment
	}
}

// largeFileStatements is the number of statements above which a directory's
// first file counts what it leaves behind under the Atlas counts.
//
// Measured against the pinned community binary v1.3.0 on SQLite, PostgreSQL
// and MySQL, a directory whose only file creates tables t1 to t10 and then
// creates and drops a table tmp says 10 schema changes, one per table left; the
// same file with t1 to t9 (eleven statements) says 9, and with t1 to t8 (ten
// statements) says 10, one per change of each statement. An INSERT counts
// toward the eleven: t1 to t8, the pair and one INSERT say 8. The first file
// of a directory meets this whatever --latest selects, and no later file does:
// the same twelve statements as a second file say 12, after a first file that
// creates one table or that creates and drops one, and after a first file
// holding only a SELECT.
const largeFileStatements = 10

// fileSchemaChanges returns the schema changes an up file counts under count,
// the statements the scope removed entirely, and the statements whose change
// only the starting state decides.
//
// opensDirectory reports the file whose version comes first in the directory,
// which starts from an empty schema.
func fileSchemaChanges(
	file *File,
	dialect string,
	scope schemaScope,
	compatibility CompatibilityProfile,
	count ChangeCount,
	opensDirectory bool,
) ([]SchemaChange, map[int]bool, []int) {
	changes, excluded := extractSchemaChanges(file, dialect, scope)
	leaves := count.readsStartingState() && opensDirectory && len(file.Statements) > largeFileStatements
	var decided []int
	if !leaves {
		decided = stateDecidedStatements(file, changes)
	}
	changes = slices.DeleteFunc(changes, func(change countedChange) bool {
		return changesNothing(file, change)
	})
	if count == ChangeCountAtlasCommunity {
		changes = slices.DeleteFunc(changes, func(change countedChange) bool {
			return !change.on.communityInspects()
		})
	}
	if leaves {
		changes = leftBehind(changes)
	}
	counted := make([]SchemaChange, 0, len(changes))
	for _, change := range changes {
		counted = append(counted, change.SchemaChange)
	}
	if compatibility == CompatibilityProfileAtlas && lintOf(dialect).AtlasRebuildColumnDiff && !leaves {
		counted = foldAtlasReplacements(file, counted)
	}
	return counted, excluded, decided
}

// leftBehind returns, for a file that starts from an empty schema, one change
// per standalone object the file leaves behind, at the statement that brought
// it in, in statement order. A part of an object, such as a column, an index
// or a comment, is counted with the object, and an object the file creates and
// drops again is not counted at all.
//
// Measured against the pinned community binary v1.3.0, on a directory whose
// only file creates tables t1 to t10 and adds one more statement: a CREATE
// INDEX, an ALTER TABLE ... ADD COLUMN, an ALTER TABLE t1 RENAME TO r1, a
// COMMENT ON TABLE and a DROP TABLE IF EXISTS of a table that is not there
// each say 10 schema changes. A CREATE VIEW, a CREATE TRIGGER, a CREATE
// SEQUENCE and a CREATE FUNCTION say 10 as well, kinds
// [objectKind.communityInspects] leaves out, and a CREATE TYPE ... AS ENUM on
// PostgreSQL says 11.
func leftBehind(changes []countedChange) []countedChange {
	type identity struct {
		on   objectKind
		name string
	}
	alive := make(map[identity]countedChange)
	var order []identity
	for _, change := range changes {
		if !change.on.standalone() {
			continue
		}
		id := identity{on: change.on, name: lintkit.NormalizeReference(change.Object)}
		if id.name == "" {
			// An object the parser gave no name is its own object.
			id.name = "\x00" + strconv.Itoa(change.StatementIndex)
		}
		switch change.Kind {
		case SchemaChangeAdd:
			if _, ok := alive[id]; !ok {
				alive[id] = change
				order = append(order, id)
			}
		case SchemaChangeDrop:
			delete(alive, id)
		case SchemaChangeModify, SchemaChangeRename:
			// A change in place lands on an object the file brought in.
		}
	}
	left := make([]countedChange, 0, len(alive))
	emitted := make(map[identity]bool, len(alive))
	for _, id := range order {
		change, ok := alive[id]
		if !ok || emitted[id] {
			continue
		}
		emitted[id] = true
		left = append(left, change)
	}
	slices.SortStableFunc(left, func(a, b countedChange) int { return cmp.Compare(a.StatementIndex, b.StatementIndex) })
	return left
}

// openingFile returns the position in files of the up file whose version comes
// first in the directory, or -1 when no up file carries a version. A
// repeatable file runs after every versioned one, so it never opens the
// directory.
func openingFile(files []File) int {
	opening := -1
	for i := range files {
		file := &files[i]
		if !file.IsUp || file.Version == 0 || file.Repeatable {
			continue
		}
		if opening < 0 || file.Version < files[opening].Version ||
			file.Version == files[opening].Version && file.Name < files[opening].Name {
			opening = i
		}
	}
	return opening
}

// countReadsStartingState reports whether count asks for this file's starting
// state: a selected up file holding a statement whose change only that state
// decides.
func countReadsStartingState(file *File, opts Options) bool {
	return opts.ChangeCount.readsStartingState() && file.Selected && file.IsUp && file.Version != 0 &&
		len(file.stateDecided) > 0
}
