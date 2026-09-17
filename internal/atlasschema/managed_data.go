package atlasschema

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dataorder"
	"ptah.run/internal/managedrows"
	"ptah.run/internal/pathguard"
	"ptah.run/internal/protectedtable"
	"ptah.run/migration/datadiff"
	"ptah.run/migration/safety"
)

// dataPhase is which of the three passes a statement belongs to. The order of
// the constants is the order the passes run in, and dataPhases is that order
// written once so a reader and the loop cannot disagree.
type dataPhase int

const (
	phaseInsert dataPhase = iota
	phaseUpdate
	phaseDelete
)

var dataPhases = []dataPhase{phaseInsert, phaseUpdate, phaseDelete}

// dataStatement is one statement reconciling declared rows, carrying the
// severity this package assigns it rather than the one a SQL analyzer would,
// and the pass it runs in.
//
// The analyzer reads a DELETE of a reference row as safe, because it does not
// remove a table or tighten a constraint. That is true about the schema and
// false about the rows, and a plan whose deletions read as safe is a plan an
// approval policy waves through.
type dataStatement struct {
	sql      string
	phase    dataPhase
	severity safety.Severity
	reason   string
}

// managedDataStatements reconciles every declared row set against the database.
//
// It runs whether or not the schema diff has changes: a release that only edits
// a reference row changes no DDL, and a planner that skipped the data stage
// then would report an empty plan for a change the author made.
//
// The statements come out in the order the foreign keys allow, not in the order
// the tables happen to be named: every INSERT and UPDATE parents-first, then
// every DELETE children-first. Grouping by table instead puts a child's rows in
// front of the parent's whenever the child sorts first — "countries" before
// "regions" — and the constraint the same plan just created refuses them
// (stokaro/ptah#3252). The rank is dataorder's, which is what the migration
// body reads, so a plan and a migration cannot disagree about which row goes in
// first.
func managedDataStatements(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	current *catalog.Database,
	projectRoot string,
	protected protectedtable.Set,
) ([]dataStatement, []PlanRowSet, error) {
	if conn == nil || desired == nil || len(desired.ManagedData) == 0 {
		return nil, nil, nil
	}
	ranker := dataorder.New(desired)
	declarations := slices.Clone(desired.ManagedData)
	// One order for the statements a plan records, so two runs over one
	// declaration produce one plan. Tables the schema does not define rank
	// together at the end and keep their names' order.
	sort.SliceStable(declarations, func(i, j int) bool {
		left, right := declarations[i], declarations[j]
		leftRank := ranker.Rank(left.Schema, left.Table)
		rightRank := ranker.Rank(right.Schema, right.Table)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		return managedDataTableName(left) < managedDataTableName(right)
	})
	phases := make([][]dataStatement, len(dataPhases))
	var fenced []string
	// covered names every row set this plan read, in the order it read them. A
	// saved plan records it, so applying the plan can read the same rows again
	// and refuse when they moved (stokaro/ptah#3378).
	var covered []PlanRowSet
	for _, declaration := range declarations {
		declared, read, err := managedDataDiff(ctx, conn, declaration, desired, current, projectRoot)
		if err != nil {
			return nil, nil, err
		}
		if read.Performed {
			covered = append(covered, PlanRowSet{
				Schema:  declaration.Schema,
				Table:   declaration.Table,
				Keys:    slices.Clone(declaration.Keys),
				Columns: read.Columns,
			})
		}
		// A fenced table is read the same way the migration body reads it, and
		// only where this plan would change it: an entry on a table the
		// declaration already agrees with refuses nothing, which is what lets a
		// fence sit in a configuration permanently.
		if len(declared) > 0 {
			if _, ok := protected.Entry(declaration.Schema, declaration.Table); ok {
				fenced = append(fenced, managedDataTableName(declaration))
				continue
			}
		}
		for _, statement := range declared {
			phases[statement.phase] = append(phases[statement.phase], statement)
		}
	}
	if len(fenced) > 0 {
		slices.Sort(fenced)
		return nil, nil, &ProtectedTableError{Tables: slices.Compact(fenced)}
	}
	// A deletion runs against the constraint from the other side: the row that
	// references has to go before the row it references, so this phase alone
	// reads the declaration order backwards.
	slices.Reverse(phases[phaseDelete])
	statements := make([]dataStatement, 0, len(declarations))
	for _, phase := range dataPhases {
		statements = append(statements, phases[phase]...)
	}
	return statements, covered, nil
}

// ProtectedTableError reports that a plan would change a table the caller
// fenced off with [ApplyOptions.ProtectedTables].
//
// It carries no override, and that is the point of it rather than an omission.
// A severity is a question put to a policy, and every mechanism that rates a
// statement can answer yes: an approval, a flag, a policy that permits
// destructive changes. A fenced table is the statement that no such yes exists
// for it, so a refusal a caller could wave through would be the severity it
// already has (stokaro/ptah#3362).
//
// Where the change is wanted, the fence is what changes: the entry goes, or the
// rows are written as a migration through `ptah migrations data`, which is the
// path that asks a person for `--allow-prod`.
type ProtectedTableError struct {
	// Tables are the fenced tables this plan would change, qualified as the
	// declaration names them and sorted.
	Tables []string
}

func (e *ProtectedTableError) Error() string {
	return fmt.Sprintf(
		"refusing to change protected table(s) %s: a protected table is fenced off from the declarative path, "+
			"which has no override; drop the entry to plan the change, or write the rows as a migration with "+
			"`ptah migrations data --allow-prod`",
		strings.Join(e.Tables, ", "),
	)
}

// declaredRows reads the rows a declaration still names, bounded by the project
// the caller is operating in.
//
// The path is data: a schema says which file carries its rows, and a schema is
// not always one the reader wrote. Following it anywhere would let a desired
// state read whatever this process can, on every plan and apply rather than
// only where an author publishes their own work. The boundary is the one
// `file()` in atlas.hcl already has, and it is the project rather than the
// declaration's own directory because `schema export` writes a path back out of
// the directory it exports into, and refusing Ptah's own output would be a rule
// that contradicts the tool applying it.
func declaredRows(
	declaration schemamodel.ManagedData,
	qualified, projectRoot string,
) ([]schemamodel.ManagedRow, error) {
	sourceDir := declaration.SourceDir
	if sourceDir == "" {
		sourceDir = "."
	}
	if _, err := pathguard.ResolveWithinRoot(
		filepath.Join(sourceDir, declaration.File), projectRoot,
	); err != nil {
		return nil, fmt.Errorf("managed data file %q for table %s: %w", declaration.File, qualified, err)
	}
	return schemamodel.LoadManagedRowValues("", declaration)
}

// managedDataDiff reconciles one declaration against the database and renders
// the statements that close the difference.
//
// The comparison is managedrows.Compare, which the migration body reads too, so
// a plan and a migration cannot disagree about which live table a declaration
// names, which of its columns are read, or which rows differ
// (stokaro/ptah#3276). What is left here is what a plan does with the answer:
// render the statements and rate them.
func managedDataDiff(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	declaration schemamodel.ManagedData,
	desired *schemamodel.Database,
	current *catalog.Database,
	projectRoot string,
) ([]dataStatement, managedrows.Read, error) {
	qualified := managedDataTableName(declaration)
	if declaration.Rows == nil {
		// The rows are read here because this is the function that cannot go on
		// without them. An artifact carries them in its own layer and arrives
		// with Rows already set; a working copy carries a file name, and only
		// publication used to resolve it, so `schema plan` and `schema apply`
		// refused a row set sitting next to the schema they were planning
		// (stokaro/ptah#3269). Reading it at the refusal leaves one place that
		// knows a declaration can still become rows.
		//
		// A declaration that names no file is the artifact case gone wrong: the
		// layer was dropped on the way here, and there is nothing to resolve.
		if declaration.File == "" {
			return nil, managedrows.Read{}, fmt.Errorf(
				"managed data for table %s was never read; a desired state that declares rows has to carry them",
				qualified,
			)
		}
		rows, err := declaredRows(declaration, qualified, projectRoot)
		if err != nil {
			return nil, managedrows.Read{}, err
		}
		// A file that declares no rows is an empty row set, not an unread one.
		// The difference is the whole statement: nil means nobody read the file,
		// and planning an empty set deletes every row the table holds.
		if rows == nil {
			rows = make([]schemamodel.ManagedRow, 0)
		}
		declaration.Rows = rows
	}
	// The declared scalars resolve the way the YAML resolver resolves them,
	// which is what the row report and the migration body resolve them with. A
	// second resolver here read a timestamp back as its source text, so a
	// declared moment never paired with the moment a driver returns and the two
	// stages answered differently about one converged row (stokaro/ptah#3276).
	rows, err := schemamodel.ResolveManagedRows(declaration)
	if err != nil {
		return nil, managedrows.Read{}, err
	}
	var read managedrows.Read
	diff, err := managedrows.Compare(ctx, conn, managedrows.Request{
		Desired:     desired,
		Declaration: declaration,
		Rows:        rows,
		Live:        current,
		// A table this plan is about to create holds nothing, and reading it
		// would fail rather than answer. Every declared row is an insert then.
		CatalogIsComplete: true,
		// The schema stage of this plan runs before its data stage, so the
		// comparison is against the table the plan produces rather than the one
		// it starts from.
		Intent: managedrows.Plan,
		Read:   &read,
	})
	if err != nil {
		return nil, managedrows.Read{}, err
	}
	// The statements come from the renderer as a list. Its script form cannot
	// be cut back into statements at line breaks: a declared value may carry a
	// newline, which is legal inside a literal and is the byte the script joins
	// statements with (stokaro/ptah#3278).
	up, _, err := datadiff.RenderStatements(diff, conn.Info().Dialect)
	if err != nil {
		return nil, managedrows.Read{}, fmt.Errorf("render managed rows of %s: %w", qualified, err)
	}
	return classifyDataStatements(up, qualified), read, nil
}

// classifyDataStatements assigns the severity a reference-row change actually
// carries.
//
// An INSERT adds what the declaration asks for. An UPDATE overwrites a value
// the database holds, and a DELETE removes a row entirely: both are losses a
// policy must be able to refuse, and neither is destructive to the schema, so
// nothing in the DDL analyzer would have said so.
//
// Each element of up is one whole statement, so its leading keyword is the
// statement's own and not that of a fragment cut out of a literal.
func classifyDataStatements(up []string, qualified string) []dataStatement {
	statements := make([]dataStatement, 0, len(up))
	for _, statement := range up {
		trimmed := strings.TrimSpace(statement)
		declared := dataStatement{
			sql:      trimmed,
			phase:    phaseInsert,
			severity: safety.Safe,
			reason:   fmt.Sprintf("inserts declared rows into %s", qualified),
		}
		switch {
		case strings.HasPrefix(strings.ToUpper(trimmed), "DELETE"):
			declared.phase = phaseDelete
			declared.severity = safety.Destructive
			declared.reason = fmt.Sprintf("removes a row from %s that the declaration no longer holds", qualified)
		case strings.HasPrefix(strings.ToUpper(trimmed), "UPDATE"):
			declared.phase = phaseUpdate
			declared.severity = managedrows.PlanUpdateSeverity
			declared.reason = fmt.Sprintf("overwrites managed columns of a row in %s", qualified)
		}
		statements = append(statements, declared)
	}
	return statements
}

// managedDataColumns is what is read back from the database: the key columns
// and the managed ones, and nothing else.
//
// A column no declaration names is not read and never written. Reconciling a
// reference table is not permission to rewrite the columns beside it.
func managedDataTableName(declaration schemamodel.ManagedData) string {
	if declaration.Schema == "" {
		return declaration.Table
	}
	return declaration.Schema + "." + declaration.Table
}
