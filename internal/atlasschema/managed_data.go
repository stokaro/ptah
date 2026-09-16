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
) ([]dataStatement, error) {
	if conn == nil || desired == nil || len(desired.ManagedData) == 0 {
		return nil, nil
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
	for _, declaration := range declarations {
		declared, err := managedDataDiff(ctx, conn, declaration, desired, current, projectRoot)
		if err != nil {
			return nil, err
		}
		for _, statement := range declared {
			phases[statement.phase] = append(phases[statement.phase], statement)
		}
	}
	// A deletion runs against the constraint from the other side: the row that
	// references has to go before the row it references, so this phase alone
	// reads the declaration order backwards.
	slices.Reverse(phases[phaseDelete])
	statements := make([]dataStatement, 0, len(declarations))
	for _, phase := range dataPhases {
		statements = append(statements, phases[phase]...)
	}
	return statements, nil
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
) ([]dataStatement, error) {
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
			return nil, fmt.Errorf(
				"managed data for table %s was never read; a desired state that declares rows has to carry them",
				qualified,
			)
		}
		rows, err := declaredRows(declaration, qualified, projectRoot)
		if err != nil {
			return nil, err
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
		return nil, err
	}
	diff, err := managedrows.Compare(ctx, conn, managedrows.Request{
		Desired:     desired,
		Declaration: declaration,
		Rows:        rows,
		Live:        current,
		// A table this plan is about to create holds nothing, and reading it
		// would fail rather than answer. Every declared row is an insert then.
		CatalogIsComplete: true,
		Intent:            managedrows.Report,
	})
	if err != nil {
		return nil, err
	}
	// The statements come from the renderer as a list. Its script form cannot
	// be cut back into statements at line breaks: a declared value may carry a
	// newline, which is legal inside a literal and is the byte the script joins
	// statements with (stokaro/ptah#3278).
	up, _, err := datadiff.RenderStatements(diff, conn.Info().Dialect)
	if err != nil {
		return nil, fmt.Errorf("render managed rows of %s: %w", qualified, err)
	}
	return classifyDataStatements(up, qualified), nil
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
