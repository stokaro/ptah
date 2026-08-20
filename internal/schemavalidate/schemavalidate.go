// Package schemavalidate reports structural problems in a desired schema
// without a database.
//
// It sits below the CLI because the question means something without Atlas: a
// schema that names a column no table declares is wrong whoever asks. The
// renderer already refuses a schema it cannot render, but it refuses the first
// problem it meets and only while producing SQL, so the answer arrives after
// output the caller has to read past. Indexes are not covered there at all --
// an index on a column, or a table, that no declaration mentions renders
// happily (stokaro/ptah#1711).
package schemavalidate

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// Problem is one structural fault found in a desired schema.
type Problem struct {
	// Dialect is the target the schema was validated against.
	Dialect string
	// Kind names the declaration the fault belongs to, such as "index" or
	// "schema" for a whole-schema fault.
	Kind string
	// Object is the declaration's own name, empty for a whole-schema fault.
	Object string
	// Message states the fault.
	Message string
}

// String renders a problem as one diagnostic line.
func (p Problem) String() string {
	if p.Object == "" {
		return fmt.Sprintf("%s: %s: %s", p.Dialect, p.Kind, p.Message)
	}
	return fmt.Sprintf("%s: %s %q: %s", p.Dialect, p.Kind, p.Object, p.Message)
}

// Collect reports every structural problem it can find in database for one
// dialect, against that dialect's default capability preset.
func Collect(database *goschema.Database, dialect string) []Problem {
	return CollectWithCapabilities(database, dialect, capability.ForDialect(dialect))
}

// CollectWithCapabilities reports every structural problem it can find against
// a concrete capability set.
//
// The checks are ordered cheapest first and none of them stops the others: a
// caller asking what is wrong with a schema wants the list, not the first
// entry. The renderer's own validation contributes at most one problem,
// because it is fail-fast by construction.
func CollectWithCapabilities(
	database *goschema.Database,
	dialect string,
	caps capability.Capabilities,
) []Problem {
	if database == nil {
		return []Problem{{
			Dialect: dialect,
			Kind:    "schema",
			Message: "no schema was loaded",
		}}
	}
	// Scoped first, for the same reason the renderer scopes before validating:
	// a declaration this dialect was not given is not part of its desired
	// state, so faulting it here would refuse what the operator excluded.
	scoped := goschema.ScopeToDialect(database, dialect)
	problems := collectIndexProblems(scoped, dialect)
	if err := renderer.ValidateSchemaWithCapabilities(scoped, dialect, caps); err != nil {
		problems = append(problems, Problem{
			Dialect: dialect,
			Kind:    "schema",
			Message: err.Error(),
		})
	}
	return problems
}

// collectIndexProblems checks every index against the relation it belongs to.
//
// This is the check the renderer does not make. An index whose owner resolves
// to nothing falls back to the Go struct name and renders as `ON "Struct"`,
// which the server answers at apply time; an index naming a column the table
// does not declare renders and fails the same way.
func collectIndexProblems(database *goschema.Database, dialect string) []Problem {
	if len(database.Indexes) == 0 {
		return nil
	}
	owners := goschema.ResolveIndexOwners(database.Indexes, database.Tables, database.MaterializedViews)
	columnsByTable := indexableColumns(database)
	var problems []Problem
	for position, index := range database.Indexes {
		owner := owners[position]
		columns, known := columnsByTable[owner]
		if !known {
			if isMaterializedViewOwner(database, owner) {
				// A materialized view's columns come from its query, which is
				// opaque here, so only its existence is checkable.
				continue
			}
			problems = append(problems, Problem{
				Dialect: dialect,
				Kind:    "index",
				Object:  indexName(index, position),
				Message: fmt.Sprintf(
					"names table %q, which no declaration defines",
					declaredOwner(index, owner),
				),
			})
			continue
		}
		problems = append(problems, missingIndexColumns(index, position, owner, columns, dialect)...)
	}
	return problems
}

// missingIndexColumns reports every column an index names that its table does
// not declare.
func missingIndexColumns(
	index goschema.Index,
	position int,
	owner string,
	columns []string,
	dialect string,
) []Problem {
	var problems []Problem
	for _, column := range indexColumnNames(index) {
		if slices.ContainsFunc(columns, func(declared string) bool {
			return strings.EqualFold(declared, column)
		}) {
			continue
		}
		problems = append(problems, Problem{
			Dialect: dialect,
			Kind:    "index",
			Object:  indexName(index, position),
			Message: fmt.Sprintf("names column %q, which table %q does not declare", column, owner),
		})
	}
	return problems
}

// indexColumnNames collects the column names an index refers to.
//
// Parts wins when it is populated, because the two spellings are not
// alternatives: a declaration that fills Parts fills Fields from the same
// loop, and for an expression key it puts the whole expression in Fields.
// Reading both would report `lower(total)` as a column no table declares --
// which is what a functional index looked like before this preferred Parts.
// An index with no Parts carries plain column names in Fields.
func indexColumnNames(index goschema.Index) []string {
	names := make([]string, 0, len(index.Fields)+len(index.Parts)+len(index.IncludeColumns))
	if len(index.Parts) > 0 {
		for _, part := range index.Parts {
			// An expression key names no column at all.
			if strings.TrimSpace(part.Expr) != "" || strings.TrimSpace(part.Name) == "" {
				continue
			}
			names = append(names, part.Name)
		}
	} else {
		names = append(names, index.Fields...)
	}
	names = append(names, index.IncludeColumns...)
	return names
}

// indexableColumns maps each table's qualified name to the columns an index on
// it may name, embedded fields included.
func indexableColumns(database *goschema.Database) map[string][]string {
	fields := fromschema.ProcessEmbeddedFields(database.EmbeddedFields, database.Fields)
	byStruct := make(map[string][]string, len(database.Tables))
	for _, field := range fields {
		byStruct[field.StructName] = append(byStruct[field.StructName], field.Name)
	}
	columns := make(map[string][]string, len(database.Tables))
	for _, table := range database.Tables {
		columns[table.QualifiedName()] = byStruct[table.StructName]
	}
	return columns
}

// isMaterializedViewOwner reports whether the resolved owner names a declared
// materialized view.
func isMaterializedViewOwner(database *goschema.Database, owner string) bool {
	return slices.ContainsFunc(database.MaterializedViews, func(view goschema.MaterializedView) bool {
		return view.Name == owner || view.StructName == owner
	})
}

// indexName names an index for a diagnostic, falling back to its position when
// the declaration carries no name.
func indexName(index goschema.Index, position int) string {
	if strings.TrimSpace(index.Name) != "" {
		return index.Name
	}
	return fmt.Sprintf("#%d", position+1)
}

// Dialects normalizes and de-duplicates the dialects a run validates against,
// preserving the order they were given in.
func Dialects(requested []string) []string {
	out := make([]string, 0, len(requested))
	for _, dialect := range requested {
		normalized := platform.NormalizeDialect(strings.TrimSpace(dialect))
		if normalized == "" || slices.Contains(out, normalized) {
			continue
		}
		out = append(out, normalized)
	}
	return out
}

// declaredOwner names the relation an index meant, for a diagnostic about an
// owner that resolved to nothing.
//
// The resolver returns an empty string when it can match no relation, and
// echoing that back asks the reader to fix a table called "". The declaration
// itself still carries the name, under whichever spelling it used.
func declaredOwner(index goschema.Index, resolved string) string {
	for _, candidate := range []string{resolved, index.TableName, index.StructName} {
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return "(unnamed)"
}
