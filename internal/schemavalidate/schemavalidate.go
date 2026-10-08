// Package schemavalidate reports structural problems in a desired schema
// without a database.
//
// Common structural checks run before the selected target validator. Index
// references need this common check because a syntactically valid index can
// name a relation or column absent from the declaration (stokaro/ptah#1711).
package schemavalidate

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
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
func Collect(ctx context.Context, service schemavalidation.Runtime, database *schemamodel.Database, dialect string) ([]Problem, error) {
	return CollectWithOptions(ctx, service, database, dialect, Options{})
}

// Options selects what a collection checks.
type Options struct {
	// Capabilities is the target capability set the schema is checked against.
	// The zero value selects the dialect's default preset.
	Capabilities capability.Capabilities
	// NoSkipped adds the render check: every declaration this target would
	// leave out of its DDL becomes a problem. It is opt-in because a schema
	// written for several engines is expected to lose engine-specific
	// declarations on the others, and failing that by default would refuse the
	// authoring style Ptah supports.
	NoSkipped bool
}

// CollectWithCapabilities reports every structural problem it can find against
// a concrete capability set.
func CollectWithCapabilities(
	ctx context.Context,
	service schemavalidation.Runtime,
	database *schemamodel.Database,
	dialect string,
	caps capability.Capabilities,
) ([]Problem, error) {
	return CollectWithOptions(ctx, service, database, dialect, Options{Capabilities: caps})
}

// CollectWithOptions reports every problem the selected checks can find.
//
// Common checks and selected target validation contribute to the same report.
// With [Options.NoSkipped], the provider also diagnoses declarations it would
// omit. A completed validation can report schema refusals. A service failure,
// incomplete reply, or cancellation returns an error and no partial problem list.
func CollectWithOptions(
	ctx context.Context,
	service schemavalidation.Runtime,
	database *schemamodel.Database,
	dialect string,
	opts Options,
) ([]Problem, error) {
	if err := schemaext.RequireRuntime(ctx, service); err != nil {
		return nil, err
	}
	selected, err := service.ResolveTarget(dialect)
	if err != nil {
		return nil, err
	}
	dialect = selected.Name()
	caps := opts.Capabilities
	if caps == nil {
		caps = capability.ForDialect(dialect)
	}
	if database == nil {
		return []Problem{{Dialect: dialect, Kind: "schema", Message: "no schema was loaded"}}, nil
	}
	// Target scoping precedes both common structural checks and owner validation.
	scoped, err := schemamodel.ScopeToTarget(database, selected)
	if err != nil {
		return nil, err
	}
	problems := collectIndexProblems(scoped, dialect)
	target := selected.Name()
	result, err := schemavalidation.Validate(ctx, service, schemavalidation.Request{
		Target: target, Schema: scoped, Capabilities: caps,
		Identifiers: identifier.ForDialect(dialect), NoSkipped: opts.NoSkipped,
	})
	if err != nil {
		return nil, err
	}
	for _, diagnostic := range result.Diagnostics {
		problems = append(problems, Problem{Dialect: dialect, Kind: diagnostic.Kind, Object: diagnostic.Object, Message: diagnostic.Message})
	}
	return problems, nil
}

// collectIndexProblems checks every index against the relation it belongs to.
//
// This is the check the renderer does not make. An index whose owner resolves
// to nothing falls back to the Go struct name and renders as `ON "Struct"`,
// which the server answers at apply time; an index naming a column the table
// does not declare renders and fails the same way.
func collectIndexProblems(database *schemamodel.Database, dialect string) []Problem {
	if len(database.Indexes) == 0 {
		return nil
	}
	owners := schemamodel.ResolveIndexOwners(database.Indexes, database.Tables, database.MaterializedViews)
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
	index schemamodel.Index,
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
func indexColumnNames(index schemamodel.Index) []string {
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
func indexableColumns(database *schemamodel.Database) map[string][]string {
	fields := schemamodel.ProcessEmbeddedFields(database.EmbeddedFields, database.Fields)
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
func isMaterializedViewOwner(database *schemamodel.Database, owner string) bool {
	return slices.ContainsFunc(database.MaterializedViews, func(view schemamodel.MaterializedView) bool {
		return view.Name == owner || view.StructName == owner
	})
}

// indexName names an index for a diagnostic, falling back to its position when
// the declaration carries no name.
func indexName(index schemamodel.Index, position int) string {
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
func declaredOwner(index schemamodel.Index, resolved string) string {
	for _, candidate := range []string{resolved, index.TableName, index.StructName} {
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return "(unnamed)"
}
