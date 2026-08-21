// Package lineage derives column-to-column dependencies from the views a
// schema declares.
//
// The question it answers is "what breaks if I drop this column", before the
// drop rather than after. A view column that reads a base column depends on it;
// nothing in a schema description says so, because the dependency lives inside
// the view's body (stokaro/ptah#1712).
//
// It resolves what a projection parser can resolve and reports the rest as
// unresolved rather than omitting it. A lineage report that silently leaves out
// what it could not work out is worse than one that says so: the reader cannot
// tell "this column depends on nothing" from "this tool did not look".
package lineage

import (
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/viewprojection"
)

// Edge is one column-to-column dependency: a view column reads a base column.
type Edge struct {
	// FromTable and FromColumn name the base column the value comes from.
	FromTable  string
	FromColumn string
	// ToView and ToColumn name the view column that reads it.
	ToView   string
	ToColumn string
}

// Unresolved names something the derivation could not establish, and why.
//
// Column is empty where the whole body was unreadable, because then no
// individual column was reached.
type Unresolved struct {
	View   string
	Column string
	Reason string
}

// Result is what one derivation found and what it could not.
type Result struct {
	Edges      []Edge
	Unresolved []Unresolved
}

// Reasons a column stays unresolved. They are values rather than free text so a
// caller can group by them without matching prose.
const (
	// ReasonBodyNotModeled marks a view whose body the projection parser does
	// not model, so none of its columns was reached.
	ReasonBodyNotModeled = "the view body has a shape the projection parser does not model"
	// ReasonComputed marks a column whose value is an expression. The columns
	// inside that expression are not opened; that is a separate increment.
	ReasonComputed = "the column is computed from an expression, whose references are not resolved"
	// ReasonNoSuchColumn marks a reference no relation in the view's FROM
	// declares, which usually means the FROM names something this schema does
	// not describe.
	ReasonNoSuchColumn = "no relation the view reads declares a column of that name"
	// ReasonAmbiguous marks a bare reference more than one relation in the
	// view's FROM could satisfy. The server resolves it from information this
	// derivation does not have.
	ReasonAmbiguous = "more than one relation the view reads declares a column of that name"
)

// Derive returns the column dependencies the declared views carry.
//
// The result is ordered, so two runs over the same schema produce the same
// report and a diff of two reports is about the schema.
func Derive(db *goschema.Database) Result {
	columns := columnOwners(db)
	tables := declaredTableNames(db)

	var result Result
	for _, view := range db.Views {
		result.append(deriveOne(view.Name, view.Body, tables, columns))
	}
	for _, view := range db.MaterializedViews {
		result.append(deriveOne(view.Name, view.Body, tables, columns))
	}
	result.sort()
	return result
}

// append merges one view's findings into the result.
func (r *Result) append(other Result) {
	r.Edges = append(r.Edges, other.Edges...)
	r.Unresolved = append(r.Unresolved, other.Unresolved...)
}

// sort orders both halves so the report is stable.
func (r *Result) sort() {
	sort.Slice(r.Edges, func(i, j int) bool { return edgeKey(r.Edges[i]) < edgeKey(r.Edges[j]) })
	sort.Slice(r.Unresolved, func(i, j int) bool {
		return unresolvedKey(r.Unresolved[i]) < unresolvedKey(r.Unresolved[j])
	})
}

func edgeKey(e Edge) string {
	return e.ToView + "\x00" + e.ToColumn + "\x00" + e.FromTable + "\x00" + e.FromColumn
}

func unresolvedKey(u Unresolved) string {
	return u.View + "\x00" + u.Column + "\x00" + u.Reason
}

// deriveOne resolves a single view body against the schema's columns.
func deriveOne(viewName, body string, tables []string, columns map[string][]string) Result {
	items, from, ok := viewprojection.Parse(body)
	if !ok {
		return Result{Unresolved: []Unresolved{{View: viewName, Reason: ReasonBodyNotModeled}}}
	}

	read := relationsRead(from, tables)
	var result Result
	for _, item := range items {
		column, isReference := item.ColumnReference()
		if !isReference {
			result.Unresolved = append(result.Unresolved,
				Unresolved{View: viewName, Column: item.Column, Reason: ReasonComputed})
			continue
		}
		owners := ownersOf(column, read, columns)
		switch len(owners) {
		case 1:
			result.Edges = append(result.Edges, Edge{
				FromTable: owners[0], FromColumn: bareColumn(column),
				ToView: viewName, ToColumn: item.Column,
			})
		case 0:
			result.Unresolved = append(result.Unresolved,
				Unresolved{View: viewName, Column: item.Column, Reason: ReasonNoSuchColumn})
		default:
			result.Unresolved = append(result.Unresolved,
				Unresolved{View: viewName, Column: item.Column, Reason: ReasonAmbiguous})
		}
	}
	return result
}

// ownersOf lists the relations among read that declare the referenced column.
//
// A reference may be qualified -- "t.id" -- in which case only that relation
// can own it, and the qualification resolves what a bare name could not.
func ownersOf(reference string, read []string, columns map[string][]string) []string {
	qualifier, bare := splitQualified(reference)
	var owners []string
	for _, relation := range read {
		if qualifier != "" && !strings.EqualFold(relation, qualifier) {
			continue
		}
		if declaresColumn(columns[strings.ToLower(relation)], bare) {
			owners = append(owners, relation)
		}
	}
	return owners
}

// splitQualified separates "table.column" into its parts, with an empty
// qualifier for a bare reference.
func splitQualified(reference string) (qualifier, column string) {
	index := strings.LastIndex(reference, ".")
	if index < 0 {
		return "", reference
	}
	return reference[:index], reference[index+1:]
}

// bareColumn is the column half of a reference that may be qualified.
func bareColumn(reference string) string {
	_, column := splitQualified(reference)
	return column
}

func declaresColumn(declared []string, column string) bool {
	for _, candidate := range declared {
		if strings.EqualFold(candidate, column) {
			return true
		}
	}
	return false
}

// relationsRead lists the declared tables a view's FROM text names.
//
// The FROM text is matched against the names the schema declares rather than
// parsed as a grammar: a token that is not a declared table -- a keyword, an
// alias, a join condition -- names nothing this derivation can resolve against,
// and leaving it out is what makes an unresolvable reference report itself.
func relationsRead(from string, tables []string) []string {
	fields := strings.FieldsFunc(from, func(r rune) bool {
		return r != '_' && r != '.' && r != '"' && !isIdentifierRune(r)
	})
	seen := make(map[string]bool, len(fields))
	var read []string
	for _, field := range fields {
		token := strings.Trim(field, `"`)
		for _, table := range tables {
			if strings.EqualFold(table, token) && !seen[strings.ToLower(table)] {
				seen[strings.ToLower(table)] = true
				read = append(read, table)
			}
		}
	}
	return read
}

func isIdentifierRune(r rune) bool {
	return r == '_' ||
		(r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9')
}

// columnOwners maps a declared table name, lowered, to the columns it declares.
func columnOwners(db *goschema.Database) map[string][]string {
	byStruct := make(map[string]string, len(db.Tables))
	for _, table := range db.Tables {
		byStruct[table.StructName] = table.Name
	}
	columns := make(map[string][]string, len(db.Tables))
	for _, field := range db.Fields {
		table, known := byStruct[field.StructName]
		if !known {
			continue
		}
		key := strings.ToLower(table)
		columns[key] = append(columns[key], field.Name)
	}
	return columns
}

// declaredTableNames lists the table names a FROM clause may name.
func declaredTableNames(db *goschema.Database) []string {
	names := make([]string, 0, len(db.Tables))
	for _, table := range db.Tables {
		names = append(names, table.Name)
	}
	return names
}
