// Package schemalineage derives column-to-column dependencies from the view and
// materialized-view bodies a schema declares.
//
// # What this answers
//
// "What breaks if I drop this column." Ptah models views and reads their
// bodies, but nothing connected a base column to the view column it feeds, so
// the question could only be answered after the drop. The edges here are
// ordinary static analysis over material the schema model already holds: no
// server, no account, no hosted service (stokaro/ptah#1712).
//
// # Why undecidability is part of the result
//
// A view body is SQL, and SQL says things this package does not resolve: a
// function call over three columns, a UNION, a subquery in the select list. The
// honest answer for those is "this view has a dependency I did not resolve",
// not silence. [Result.Undecided] carries them, so a caller asking "is it safe
// to drop this column" can tell "nothing depends on it" from "I could not
// tell" -- a distinction that decides whether the answer may be trusted.
package schemalineage

import (
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
)

// Edge is one column-to-column dependency: a base column feeding a view column.
type Edge struct {
	// FromTable and FromColumn name the source the view reads.
	FromTable  string `json:"from_table"`
	FromColumn string `json:"from_column"`
	// ToView and ToColumn name the view column that source feeds.
	ToView   string `json:"to_view"`
	ToColumn string `json:"to_column"`
	// Materialized reports whether the consuming view is materialized.
	Materialized bool `json:"materialized,omitempty"`
}

// Undecided records a view body this package could not fully resolve, and why.
//
// It names the view rather than a column because the whole body is what went
// unresolved: a caller cannot know which columns would have appeared.
type Undecided struct {
	View   string `json:"view"`
	Reason string `json:"reason"`
	// Materialized reports whether the unresolved view is materialized.
	Materialized bool `json:"materialized,omitempty"`
}

// Result is the derived lineage plus what could not be derived.
type Result struct {
	Edges     []Edge      `json:"edges"`
	Undecided []Undecided `json:"undecided,omitempty"`
}

// Derive returns the column-level edges a schema's views declare.
//
// Edges are sorted, so two runs over the same schema produce the same document
// and a diff of two schemas is a diff of their lineage rather than of Go's map
// iteration order.
func Derive(db *goschema.Database) Result {
	if db == nil {
		return Result{}
	}
	columns := columnsByTable(db)
	var result Result
	for _, view := range db.Views {
		result.absorb(deriveView(view.Name, view.Body, false, columns))
	}
	for _, view := range db.MaterializedViews {
		result.absorb(deriveView(view.Name, view.Body, true, columns))
	}
	sort.Slice(result.Edges, func(i, j int) bool {
		a, b := result.Edges[i], result.Edges[j]
		if a.ToView != b.ToView {
			return a.ToView < b.ToView
		}
		if a.ToColumn != b.ToColumn {
			return a.ToColumn < b.ToColumn
		}
		if a.FromTable != b.FromTable {
			return a.FromTable < b.FromTable
		}
		return a.FromColumn < b.FromColumn
	})
	sort.Slice(result.Undecided, func(i, j int) bool {
		return result.Undecided[i].View < result.Undecided[j].View
	})
	return result
}

func (r *Result) absorb(other Result) {
	r.Edges = append(r.Edges, other.Edges...)
	r.Undecided = append(r.Undecided, other.Undecided...)
}

// columnsByTable indexes the declared columns of every table, which is what
// lets `SELECT *` resolve to names instead of being abandoned.
func columnsByTable(db *goschema.Database) map[string][]string {
	byStruct := make(map[string][]string, len(db.Tables))
	for _, field := range db.Fields {
		byStruct[field.StructName] = append(byStruct[field.StructName], field.Name)
	}
	columns := make(map[string][]string, len(db.Tables))
	for _, table := range db.Tables {
		columns[strings.ToLower(table.Name)] = byStruct[table.StructName]
	}
	return columns
}

func lowerName(name string) string { return strings.ToLower(unquote(name)) }

func sameName(a, b string) bool { return lowerName(a) == lowerName(b) }
