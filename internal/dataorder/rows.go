package dataorder

import (
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/tableref"
)

// SelfReferences returns the columns of one table that reference that same
// table, as declared on its fields.
//
// A hierarchy is one table -- a category tree, an org chart -- so the table
// order [Ranker] answers cannot decide it: every row is in the same table, and
// what has to be ordered is the rows. The columns this returns are what pairs a
// row with the row it references.
//
// The name is resolved the way the renderer resolves it: `nodes(code)` names a
// table, and a reference to another table is not a self-reference however the
// two are spelled.
func SelfReferences(db *schemamodel.Database, table schemamodel.Table) []string {
	if db == nil {
		return nil
	}
	var columns []string
	for _, field := range db.Fields {
		if field.StructName != table.StructName || field.Foreign == "" {
			continue
		}
		// The declaration spells the target as `nodes(code)` or as the
		// shorthand `nodes`, and schemaprep is the one reader of that grammar.
		parsed := schemaprep.ParseForeignKeyReference(field.Foreign)
		if parsed == nil {
			continue
		}
		reference, ok := tableref.Parse(parsed.Table)
		if !ok || reference.Name != table.Name {
			continue
		}
		if reference.Qualified && strings.TrimSpace(reference.Schema) != strings.TrimSpace(table.Schema) {
			continue
		}
		columns = append(columns, field.Name)
	}
	slices.Sort(columns)
	return columns
}

// ColumnTypes returns the declared type of each column of one table, keyed by
// column name.
//
// Both stages that write declared rows hand it to the row renderer, which needs
// a column's type where a dialect refuses the literal a value would otherwise
// take: a declared moment is text, and Oracle refuses text for a DATE or
// TIMESTAMP column. It sits beside [SelfReferences] for the reason the package
// exists: two stages reading one declaration must read it the same way.
func ColumnTypes(db *schemamodel.Database, table schemamodel.Table) map[string]string {
	if db == nil {
		return nil
	}
	types := make(map[string]string)
	for _, field := range db.Fields {
		if field.StructName == table.StructName {
			types[field.Name] = field.Type
		}
	}
	return types
}

// Rows orders rows so that a row arrives after every row it references.
//
// references names the self-referencing columns; a row's value in one of them
// is the key of another row in the same set. Rows that reference nothing keep
// their order relative to each other, which is the caller's -- two runs over one
// declaration produce one order.
//
// A cycle cannot be ordered: no arrangement satisfies two rows that reference
// each other, and a foreign key that is not deferrable refuses both. Those rows
// keep the order they arrived in rather than being dropped or reported here, so
// the caller's plan still carries them and the server gives the verdict.
func Rows(rows []map[string]any, keys, references []string) []map[string]any {
	if len(rows) < 2 || len(references) == 0 || len(keys) != 1 {
		// A composite key cannot be paired with a single-column reference, and
		// nothing in the declaration says which component the reference names.
		return rows
	}

	key := keys[0]
	position := make(map[string]int, len(rows))
	for index, row := range rows {
		position[rowKeyText(row, key)] = index
	}

	ordered := make([]map[string]any, 0, len(rows))
	state := make([]int, len(rows))
	const (
		unvisited = 0
		open      = 1
		placed    = 2
	)
	cyclic := false

	var place func(index int)
	place = func(index int) {
		if state[index] == placed {
			return
		}
		if state[index] == open {
			// Arriving back at a row still being placed is a cycle.
			cyclic = true
			return
		}
		state[index] = open
		for _, reference := range references {
			parent, ok := position[rowKeyText(rows[index], reference)]
			if !ok || parent == index {
				continue
			}
			place(parent)
		}
		state[index] = placed
		ordered = append(ordered, rows[index])
	}

	for index := range rows {
		place(index)
	}
	if cyclic {
		return rows
	}
	return ordered
}

// rowKeyText renders a cell as the text the pairing compares. A key and the
// reference that names it are the same value in the same column type, so the
// comparison is on the rendered form rather than on the Go type: a declaration
// carries text, and a row read back from a driver may carry bytes.
func rowKeyText(row map[string]any, column string) string {
	value, ok := row[column]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	}
	return ""
}
