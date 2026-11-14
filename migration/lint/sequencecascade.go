package lint

import (
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/pgregclass"
)

// withCascadedDefaults returns changes with the table changes each DROP
// SEQUENCE ... CASCADE makes, one after the drop for every table of the
// starting state whose column defaults name the sequence.
//
// PostgreSQL records a dependency of each `nextval('s'::regclass)` default on
// s, so the cascade drops the default and the table changes with the sequence.
// Measured against the pinned community binary v1.3.0 on PostgreSQL, after a
// version that creates the sequence s: `DROP SEQUENCE s CASCADE` counts 1
// schema change where one table's default names s, 1 where two columns of that
// table do, 2 where two tables do, and 0 where none does. The same table counts
// again for a second statement that changes it.
//
// Only the starting state says which defaults name the sequence, so a table
// is counted only where nothing before the drop in the file can have changed
// those defaults: [unseenBefore] holds for the sequence and the table, with
// one exception. An earlier DROP SEQUENCE ... CASCADE changes no table and no
// column, only the defaults that name the sequence it drops, so it hides
// nothing from a column whose defaults name none of the sequences it drops. A
// default a statement of the same file sets is not in the state and is not
// counted.
func withCascadedDefaults(file *File, changes []countedChange) []countedChange {
	if file.baseline.Unread() || !slices.ContainsFunc(changes, func(change countedChange) bool { return change.cascades != "" }) {
		return changes
	}
	out := make([]countedChange, 0, len(changes))
	// counted holds, per statement, the tables a cascade already changed: two
	// sequences one statement drops change a table once.
	counted := make(map[int]map[string]bool)
	// earlier holds the cascades read so far, and cascading the statements
	// that hold them.
	var earlier []countedChange
	cascading := make(map[int]bool)
	for _, change := range changes {
		out = append(out, change)
		index := change.StatementIndex
		if change.cascades == "" {
			continue
		}
		before := slices.DeleteFunc(slices.Clone(earlier), func(drop countedChange) bool { return drop.StatementIndex >= index })
		if unseenBeforeExcept(file, index, change.cascades, cascading) {
			for _, table := range defaultSequenceTables(file, change.cascades, before) {
				ref := lintkit.NormalizeReference(table)
				if counted[index][ref] || !unseenBeforeExcept(file, index, ref, cascading) {
					continue
				}
				if counted[index] == nil {
					counted[index] = make(map[string]bool)
				}
				counted[index][ref] = true
				modified := change
				modified.Kind, modified.Object = SchemaChangeModify, table
				modified.on, modified.parent, modified.guarded, modified.cascades, modified.outside = kindTablePart, ref, false, "", false
				out = append(out, modified)
			}
		}
		earlier = append(earlier, change)
		cascading[index] = true
	}
	return out
}

// defaultSequenceTables returns the tables of the starting state with a
// column whose default names the sequence dropped and none of the sequences
// the earlier cascades drop, in the order the state lists them, each once. A
// table is spelled qualified where the state names its schema.
func defaultSequenceTables(file *File, dropped string, earlier []countedChange) []string {
	var tables []string
	seen := make(map[string]bool)
	for _, column := range file.baseline.Columns() {
		if column.Default == "" {
			continue
		}
		sequences := pgregclass.Constants(column.Default)
		if !namesSequence(file, sequences, dropped) || slices.ContainsFunc(earlier, func(drop countedChange) bool {
			return namesSequence(file, sequences, drop.cascades)
		}) {
			continue
		}
		table := tableSpelling(column)
		if !seen[table] {
			seen[table] = true
			tables = append(tables, table)
		}
	}
	return tables
}

// namesSequence reports whether one of the sequences a column's default names,
// its regclass constants, is the one the folded reference dropped names.
//
// The server spells the constant bare for a sequence the search path of the
// read finds, and qualified otherwise. A drop spelled the same way names the
// same sequence, since the replay and the read share the dev database's search
// path. A drop qualified by the schema an unqualified name resolves to names
// the sequence that path finds too ([runReference]); any other qualifier on a
// drop of a bare constant may name another schema's sequence, so it is not a
// match.
func namesSequence(file *File, sequences []string, dropped string) bool {
	for _, sequence := range sequences {
		held := lintkit.NormalizeReference(sequence)
		if held == dropped || !strings.Contains(held, ".") && runReference(file, dropped) == held {
			return true
		}
	}
	return false
}

// tableSpelling is the table of column as the state spells it, qualified
// where the state names its schema.
func tableSpelling(column lintext.BaselineColumn) string {
	if column.Schema == "" {
		return column.Table
	}
	return column.Schema + "." + column.Table
}
