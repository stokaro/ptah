// Package columnchange reads which properties of a column a comparison changed,
// in the terms an [ast.ModifyColumnOperation] states them.
//
// Every dialect planner states them on the operations it builds, and they
// have to agree. The PostgreSQL renderer writes a clause only for a stated
// property, and migration/safety judges any dialect's restated column by
// them: a MySQL MODIFY or a SQL Server ALTER COLUMN repeats NOT NULL whatever
// changed, and only the stated properties say whether NOT NULL is new. One
// reading of the diff keys serves both, so a key the comparator adds is read
// the same way everywhere.
package columnchange

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/migration/schemadiff/difftypes"
)

// Properties names the properties colDiff changes that an ALTER COLUMN clause
// carries: the type, nullability and the default. Changes no such clause
// carries, such as a UNIQUE or PRIMARY KEY flag, are not among them.
//
// The keys are the comparator's: a default is recorded under "default" or
// "default_expr", depending on how the live side spelled it.
func Properties(colDiff difftypes.ColumnDiff) ast.ColumnProperties {
	_, typeChanged := colDiff.Changes["type"]
	_, nullabilityChanged := colDiff.Changes["nullable"]
	_, literalDefaultChanged := colDiff.Changes["default"]
	_, expressionDefaultChanged := colDiff.Changes["default_expr"]
	return ast.ColumnProperties{
		Type:        typeChanged,
		Nullability: nullabilityChanged,
		Default:     literalDefaultChanged || expressionDefaultChanged,
	}
}

// PreviousDefault returns the default the live column carried before the change
// colDiff records, and whether the comparison recorded a default change at all.
//
// The two answers are separate because an empty default is a fact too: a
// change recorded as ` -> 7` says the column had no default, while no default
// change says nothing about the default. A renderer that has to remove a
// default needs to know it existed, since ClickHouse refuses `REMOVE DEFAULT`
// on a column that has none, and Oracle has to spell the removal out because a
// MODIFY that omits it keeps the old one.
func PreviousDefault(colDiff difftypes.ColumnDiff) (string, bool) {
	changed := false
	for _, key := range []string{"default", "default_expr"} {
		change, present := colDiff.Changes[key]
		if !present {
			continue
		}
		changed = true
		if before, _, ok := strings.Cut(change, " -> "); ok {
			return strings.TrimSpace(before), true
		}
	}
	return "", changed
}
