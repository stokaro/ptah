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
