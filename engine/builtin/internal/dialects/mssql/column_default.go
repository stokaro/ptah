package mssql

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/sqlserverexec"
)

// writeAlterColumn renders the two ALTER COLUMN actions SQL Server has a
// statement for: setting a column's default and dropping it. Every other
// action changes a property that SQL Server's ALTER COLUMN states together
// with the whole column, which is [ast.ModifyColumnOperation]'s shape, so it is
// refused rather than rendered as a restatement that would guess the rest.
//
// A default is not a property of the column in SQL Server. It is a constraint
// of its own, listed in sys.default_constraints, added with ADD DEFAULT ... FOR
// and removed by name with DROP CONSTRAINT. Ptah leaves it unnamed, here and in
// CREATE TABLE, so the name is the server's: DF__, parts of the table and
// column names, and a hexadecimal suffix that differs from one database to the
// next. The drop therefore reads the name from the catalog when it runs, rather
// than writing down the name one database happened to give it.
func (r *Renderer) writeAlterColumn(table string, op *ast.AlterColumnOperation) error {
	parts := nameParts(table)
	column := unquoteIdentifier(strings.TrimSpace(op.ColumnName))
	switch op.Action {
	case ast.AlterColumnDropDefault:
		r.w.WriteLine(sqlserverexec.DefaultConstraintBatch(parts, column, ""))
	case ast.AlterColumnSetDefault:
		// Replacing a default takes the drop as well, because a column holds
		// one default at most.
		value, _ := defaultSQL("", op.Default)
		r.w.WriteLine(sqlserverexec.DefaultConstraintBatch(parts, column, sqlserverexec.AddDefault(parts, column, value)))
	default:
		return unsupportedFeaturef("ALTER COLUMN %s: SQL Server changes a column's type and nullability by restating the column", op.Action)
	}
	return nil
}
