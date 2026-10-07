package mssql

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
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
	switch op.Action {
	case ast.AlterColumnDropDefault:
		r.w.WriteLine(defaultConstraintBatch(table, op.ColumnName, ""))
	case ast.AlterColumnSetDefault:
		value, _ := defaultSQL(op.Default)
		r.w.WriteLine(defaultConstraintBatch(table, op.ColumnName, fmt.Sprintf("ALTER TABLE %s ADD DEFAULT %s FOR %s;",
			escapeQualifiedIdentifier(table),
			value,
			escapeIdentifier(op.ColumnName),
		)))
	default:
		return unsupportedFeaturef("ALTER COLUMN %s: SQL Server changes a column's type and nullability by restating the column", op.Action)
	}
	return nil
}

// defaultConstraintBatch renders one statement that drops the default of a
// column, if it has one, and then runs add, which is empty for a drop alone.
//
// Replacing a default takes the drop as well, because a column holds one
// default at most: ADD DEFAULT on a column that has one answers Msg 1781,
// "Column already has a DEFAULT bound to it".
//
// The drop reads the constraint's name into a variable and executes the
// statement it builds from it. That is several statements to the server, and
// a variable does not outlive the batch that declares it, so they travel as
// one: the literal sp_executesql runs, which also keeps a migration that is
// split into statements from separating them. sp_executesql takes only a
// Unicode literal. EXEC of a NULL string runs nothing, so a column with no
// default is left as it is. Measured on SQL Server 2022.
func defaultConstraintBatch(table, column, add string) string {
	qualified := escapeQualifiedIdentifier(table)
	batch := fmt.Sprintf("DECLARE @drop nvarchar(max) = (SELECT %s + QUOTENAME(dc.name) FROM sys.default_constraints AS dc"+
		" WHERE dc.parent_object_id = OBJECT_ID(%s)"+
		" AND dc.parent_column_id = COLUMNPROPERTY(dc.parent_object_id, %s, 'ColumnId')); EXEC (@drop);",
		unicodeLiteral("ALTER TABLE "+qualified+" DROP CONSTRAINT "),
		unicodeLiteral(qualified),
		unicodeLiteral(unquoteIdentifier(strings.TrimSpace(column))),
	)
	if add != "" {
		batch += " " + add
	}
	return "EXEC sp_executesql " + unicodeLiteral(batch) + ";"
}

// unicodeLiteral quotes s as an N'...' literal.
func unicodeLiteral(s string) string {
	return "N" + escapeStringLiteral(s)
}
