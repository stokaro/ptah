package sqlserverexec

import (
	"fmt"
	"strings"

	"ptah.run/internal/lexer"
)

// DefaultConstraintBatch renders one statement that drops the default of
// column on table, if the column has one, and then runs add, which is empty
// for a drop alone and an [AddDefault] statement for a replacement. table is
// the parts of the table's name and column the column's name, without quotes;
// add is a statement, written as the server reads it.
//
// The default is a constraint the server named, and the drop reads its name
// from sys.default_constraints into a variable and executes the statement it
// builds from it. That is several statements to the server, and a variable
// does not outlive the batch that declares it, so they travel as one: the
// literal sp_executesql runs, which also keeps a migration that is split into
// statements from separating them. sp_executesql takes only a Unicode literal.
// EXEC of a NULL string runs nothing, so a column with no default is left as
// it is. Measured on SQL Server 2022.
func DefaultConstraintBatch(table []string, column, add string) string {
	batch := dropDefaultBatch(table, column)
	if add != "" {
		batch += " " + add
	}
	return "EXEC sp_executesql " + UnicodeLiteral(batch) + ";"
}

// dropDefaultBatch is the text sp_executesql runs to drop the default of
// column on table.
func dropDefaultBatch(table []string, column string) string {
	qualified := QuoteQualifiedName(table)
	return fmt.Sprintf("DECLARE @drop nvarchar(max) = (SELECT %s + QUOTENAME(dc.name) FROM sys.default_constraints AS dc"+
		" WHERE dc.parent_object_id = OBJECT_ID(%s)"+
		" AND dc.parent_column_id = COLUMNPROPERTY(dc.parent_object_id, %s, 'ColumnId')); EXEC (@drop);",
		UnicodeLiteral("ALTER TABLE "+qualified+" DROP CONSTRAINT "),
		UnicodeLiteral(qualified),
		UnicodeLiteral(column),
	)
}

// AddDefault renders the statement that gives column on table the default
// expression, which is written as the server reads it. A column holds one
// default at most, so the statement answers Msg 1781, "Column already has a
// DEFAULT bound to it", unless the column's default was dropped first; a
// replacement is therefore DefaultConstraintBatch(table, column,
// AddDefault(table, column, expression)).
func AddDefault(table []string, column, expression string) string {
	return "ALTER TABLE " + QuoteQualifiedName(table) + " ADD DEFAULT " + expression + " FOR " + QuoteName(column) + ";"
}

// DefaultDrop is the column whose default a [DefaultConstraintBatch] with
// nothing to add drops.
type DefaultDrop struct {
	// Table is the parts of the table's name, without quotes.
	Table []string
	// Column is the column's name, without quotes.
	Column string
}

// ReadDefaultDrop reads stmt back into the column whose default it drops, when
// stmt is the statement [DefaultConstraintBatch] writes for that column with
// nothing to add. The batch reads the constraint's name from the catalog,
// scoped to that table and column, and drops it from that table; a batch that
// differs from that rendering in any byte reads as false, and so does one that
// adds a statement after the drop.
func ReadDefaultDrop(stmt string) (DefaultDrop, bool) {
	batch, drop, ok := readDefaultBatch(stmt)
	if !ok || batch != dropDefaultBatch(drop.Table, drop.Column) ||
		!sameStatement(stmt, DefaultConstraintBatch(drop.Table, drop.Column, "")) {
		return DefaultDrop{}, false
	}
	return drop, true
}

// DefaultReplace is the column whose default a [DefaultConstraintBatch] that
// adds an [AddDefault] replaces, and the expression of the new default.
type DefaultReplace struct {
	// Table is the parts of the table's name, without quotes.
	Table []string
	// Column is the column's name, without quotes.
	Column string
	// Expression is the new default, as the batch writes it. It is text the
	// server reads as SQL, and nothing here reads what it does.
	Expression string
}

// ReadDefaultReplace reads stmt back into the column whose default it
// replaces, when stmt is the statement DefaultConstraintBatch writes for that
// column with the [AddDefault] of an expression for the same table and
// column. The drop is read as [ReadDefaultDrop] reads it; what follows it is
// read as AddDefault writes it, and the expression is whatever text stands
// between ADD DEFAULT and the FOR that ends the batch. Whether that text is
// one expression is for the caller to judge.
func ReadDefaultReplace(stmt string) (DefaultReplace, bool) {
	batch, drop, ok := readDefaultBatch(stmt)
	if !ok {
		return DefaultReplace{}, false
	}
	add, found := strings.CutPrefix(batch, dropDefaultBatch(drop.Table, drop.Column)+" ")
	if !found {
		return DefaultReplace{}, false
	}
	expression, prefixed := strings.CutPrefix(add, "ALTER TABLE "+QuoteQualifiedName(drop.Table)+" ADD DEFAULT ")
	expression, suffixed := strings.CutSuffix(expression, " FOR "+QuoteName(drop.Column)+";")
	if !prefixed || !suffixed || expression == "" ||
		!sameStatement(stmt, DefaultConstraintBatch(drop.Table, drop.Column, AddDefault(drop.Table, drop.Column, expression))) {
		return DefaultReplace{}, false
	}
	return DefaultReplace{Table: drop.Table, Column: drop.Column, Expression: expression}, true
}

// readDefaultBatch reads the batch an EXEC sp_executesql of one Unicode
// literal runs, and the table and column its catalog lookup names.
func readDefaultBatch(stmt string) (string, DefaultDrop, bool) {
	read := tokens(stmt)
	if len(read) != 4 || read[0].Value != "EXEC" || read[0].Type != lexer.TokenIdentifier ||
		read[1].Value != "sp_executesql" || read[1].Type != lexer.TokenIdentifier {
		return "", DefaultDrop{}, false
	}
	batch, _, ok := unicodeLiteralAt(read, 2)
	if !ok {
		return "", DefaultDrop{}, false
	}
	inner := tokens(batch)
	table, ok := literalArgument(inner, "OBJECT_ID", 0)
	if !ok {
		return "", DefaultDrop{}, false
	}
	column, ok := literalArgument(inner, "COLUMNPROPERTY", 1)
	if !ok {
		return "", DefaultDrop{}, false
	}
	parts, ok := parseQualifiedName(table)
	if !ok {
		return "", DefaultDrop{}, false
	}
	return batch, DefaultDrop{Table: parts, Column: column}, true
}

// literalArgument reads the N'...' literal that is argument position of the
// first call of function in tokens. position counts the arguments before it,
// which are read as one token each, with the dots of a qualified name.
func literalArgument(tokens []lexer.Token, function string, position int) (string, bool) {
	for index := 0; index+1 < len(tokens); index++ {
		if tokens[index].Type != lexer.TokenIdentifier || tokens[index].Value != function ||
			!tokens[index+1].MatchOperatorValue("(") {
			continue
		}
		argument := index + 2
		for skipped := 0; skipped < position; skipped++ {
			for argument < len(tokens) && !tokens[argument].MatchOperatorValue(",") {
				argument++
			}
			argument++
		}
		value, _, ok := unicodeLiteralAt(tokens, argument)
		return value, ok
	}
	return "", false
}
