package query

import (
	"slices"
	"strings"
)

// This file answers two questions about a statement's shape that the renderer
// asks before writing YQL: whether a subquery reads a name from the query
// around it, and whether a JOIN condition is only equalities between columns.
// YDB runs neither the correlated subquery nor the other JOIN conditions; see
// capability.CorrelatedSubqueries and capability.NonEquiJoins.

// boundNames lists the names a statement's FROM and joins bind: the alias of
// each table, or the table's own name where it has no alias. A qualified column
// that names one of them reads the statement's own rows.
func boundNames(stmt *SelectStatement) []string {
	names := []string{bindingName(stmt.From, stmt.FromAlias)}
	for _, join := range stmt.Joins {
		names = append(names, bindingName(join.Table, join.Alias))
	}
	return names
}

func bindingName(table, alias string) string {
	if a := strings.TrimSpace(alias); a != "" {
		return a
	}
	return strings.TrimSpace(table)
}

// correlatedReference returns the first qualifier stmt reads that it does not
// bind itself, and whether there is one. Such a qualifier names a table of an
// enclosing query, which makes stmt a correlated subquery. The statements
// nested inside stmt are not read here: each answers for itself when it is
// rendered.
//
// An unqualified column is not counted. It is the subquery's own column in
// every dialect this renderer writes for, and YQL does not reach outward for
// one the subquery's tables lack; it refuses it instead.
func correlatedReference(stmt *SelectStatement) (string, bool) {
	bound := boundNames(stmt)
	for _, qualifier := range statementQualifiers(stmt) {
		if !slices.Contains(bound, qualifier) {
			return qualifier, true
		}
	}
	return "", false
}

// statementQualifiers lists, in the order they are written, the qualifiers of
// the columns stmt names in its own clauses.
func statementQualifiers(stmt *SelectStatement) []string {
	var out []string
	add := func(qualifier string) {
		if q := strings.TrimSpace(qualifier); q != "" {
			out = append(out, q)
		}
	}
	for _, column := range stmt.Columns {
		add(column.Qualifier)
		out = append(out, expressionQualifiers(column.Expr)...)
	}
	for _, join := range stmt.Joins {
		out = append(out, expressionQualifiers(join.On)...)
	}
	out = append(out, expressionQualifiers(stmt.Where)...)
	for _, column := range stmt.GroupBy {
		add(column.Qualifier)
	}
	out = append(out, expressionQualifiers(stmt.Having)...)
	for _, term := range stmt.OrderBy {
		add(term.Qualifier)
	}
	return out
}

// expressionQualifiers lists the column qualifiers an expression names, in the
// order they are written. A subquery inside it is a statement of its own and is
// not read.
func expressionQualifiers(expr Expression) []string {
	var out []string
	walkColumns(expr, func(qualifier string) {
		if q := strings.TrimSpace(qualifier); q != "" {
			out = append(out, q)
		}
	})
	return out
}

// walkColumns calls visit with the qualifier of every column reference in
// expr, stopping at a subquery.
func walkColumns(expr Expression, visit func(qualifier string)) {
	switch e := expr.(type) {
	case *ColumnRef:
		if e != nil {
			visit(e.Qualifier)
		}
	case *Comparison:
		if e != nil {
			walkColumns(e.Left, visit)
			walkColumns(e.Right, visit)
		}
	case *InExpr:
		if e != nil {
			walkColumns(e.Operand, visit)
			for _, value := range e.Values {
				walkColumns(value, visit)
			}
		}
	case *NullTest:
		if e != nil {
			walkColumns(e.Operand, visit)
		}
	case *LogicalExpr:
		if e != nil {
			for _, operand := range e.Operands {
				walkColumns(operand, visit)
			}
		}
	case *NotExpr:
		if e != nil {
			walkColumns(e.Operand, visit)
		}
	case *Arithmetic:
		if e != nil {
			walkColumns(e.Left, visit)
			walkColumns(e.Right, visit)
		}
	case *FuncCall:
		if e != nil {
			walkFuncColumns(e, visit)
		}
	}
}

func walkFuncColumns(call *FuncCall, visit func(qualifier string)) {
	for _, arg := range call.Args {
		walkColumns(arg, visit)
	}
	if call.Over == nil {
		return
	}
	for _, column := range call.Over.PartitionBy {
		visit(column.Qualifier)
	}
	for _, term := range call.Over.OrderBy {
		visit(term.Qualifier)
	}
}

// isColumnEquality reports whether a JOIN condition is an equality between two
// columns, or a conjunction of such equalities: the one shape YDB joins on.
func isColumnEquality(expr Expression) bool {
	switch e := expr.(type) {
	case *Comparison:
		if e == nil || e.Operator != OpEqual {
			return false
		}
		_, leftColumn := e.Left.(*ColumnRef)
		_, rightColumn := e.Right.(*ColumnRef)
		return leftColumn && rightColumn
	case *LogicalExpr:
		if e == nil || e.Operator != LogicalAnd || len(e.Operands) == 0 {
			return false
		}
		for _, operand := range e.Operands {
			if !isColumnEquality(operand) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
