package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
)

func TestComparisonOperatorString(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		op   ast.ComparisonOperator
		want string
	}{
		{name: "equal", op: ast.OpEqual, want: "="},
		{name: "not equal", op: ast.OpNotEqual, want: "<>"},
		{name: "less than", op: ast.OpLessThan, want: "<"},
		{name: "less than or equal", op: ast.OpLessThanOrEqual, want: "<="},
		{name: "greater than", op: ast.OpGreaterThan, want: ">"},
		{name: "greater than or equal", op: ast.OpGreaterThanOrEqual, want: ">="},
		{name: "out of range yields empty", op: ast.ComparisonOperator(42), want: ""},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.op.String(), qt.Equals, tt.want)
		})
	}
}

func TestLogicalOperatorString(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		op   ast.LogicalOperator
		want string
	}{
		{name: "and", op: ast.LogicalAnd, want: "AND"},
		{name: "or", op: ast.LogicalOr, want: "OR"},
		{name: "out of range yields empty", op: ast.LogicalOperator(42), want: ""},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.op.String(), qt.Equals, tt.want)
		})
	}
}

func TestJoinTypeString(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		jt   ast.JoinType
		want string
	}{
		{name: "inner", jt: ast.JoinInner, want: "INNER JOIN"},
		{name: "left", jt: ast.JoinLeft, want: "LEFT JOIN"},
		{name: "right", jt: ast.JoinRight, want: "RIGHT JOIN"},
		{name: "full", jt: ast.JoinFull, want: "FULL OUTER JOIN"},
		{name: "out of range yields empty", jt: ast.JoinType(42), want: ""},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.jt.String(), qt.Equals, tt.want)
		})
	}
}

func TestSortDirectionString(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		dir  ast.SortDirection
		want string
	}{
		{name: "ascending", dir: ast.SortAscending, want: "ASC"},
		{name: "descending", dir: ast.SortDescending, want: "DESC"},
		{name: "out of range yields empty", dir: ast.SortDirection(42), want: ""},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.dir.String(), qt.Equals, tt.want)
		})
	}
}
