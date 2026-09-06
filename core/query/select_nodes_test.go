package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/query"
)

func TestComparisonOperatorString(t *testing.T) {
	tests := []struct {
		name string
		op   query.ComparisonOperator
		want string
	}{
		{name: "equal", op: query.OpEqual, want: "="},
		{name: "not equal", op: query.OpNotEqual, want: "<>"},
		{name: "less than", op: query.OpLessThan, want: "<"},
		{name: "less than or equal", op: query.OpLessThanOrEqual, want: "<="},
		{name: "greater than", op: query.OpGreaterThan, want: ">"},
		{name: "greater than or equal", op: query.OpGreaterThanOrEqual, want: ">="},
		{name: "out of range yields empty", op: query.ComparisonOperator(42), want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tt.op.String(), qt.Equals, tt.want)
		})
	}
}

func TestLogicalOperatorString(t *testing.T) {
	tests := []struct {
		name string
		op   query.LogicalOperator
		want string
	}{
		{name: "and", op: query.LogicalAnd, want: "AND"},
		{name: "or", op: query.LogicalOr, want: "OR"},
		{name: "out of range yields empty", op: query.LogicalOperator(42), want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tt.op.String(), qt.Equals, tt.want)
		})
	}
}

func TestJoinTypeString(t *testing.T) {
	tests := []struct {
		name string
		jt   query.JoinType
		want string
	}{
		{name: "inner", jt: query.JoinInner, want: "INNER JOIN"},
		{name: "left", jt: query.JoinLeft, want: "LEFT JOIN"},
		{name: "right", jt: query.JoinRight, want: "RIGHT JOIN"},
		{name: "full", jt: query.JoinFull, want: "FULL OUTER JOIN"},
		{name: "out of range yields empty", jt: query.JoinType(42), want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tt.jt.String(), qt.Equals, tt.want)
		})
	}
}

func TestSortDirectionString(t *testing.T) {
	tests := []struct {
		name string
		dir  query.SortDirection
		want string
	}{
		{name: "ascending", dir: query.SortAscending, want: "ASC"},
		{name: "descending", dir: query.SortDescending, want: "DESC"},
		{name: "out of range yields empty", dir: query.SortDirection(42), want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tt.dir.String(), qt.Equals, tt.want)
		})
	}
}
