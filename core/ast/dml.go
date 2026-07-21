package ast

import "slices"

// UpsertAssignment describes one column assignment in the UPDATE arm of an
// upsert statement. Expression is a SQL expression fragment, typically a
// bind placeholder or a reference to the source row.
type UpsertAssignment struct {
	Column     string
	Expression string
}

// UpsertNode represents a dialect-independent row upsert operation.
//
// InsertColumns and Values must have the same length. MatchColumns identifies
// the key columns used to decide whether the source row matches an existing
// target row. UpdateAssignments controls the update arm. The optional predicate
// fields are SQL expression fragments appended to the dialect-specific update
// and insert conditions.
type UpsertNode struct {
	Table             string
	InsertColumns     []string
	Values            []string
	MatchColumns      []string
	UpdateAssignments []UpsertAssignment
	UpdatePredicate   string
	InsertPredicate   string
	Comment           string
}

// NewUpsert creates an upsert node for table.
func NewUpsert(table string) *UpsertNode {
	return &UpsertNode{Table: table}
}

// SetInsert sets the columns and value expressions used by the INSERT arm.
func (n *UpsertNode) SetInsert(columns, values []string) *UpsertNode {
	n.InsertColumns = slices.Clone(columns)
	n.Values = slices.Clone(values)
	return n
}

// AddInsertValue appends one INSERT column/value pair.
func (n *UpsertNode) AddInsertValue(column, value string) *UpsertNode {
	n.InsertColumns = append(n.InsertColumns, column)
	n.Values = append(n.Values, value)
	return n
}

// SetMatchColumns sets the key columns used by the upsert match condition.
func (n *UpsertNode) SetMatchColumns(columns ...string) *UpsertNode {
	n.MatchColumns = slices.Clone(columns)
	return n
}

// AddUpdateAssignment appends one UPDATE assignment.
func (n *UpsertNode) AddUpdateAssignment(column, expression string) *UpsertNode {
	n.UpdateAssignments = append(n.UpdateAssignments, UpsertAssignment{
		Column:     column,
		Expression: expression,
	})
	return n
}

// SetUpdatePredicate adds a predicate to the update arm.
func (n *UpsertNode) SetUpdatePredicate(predicate string) *UpsertNode {
	n.UpdatePredicate = predicate
	return n
}

// SetInsertPredicate adds a predicate to the insert arm.
func (n *UpsertNode) SetInsertPredicate(predicate string) *UpsertNode {
	n.InsertPredicate = predicate
	return n
}

// SetComment sets a SQL comment emitted before the rendered upsert.
func (n *UpsertNode) SetComment(comment string) *UpsertNode {
	n.Comment = comment
	return n
}

// Accept implements the Node interface for UpsertNode.
func (n *UpsertNode) Accept(visitor Visitor) error {
	return visitor.VisitUpsert(n)
}
