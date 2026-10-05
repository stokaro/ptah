package difftypes

import "ptah.run/core/ast"

// YDBColumnTableChange carries both sides of a column-table layout or TTL change.
// A nil side denotes row storage; it must never be mistaken for a default layout.
type YDBColumnTableChange struct {
	// Desired is the declared column-table state, or nil for row storage.
	Desired *ast.YDBColumnTableSpec `json:"desired"`
	// Current is the state read from the database, or nil for row storage.
	Current *ast.YDBColumnTableSpec `json:"current"`
}
