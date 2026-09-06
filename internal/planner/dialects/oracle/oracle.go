// Package oracle provides Oracle migration planning.
package oracle

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	mysqlplanner "ptah.run/internal/planner/dialects/mysql"
)

const DialectName = platform.Oracle

// Planner reuses the MySQL-family structural planning algorithm and routes
// every conversion and rendering through the Oracle dialect, which is the
// arrangement the SQL Server planner already has.
//
// The family fits for a reason rather than by elimination: Oracle changes a
// column in place with a MODIFY clause carrying the whole new definition, which
// is what this algorithm plans, and not with the PostgreSQL sequence of
// separate SET DATA TYPE, SET DEFAULT and SET NOT NULL steps.
//
// Keeping the package boundary separate is what lets Oracle diverge, and it
// will have to. Measured on 23.26, Oracle's nullability change is not
// idempotent -- `MODIFY (n NUMBER(10) NOT NULL)` on a column already NOT NULL
// answers ORA-01442, and `MODIFY (n NULL)` on one already nullable answers
// ORA-01451 -- so a plan that always emits the clause with the statement fails
// on re-application. ModifyColumnOperation carries the previous nullability for
// that decision and no planner spends it yet (stokaro/ptah#1875).
type Planner = mysqlplanner.Planner

// New returns an Oracle planner configured with the default Oracle23 preset.
func New() *Planner {
	return NewWithCapabilities(capability.Oracle23())
}

// NewWithCapabilities returns an Oracle planner for a concrete server
// capability set.
func NewWithCapabilities(caps capability.Capabilities) *Planner {
	return mysqlplanner.NewForDialect(DialectName, caps)
}
