// Package mssql provides SQL Server migration planning.
package mssql

import (
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	mysqlplanner "github.com/stokaro/ptah/internal/planner/dialects/mysql"
)

const DialectName = platform.SQLServer

// Planner currently reuses the MySQL-family structural planning algorithm
// while routing all schema conversion and rendering through the SQL Server
// dialect. Keeping this package boundary separate lets SQL Server diverge as
// soon as its ALTER/DML planning surface grows beyond the shared generic subset.
type Planner = mysqlplanner.Planner

// New returns a SQL Server planner configured with the default SQLServer2022
// capability preset.
func New() *Planner {
	return NewWithCapabilities(capability.SQLServer2022())
}

// NewWithCapabilities returns a SQL Server planner for a concrete server
// capability set.
func NewWithCapabilities(caps capability.Capabilities) *Planner {
	return mysqlplanner.NewForDialect(DialectName, caps)
}
