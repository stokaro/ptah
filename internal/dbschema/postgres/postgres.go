// Package postgres implements schema introspection and DDL execution for
// PostgreSQL-family databases in the dbschema connection layer.
package postgres

import (
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
)
