package dbexprprobe

import (
	"context"
	"database/sql"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
)

// Session is the connected server the transactional resolvers probe: the
// server's description, and one transaction that is always rolled back.
// *dbschema.DatabaseConnection implements it. The probes take this interface
// rather than the connection type so that the comparison which runs them does
// not link every schema reader the connection package carries.
type Session interface {
	schemaext.ProbeSession
	Info() catalog.ServerInfo
}

// Executor is the connected server [ResolveGeneratedExpressions] probes. Its
// probe is DDL that Oracle commits itself, so it runs plain statements instead
// of a transaction. *dbschema.DatabaseConnection implements it.
type Executor interface {
	Info() catalog.ServerInfo
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}
