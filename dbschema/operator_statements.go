package dbschema

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/core/ptaherr"
)

// OperatorStatements runs statements the operator wrote, such as a test
// assertion or a script step, and reads what they return. *sql.DB and
// *DatabaseConnection satisfy it.
//
// It is not a way to send SQL Ptah composes: a statement on this handle reaches
// an executable provider as the operator's own text, which the provider runs as
// written.
type OperatorStatements interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

// OperatorStatementProvider is implemented by a native session whose provider
// runs statements the operator wrote. OperatorStatements returns a database/sql
// handle bound to that session, which the caller closes; closing it leaves the
// session open. A read-only handle runs a statement only when the provider
// proves it changes nothing and reads only the session's database, and opens
// no transaction.
type OperatorStatementProvider interface {
	OperatorStatements(readOnly bool) (*sql.DB, error)
}

// OperatorStatements returns the handle for statements the operator wrote, and
// the function that releases it.
//
// A database/sql connection returns itself and a release that does nothing. A
// native connection returns a handle over its provider's statement service; a
// row count its provider did not measure is an error from
// [sql.Result.RowsAffected] rather than a zero. A native connection whose
// provider serves no statement refuses with ptaherr.ErrUnsupportedFeature.
func (dc *DatabaseConnection) OperatorStatements() (OperatorStatements, func() error, error) {
	if dc == nil {
		return nil, nil, fmt.Errorf("operator statements require an open database connection")
	}
	if dc.native == nil {
		return dc, func() error { return nil }, nil
	}
	db, err := nativeOperatorStatements(dc.native, false)
	if err != nil {
		return nil, nil, err
	}
	return db, db.Close, nil
}

// nativeOperatorStatements opens a provider's statement handle, or refuses
// for a provider that serves none.
func nativeOperatorStatements(native NativeSession, readOnly bool) (*sql.DB, error) {
	service, ok := native.(OperatorStatementProvider)
	if !ok || nilNativeService(service) {
		return nil, rawSQLUnavailable()
	}
	db, err := service.OperatorStatements(readOnly)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, fmt.Errorf("%w: native session returned no statement handle", ptaherr.ErrUnsupportedFeature)
	}
	return db, nil
}

// withNativeIsolatedQuerySession is [DatabaseConnection.WithIsolatedQuerySession]
// for a provider's session: the queries run on a read-only statement handle,
// so the provider proves each one changes nothing before it runs.
func withNativeIsolatedQuerySession(native NativeSession, use func(IsolatedQueryer) error) (resultErr error) {
	if use == nil {
		return fmt.Errorf("isolated query session callback is nil")
	}
	db, err := nativeOperatorStatements(native, true)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil && resultErr == nil {
			resultErr = closeErr
		}
	}()
	return use(isolatedQueryer{runner: db})
}
