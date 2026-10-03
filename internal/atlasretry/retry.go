// Package atlasretry classifies transient database errors, the conflicts after
// which a transaction can safely run again: for Atlas-compatible metadata
// updates, and for a YDB statement that commits on its own.
package atlasretry

import (
	"errors"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

// IsRetryable reports whether err represents a serialization conflict,
// deadlock, or lock contention that can safely retry the whole transaction.
func IsRetryable(err error) bool {
	var stateErr interface{ SQLState() string }
	if errors.As(err, &stateErr) {
		switch stateErr.SQLState() {
		case "40001", "40P01":
			return true
		}
	}

	if mysqlErr, ok := errors.AsType[*mysqldriver.MySQLError](err); ok {
		switch mysqlErr.Number {
		case 1205, 1213:
			return true
		}
	}

	var numberedErr interface{ SQLErrorNumber() int32 }
	if errors.As(err, &numberedErr) && numberedErr.SQLErrorNumber() == 1205 {
		return true
	}

	if oracleSerializationFailure(err) {
		return true
	}

	// YDB aborts a transaction whose reads another transaction changed before
	// it committed (`Transaction locks invalidated`), and nothing of it is
	// applied. The status is read rather than the text: the SDK carries it on
	// the error it returns from Commit and from a statement that commits.
	if ydb.IsOperationError(err, Ydb.StatusIds_ABORTED) {
		return true
	}

	var codedErr interface{ Code() int }
	if errors.As(err, &codedErr) {
		switch codedErr.Code() & 0xff {
		case 5, 6:
			return true
		}
	}
	return false
}
