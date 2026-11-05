// Package atlasretry classifies transient database errors, the conflicts after
// which a transaction can safely run again, for Atlas-compatible metadata
// updates and the migrator's tag writes.
package atlasretry

import (
	"errors"

	mysqldriver "github.com/go-sql-driver/mysql"
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

	var codedErr interface{ Code() int }
	if errors.As(err, &codedErr) {
		switch codedErr.Code() & 0xff {
		case 5, 6:
			return true
		}
	}
	return false
}
