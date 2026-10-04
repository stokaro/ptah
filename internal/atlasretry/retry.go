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
		return !ydbDeterministicAbort(ydbIssueCodes(err))
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

// ydbDeterministicAborts are the issue codes of an ABORTED answer that running
// the transaction again would repeat, with what each one means.
//
// YDB answers a data query too large for one shard program with ABORTED, the
// status it also gives a lock conflict: measured on 25.1.4.7, an INSERT ...
// SELECT of 600000 rows answers `Datashard program size limit exceeded
// (56281409 > 50331648)` under issue code 200509, every time. A conflict
// carries issue code 2001 (`Transaction locks invalidated`) and is retried.
// The other size refusals measured -- `Out of buffer memory` on 26.2.1.14 and
// `Row data size is too big` on 25.1.4.7 -- answer PRECONDITION_FAILED, which
// is not retried.
var ydbDeterministicAborts = map[int32]string{
	200509: "the datashard program size limit",
}

// ydbDeterministicAbort reports whether an ABORTED answer carrying codes is
// one a retry would repeat.
func ydbDeterministicAbort(codes []int32) bool {
	for _, code := range codes {
		if _, deterministic := ydbDeterministicAborts[code]; deterministic {
			return true
		}
	}
	return false
}

// ydbIssueCodes lists the issue codes a YDB error carries, nested ones
// included.
func ydbIssueCodes(err error) []int32 {
	var codes []int32
	ydb.IterateByIssues(err, func(_ string, code Ydb.StatusIds_StatusCode, _ uint32) {
		codes = append(codes, int32(code))
	})
	return codes
}
