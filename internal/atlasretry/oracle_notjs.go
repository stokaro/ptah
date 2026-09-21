//go:build !js

package atlasretry

import (
	"errors"

	"github.com/sijms/go-ora/v3/network"
)

// oracleSerializationFailure reports whether err is Oracle's own serialization
// conflict, ORA-08177.
//
// Oracle's driver publishes the server's number as a field rather than through
// one of the interfaces the other drivers implement, and it does not report a
// SQLSTATE, so neither arm of [IsRetryable] can see it. Reading the number is
// also what keeps this working against a server answering in a language other
// than English.
func oracleSerializationFailure(err error) bool {
	var oracleErr *network.OracleError
	if !errors.As(err, &oracleErr) {
		return false
	}
	return oracleErr.ErrCode == oracleCannotSerializeAccess
}

// oracleCannotSerializeAccess is ORA-08177, "can't serialize access for this
// transaction": the conflict a serializable transaction is told to retry.
const oracleCannotSerializeAccess = 8177
