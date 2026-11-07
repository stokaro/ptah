//go:build !js

package servercode

import (
	"errors"

	"github.com/sijms/go-ora/v3/network"
)

// oracleErrorNumber is the server's own number for an error from the Oracle
// driver, and zero for any other error.
func oracleErrorNumber(err error) int {
	if serverErr, ok := errors.AsType[*network.OracleError](err); ok {
		return serverErr.ErrCode
	}
	return 0
}
