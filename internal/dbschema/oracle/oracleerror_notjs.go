//go:build !js

package oracle

import (
	"errors"

	"github.com/sijms/go-ora/v3/network"
)

// oracleErrorCode reports the server's own error number, and whether err came
// from the Oracle driver at all. Reading the number rather than the message is
// what keeps isRoleReadDenied working against a server answering in a language
// other than English.
func oracleErrorCode(err error) (int, bool) {
	var oracleErr *network.OracleError
	if !errors.As(err, &oracleErr) {
		return 0, false
	}
	return oracleErr.ErrCode, true
}
