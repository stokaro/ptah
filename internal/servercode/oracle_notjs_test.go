//go:build !js

package servercode_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/sijms/go-ora/v3/network"

	"ptah.run/internal/servercode"
)

// Oracle gives a number and no SQLSTATE. The driver does not build for js/wasm,
// so this row lives apart from the others.
func TestRead_OracleGivesANumber(t *testing.T) {
	c := qt.New(t)
	state, number := servercode.Read(fmt.Errorf("oracle: %w", &network.OracleError{ErrCode: 1, ErrMsg: "ORA-00001: unique constraint violated"}))
	c.Assert(state, qt.Equals, "")
	c.Assert(number, qt.Equals, 1)
}
