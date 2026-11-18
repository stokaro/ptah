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

// ORA-00001 is Oracle's unique violation. Insufficient privileges, an exceeded
// quota and the two lost-connection errors are not.
func TestUniqueViolation_Oracle(t *testing.T) {
	tests := []struct {
		code int
		want bool
	}{
		{code: 1, want: true},
		{code: 1031},
		{code: 1536},
		{code: 3113},
		{code: 3114},
		{code: 60},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("ORA-%05d", test.code), func(t *testing.T) {
			c := qt.New(t)
			err := fmt.Errorf("oracle: %w", &network.OracleError{ErrCode: test.code})
			c.Assert(servercode.UniqueViolation(err), qt.Equals, test.want)
		})
	}
}
