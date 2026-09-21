//go:build !js

package atlasretry_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/sijms/go-ora/v3/network"

	"ptah.run/internal/atlasretry"
)

// Oracle's driver is tagged out of js/wasm, so its cases sit here rather than
// in the table beside the other engines.
//
// The number is read from the error rather than its text: the driver renders
// the message in the server's own language, and a match on English would
// report every Oracle conflict as permanent on a server configured otherwise.
func TestIsRetryable_Oracle(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "serialization failure",
			err:  &network.OracleError{ErrCode: 8177},
			want: true,
		},
		{
			name: "wrapped serialization failure",
			err:  fmt.Errorf("commit transaction: %w", &network.OracleError{ErrCode: 8177}),
			want: true,
		},
		{
			name: "unique violation",
			err:  &network.OracleError{ErrCode: 1},
			want: false,
		},
		{
			name: "resource busy",
			err:  &network.OracleError{ErrCode: 54},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasretry.IsRetryable(tt.err), qt.Equals, tt.want)
		})
	}
}
