package atlasretry

// White-box testing required: whether a YDB ABORTED answer is retried turns on
// the issue codes it carries, and ydb-go-sdk builds an operation error with
// issues only inside its internal package, so a black-box test cannot hand
// IsRetryable one. The live test TestYDBConnection_AbortedTransactionIsRetryable
// drives a real lock conflict through IsRetryable on both certified lines, and
// TestYDBConnection_ACopyTooLargeForOneQueryIsNotRetried a real size refusal.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// A lock conflict is retried, alone or beside an issue that says nothing about
// whether a retry repeats it; the size refusal is not, wherever it sits among
// the issues.
func TestYDBDeterministicAbort(t *testing.T) {
	tests := []struct {
		name  string
		codes []int32
		want  bool
	}{
		{name: "a lock conflict", codes: []int32{2001}, want: false},
		{name: "a lock conflict reported twice", codes: []int32{2001, 2001}, want: false},
		{name: "no issue at all", codes: nil, want: false},
		{name: "the datashard program size limit", codes: []int32{200509}, want: true},
		{name: "the size limit beside another issue", codes: []int32{1060, 200509}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbDeterministicAbort(test.codes), qt.Equals, test.want)
		})
	}
}
