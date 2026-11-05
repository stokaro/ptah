//go:build integration && ignore

// No build compiles this file. It drives the built-in YDB engine, which the
// host does not carry: stokaro/ptah-ydb-plugin serves YDB. The file is kept as
// the record of the scenarios its rows in the stokaro/ptah#4204 test ledger do
// not mark as ported, and it is deleted once the plugin ports them.

package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
)

// A statement the server refuses reaches the caller with what the server said
// -- the status, its code and the issue -- and without the stack frames
// ydb-go-sdk writes into the text, whichever way the statement went: as a
// statement, or as a query whose rows are read. The error still answers the
// SDK's own predicate, so the code that classifies it reads it as before.
func TestYDBErrors_CarryTheServersAnswerWithoutSDKFrames(t *testing.T) {
	const missing = "ptah_errors_live_missing"
	const answer = `(?s)operation/SCHEME_ERROR \(code = 400070, .*` +
		`Cannot find table 'db\.\[/local/` + missing + `\]' because it does not exist.*`
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)

			_, execErr := conn.ExecContext(c.Context(), "DELETE FROM "+missing)
			var n int64
			queryErr := conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM "+missing).Scan(&n)

			tests := []struct {
				name string
				err  error
			}{
				{name: "a statement", err: execErr},
				{name: "a query", err: queryErr},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					c.Assert(test.err, qt.ErrorMatches, answer)
					c.Assert(test.err.Error(), qt.Not(qt.Contains), "ydb-go-sdk")
					c.Assert(ydbsdk.IsOperationError(test.err, Ydb.StatusIds_SCHEME_ERROR), qt.IsTrue)
				})
			}
		})
	}
}
