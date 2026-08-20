package sqliterebuild

// White-box testing required: the decision this file makes is not observable
// through the package's exported surface. BeginTransaction needs a live
// connection, and a connection's capabilities cannot be overridden from
// outside, so the one case that separates the two keys -- a MySQL-shaped
// target -- is unreachable from a black-box test.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// TestBeginWithoutTransaction_ReadsTheKeyAboutTheWrapper pins WHICH key the
// decision reads, which the capability rows next door cannot show.
//
// The two keys agree on every target but one, so a test that only checked
// their values would pass on a build that read the wrong one. MySQL is the
// disagreement: it commits DDL implicitly -- capability.TransactionalDDL is
// false -- and takes the statement inside a transaction perfectly well. Reading
// TransactionalDDL here would stop wrapping MySQL for a reason that is not
// about MySQL at all (stokaro/ptah#1793).
func TestBeginWithoutTransaction_ReadsTheKeyAboutTheWrapper(t *testing.T) {
	tests := []struct {
		name         string
		caps         capability.Capabilities
		wantUnwapped bool
	}{
		{
			name:         "a target that takes the wrapper is wrapped",
			caps:         capability.ForDialect(platform.Postgres),
			wantUnwapped: false,
		},
		{
			// The case that separates the two keys.
			name:         "a target that commits DDL implicitly is still wrapped",
			caps:         capability.ForDialect(platform.MySQL),
			wantUnwapped: false,
		},
		{
			name:         "a target that refuses the wrapper runs unwrapped",
			caps:         capability.ForDialect(platform.Spanner),
			wantUnwapped: true,
		},
		{
			name:         "a target with no cross-statement transaction runs unwrapped",
			caps:         capability.ForDialect(platform.ClickHouse),
			wantUnwapped: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			unwrapped, ok := beginWithoutTransaction(test.caps, &recordingExecutor{})

			c.Assert(ok, qt.Equals, test.wantUnwapped)
			c.Assert(unwrapped != nil, qt.Equals, test.wantUnwapped)
		})
	}
}

// TestUnwrappedTransaction_RunsStraightAtTheWriter pins what the returned
// transaction does: it is a pass-through, and its Rollback undoes nothing,
// because there is no transaction to undo.
func TestUnwrappedTransaction_RunsStraightAtTheWriter(t *testing.T) {
	c := qt.New(t)
	writer := &recordingExecutor{}

	transaction := unwrappedTransaction{writer: writer}
	c.Assert(transaction.ExecuteSQL(context.Background(), "CREATE TABLE t (id bigint)"), qt.IsNil)
	c.Assert(transaction.Rollback(), qt.IsNil)
	c.Assert(transaction.Commit(), qt.IsNil)

	c.Assert(writer.executed, qt.DeepEquals, []string{"CREATE TABLE t (id bigint)"})
}

// recordingExecutor is a writer that remembers what it was asked to run.
type recordingExecutor struct {
	executed []string
}

func (e *recordingExecutor) ExecuteSQL(_ context.Context, sqlExpr string, _ ...any) error {
	e.executed = append(e.executed, sqlExpr)
	return nil
}

func (e *recordingExecutor) IsDryRun() bool { return false }
