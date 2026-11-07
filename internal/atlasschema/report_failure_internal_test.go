package atlasschema

// White-box testing required: a commit that went unanswered with a rollback that
// was answered takes a session dying between the two, which no offline run builds,
// and the commit's error type is this package's own.

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	qt "github.com/frankban/quicktest"
)

// A commit the server gave no answer to has no code, and the rollback tried after it
// does not lend it one.
func TestApplyFailureReadsTheCommitsOwnAnswer(t *testing.T) {
	c := qt.New(t)
	commit := &failedCommitError{statements: 3, err: errors.New("connection lost")}
	err := fmt.Errorf("commit schema apply transaction: %w", errors.Join(commit, &pgconn.PgError{Code: "57P01"}))

	c.Assert(applyFailure(err), qt.DeepEquals, &ApplyFailure{Statements: 3, Commit: true})
}
