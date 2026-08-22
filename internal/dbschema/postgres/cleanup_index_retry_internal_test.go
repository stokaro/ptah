package postgres

// White-box testing required: the retry loop and the cleanup query's relkind
// list are unexported, and the fact under test -- that a drop refused because
// of an object dropped later is attempted again -- has no exported surface. The
// only visible symptom is a cleanup that fails on a server nobody can run in a
// unit test.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
)

// refusingServer answers a drop the way an engine that will not drop an
// occupied object does: the named object is refused for as long as everything
// in blockedBy is still standing.
type refusingServer struct {
	blockedBy map[string][]string
	standing  map[string]bool
	attempts  []string
}

func newRefusingServer(objects []postgresCleanupObject, blockedBy map[string][]string) *refusingServer {
	standing := make(map[string]bool, len(objects))
	for _, object := range objects {
		standing[object.Name] = true
	}
	return &refusingServer{blockedBy: blockedBy, standing: standing}
}

func (s *refusingServer) attempt(object postgresCleanupObject) (dropErr, controlErr error) {
	s.attempts = append(s.attempts, object.Name)
	for _, blocker := range s.blockedBy[object.Name] {
		if s.standing[blocker] {
			return fmt.Errorf("Cannot drop %s with indices: %s", object.Name, blocker), nil
		}
	}
	s.standing[object.Name] = false
	return nil, nil
}

// standingObjects names what the server still holds, in the order it was given.
func (s *refusingServer) standingObjects(objects []postgresCleanupObject) []string {
	remaining := make([]string, 0, len(objects))
	for _, object := range objects {
		remaining = append(remaining, object.Name)
	}
	kept := make([]string, 0, len(remaining))
	for _, name := range remaining {
		kept = append(kept, fmt.Sprintf("%s=%t", name, s.standing[name]))
	}
	return kept
}

// TestRetryCleanupObjects_AttemptsAgainWhatALaterDropUnblocks pins the rule the
// loop runs on: progress, not a count.
//
// The Cloud Spanner emulator through PGAdapter 0.55.2 answers
// `Cannot drop table dfp with indices: dfp_uq` (SQLSTATE 0A000), and the
// cleanup orders a table before its indexes because on every other
// PostgreSQL-family server the table takes them with it. A single ordered pass
// therefore cannot finish there whatever order it picks -- one of the two
// engines is wrong about it -- which is why the retry, not a re-ordering, is
// the fix (stokaro/ptah#1901).
func TestRetryCleanupObjects_AttemptsAgainWhatALaterDropUnblocks(t *testing.T) {
	rows := []struct {
		name    string
		objects []postgresCleanupObject
		// blockedBy maps an object to what must be gone before it drops.
		blockedBy map[string][]string
		// wantErr is the substring the loop must fail with, empty when it must
		// finish.
		wantErr string
		// wantStanding is the server's state afterwards, one entry per object.
		wantStanding []string
		// wantAttempts is how many drops were issued in total. It separates
		// "finished" from "finished by luck": the blocked table has to be
		// attempted twice, so four objects cost five attempts.
		wantAttempts int
	}{
		{
			name: "a table blocked by its own index drops on the next round",
			objects: []postgresCleanupObject{
				{Kind: "table", Name: "dfp"},
				{Kind: "table", Name: "fkc"},
				{Kind: "index", Name: "dfp_uq"},
			},
			blockedBy:    map[string][]string{"dfp": {"dfp_uq"}},
			wantStanding: []string{"dfp=false", "fkc=false", "dfp_uq=false"},
			wantAttempts: 4,
		},
		{
			name: "a chain unwinds one round at a time",
			objects: []postgresCleanupObject{
				{Kind: "table", Name: "a"},
				{Kind: "table", Name: "b"},
				{Kind: "index", Name: "c"},
			},
			blockedBy:    map[string][]string{"a": {"b"}, "b": {"c"}},
			wantStanding: []string{"a=false", "b=false", "c=false"},
			wantAttempts: 6,
		},
		{
			name: "an object nothing will unblock reports its own refusal",
			objects: []postgresCleanupObject{
				{Kind: "table", Name: "held"},
				{Kind: "index", Name: "held_idx"},
			},
			blockedBy:    map[string][]string{"held": {"outside"}, "held_idx": {"outside"}},
			wantErr:      "failed to drop table held: Cannot drop held with indices: outside",
			wantStanding: []string{"held=true", "held_idx=true"},
			wantAttempts: 2,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			// The blocker of the last row is never in the object list, so it
			// stands forever -- which is what an external dependency is.
			server := newRefusingServer(row.objects, row.blockedBy)
			server.standing["outside"] = true

			err := retryCleanupObjects(context.Background(), row.objects, server.attempt)

			c.Assert(errorText(err), qt.Contains, row.wantErr)
			c.Assert(server.standingObjects(row.objects), qt.DeepEquals, row.wantStanding)
			c.Assert(server.attempts, qt.HasLen, row.wantAttempts)
		})
	}
}

// errorText renders an error for a Contains assertion so an absent error and an
// empty expectation compare as a match without a branch in the test.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestCollectAllObjects_EnumeratesIndexesAfterTheirTables guards the other half.
//
// The retry can only re-attempt a table if the index blocking it is in the list
// at all, and pg_class carries indexes under a relkind the cleanup did not ask
// for. The priority is asserted with it because it is what keeps the addition
// free on every other server: the index drop runs after the table that owns it
// is gone, so the guard turns it into a no-op rather than an error about a
// constraint that requires the index.
func TestCollectAllObjects_EnumeratesIndexesAfterTheirTables(t *testing.T) {
	rows := []struct {
		name string
		caps capability.Capabilities
	}{
		{name: "a server that assembles its own drops", caps: capability.Postgres16()},
		{name: "a server without format()", caps: capability.SpannerPostgres()},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			query := collectedQueryFor(t, row.caps)

			c.Assert(query, qt.Contains, "'r', 'p', 'v', 'm', 'f', 'S', 'i'")
			c.Assert(query, qt.Contains, "WHEN 'i' THEN 'index'")
			// 25 against the ELSE that every table takes: the drops are
			// ordered by this number, so the index leaves after its table
			// rather than before it.
			c.Assert(query, qt.Contains, "WHEN 'i' THEN 25")
			c.Assert(query, qt.Contains, "ELSE 20")
		})
	}
}

// blockingConn is a cleanupConn that refuses one statement until another has
// run, so the two dispatch arms below differ in outcome rather than only in
// which function they called.
type blockingConn struct {
	blocked  string
	unlocker string
	unlocked bool
	executed []string
}

func (c *blockingConn) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	c.executed = append(c.executed, query)
	if query == c.unlocker {
		c.unlocked = true
	}
	if query == c.blocked && !c.unlocked {
		return nil, fmt.Errorf("Cannot drop table dfp with indices: dfp_uq")
	}
	return nil, nil
}

func (c *blockingConn) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, fmt.Errorf("blockingConn is exec-only")
}

func (c *blockingConn) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return nil
}

// TestDropObjects_RetriesWhereTheTransactionWasGivenUp pins the wiring rather
// than the loop.
//
// [retryCleanupObjects] passing its own table proves the loop works, not that
// the path without a transaction reaches it -- and that path is exactly where
// the retry was missing. The two rows are the same objects against the same
// server, differing only in the capability the cleanup carries.
func TestDropObjects_RetriesWhereTheTransactionWasGivenUp(t *testing.T) {
	objects := []postgresCleanupObject{
		{Kind: "table", Name: "dfp", Statement: "DROP TABLE dfp"},
		{Kind: "index", Name: "dfp_uq", Statement: "DROP INDEX dfp_uq"},
	}
	rows := []struct {
		name string
		caps postgresCleanupCapabilities
		// wantErr is empty where the cleanup must finish.
		wantErr string
		// wantExecuted counts the statements issued, so a row that finished
		// says how: three means the table was attempted again.
		wantExecuted int
	}{
		{
			name:         "a server that refuses DDL in a transaction still retries",
			caps:         postgresCleanupCapabilities{}.withoutTransaction(),
			wantExecuted: 3,
		},
		{
			name:         "one pass stops at the first refusal",
			caps:         postgresCleanupCapabilities{},
			wantErr:      "failed to drop table dfp",
			wantExecuted: 1,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn := &blockingConn{blocked: "DROP TABLE dfp", unlocker: "DROP INDEX dfp_uq"}

			err := row.caps.dropObjects(context.Background(), conn, objects)

			c.Assert(errorText(err), qt.Contains, row.wantErr)
			c.Assert(conn.executed, qt.HasLen, row.wantExecuted)
		})
	}
}
