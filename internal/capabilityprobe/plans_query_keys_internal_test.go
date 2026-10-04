package capabilityprobe

// White-box testing required: answers is the unexported builder every query
// row is decided by, and which of its outcomes a run records is observable only
// through runPlan against a session. The linked SQLite engine answers in
// memory, so the outcomes are pinned without a server.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
)

// observeAnswers runs one answers experiment and returns what the run records.
func observeAnswers(c *qt.C, setup []string, checks ...check) observation {
	key := capability.ReturningClause
	observations, _ := runPlan(context.Background(), sqliteSession(c),
		plan{experiments: []experiment{answers(key, setup, checks...)}})
	return observations[key]
}

func TestAnswers_HappyPath(t *testing.T) {
	c := qt.New(t)
	got := observeAnswers(c, []string{sqliteKeyedTable, "INSERT INTO t (id, n) VALUES (1, 1)"},
		counts("INSERT INTO t (id, n) VALUES (2, 2) RETURNING id", 2),
		counts("UPDATE t SET n = 3 WHERE id = 2 RETURNING n", 3),
	)
	c.Assert(got, qt.Equals, observation{does: true})
}

func TestAnswers_FailurePath(t *testing.T) {
	t.Run("a refused statement is false", func(t *testing.T) {
		c := qt.New(t)
		got := observeAnswers(c, []string{sqliteKeyedTable},
			counts("SELECT COUNT(*) FROM t WHERE n = 1", 0),
			counts("SELECT n FROM t ORDER BY n OFFSET 1", 1),
		)
		c.Assert(got, qt.Equals, observation{does: false})
	})
	t.Run("an accepted statement that answers otherwise is false with what it answered", func(t *testing.T) {
		c := qt.New(t)
		got := observeAnswers(c, []string{sqliteKeyedTable, "INSERT INTO t (id, n) VALUES (1, 1)"},
			counts("SELECT COUNT(*) FROM t", 2),
		)
		c.Assert(got, qt.Equals, observation{does: false, note: "the statement was accepted and did not do what " +
			`the key names: expected "SELECT COUNT(*) FROM t" to answer 2, and it answered 1`})
	})
	t.Run("a refused setup decides nothing", func(t *testing.T) {
		c := qt.New(t)
		got := observeAnswers(c, []string{"CREATE NONSENSE t"}, counts("SELECT 1", 1))
		c.Assert(got.undecidable, qt.Not(qt.Equals), "")
	})
}
