package capabilityprobe

// White-box testing required: proven is the unexported builder every
// schema-change row is decided by, and which of its outcomes a run records is
// observable only through runPlan against a session. The linked SQLite engine
// answers in memory, so the outcomes are pinned without a server.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
)

func sqliteSession(c *qt.C) *session {
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://:memory:")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return &session{conn: conn, dialect: platform.SQLite}
}

// observe runs one schema-change experiment and returns what the run records.
func observe(c *qt.C, change schemaChange) observation {
	key := capability.AlterColumnDropNotNull
	observations, _ := runPlan(context.Background(), sqliteSession(c),
		plan{experiments: []experiment{proven(key, change)}})
	return observations[key]
}

const sqliteKeyedTable = "CREATE TABLE t (id INTEGER NOT NULL, n INTEGER NOT NULL, PRIMARY KEY (id))"

func TestProven_HappyPath(t *testing.T) {
	c := qt.New(t)
	got := observe(c, schemaChange{
		setup:  []string{sqliteKeyedTable},
		before: []check{refuses("INSERT INTO t (id, n) VALUES (1, NULL)")},
		change: []string{"ALTER TABLE t ALTER COLUMN n DROP NOT NULL"},
		after: []check{
			accepts("INSERT INTO t (id, n) VALUES (2, NULL)"),
			counts("SELECT COUNT(*) FROM t WHERE n IS NULL", 1),
		},
	})
	c.Assert(got, qt.Equals, observation{does: true})
}

func TestProven_FailurePath(t *testing.T) {
	t.Run("a refused change is false", func(t *testing.T) {
		c := qt.New(t)
		got := observe(c, schemaChange{
			setup:  []string{sqliteKeyedTable},
			change: []string{"ALTER TABLE t ALTER COLUMN n TYPE BIGINT"},
			after:  []check{accepts("INSERT INTO t (id, n) VALUES (1, 1)")},
		})
		c.Assert(got, qt.Equals, observation{does: false})
	})

	// The change the server took is not the change the key names: the index
	// is accepted, and the NOT NULL it was supposed to remove still refuses
	// the row.
	t.Run("an accepted change without its effect is false", func(t *testing.T) {
		c := qt.New(t)
		got := observe(c, schemaChange{
			setup:  []string{sqliteKeyedTable},
			change: []string{"CREATE INDEX t_n ON t (n)"},
			after:  []check{accepts("INSERT INTO t (id, n) VALUES (1, NULL)")},
		})
		c.Assert(got.does, qt.IsFalse)
		c.Assert(got.undecidable, qt.Equals, "")
		c.Assert(got.note, qt.Equals, `the change was accepted and did not take effect: expected `+
			`"INSERT INTO t (id, n) VALUES (1, NULL)" to be accepted, and it refused`)
	})

	t.Run("a count that does not match is false", func(t *testing.T) {
		c := qt.New(t)
		got := observe(c, schemaChange{
			setup:  []string{sqliteKeyedTable},
			change: []string{"CREATE INDEX t_n ON t (n)"},
			after:  []check{counts("SELECT COUNT(*) FROM t", 1)},
		})
		c.Assert(got.does, qt.IsFalse)
		c.Assert(got.note, qt.Equals, `the change was accepted and did not take effect: expected `+
			`"SELECT COUNT(*) FROM t" to answer 1, and it answered 0`)
	})

	// A control that does not hold means the evidence after the change would
	// be read against a shape the setup never produced.
	t.Run("a control that does not hold is undecidable", func(t *testing.T) {
		c := qt.New(t)
		got := observe(c, schemaChange{
			setup:  []string{sqliteKeyedTable},
			before: []check{accepts("INSERT INTO t (id, n) VALUES (1, NULL)")},
			change: []string{"ALTER TABLE t ALTER COLUMN n DROP NOT NULL"},
			after:  []check{accepts("INSERT INTO t (id, n) VALUES (2, NULL)")},
		})
		c.Assert(got.does, qt.IsFalse)
		c.Assert(got.undecidable, qt.Equals, `before the change the server did not behave as the evidence `+
			`after it assumes: expected "INSERT INTO t (id, n) VALUES (1, NULL)" to be accepted, and it refused`)
	})
}

// TestProven_AnAttemptReadsNeitherOutcome pins the check the default-removal
// row needs: MySQL refuses the row a removed default no longer fills, and a
// refusal there is as good an answer as a NULL.
func TestProven_AnAttemptReadsNeitherOutcome(t *testing.T) {
	c := qt.New(t)
	got := observe(c, schemaChange{
		setup:  []string{sqliteKeyedTable},
		change: []string{"CREATE INDEX t_n ON t (n)"},
		after: []check{
			attempts("INSERT INTO t (id, n) VALUES (1, NULL)"),
			counts("SELECT COUNT(*) FROM t", 0),
		},
	})
	c.Assert(got, qt.Equals, observation{does: true})
}
