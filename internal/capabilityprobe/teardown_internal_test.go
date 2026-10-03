package capabilityprobe

// White-box testing required: the YDB teardown and the read that confirms it
// run inside measure's deferred cleanup, which a run reaches only against a
// live YDB server. A session that names YDB over the linked SQLite engine
// reaches both: its schema writer cannot remove a directory, which is a
// refused removal without a server.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
)

// ydbSessionOverSQLite is a YDB session whose connection is an in-memory
// SQLite database: every read the YDB teardown makes is refused, and so is
// the directory removal.
func ydbSessionOverSQLite(c *qt.C) *session {
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://:memory:")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return &session{conn: conn, dialect: platform.YDB, database: "/local", namespace: "ptah_capprobe_00"}
}

// A refused removal leaves the directory standing whatever the table count
// says: the partition statistics list row tables only, so a directory holding
// a topic counts no table at all.
func TestLeftovers_ARefusedRemovalIsALeftover_FailurePath(t *testing.T) {
	c := qt.New(t)
	s := ydbSessionOverSQLite(c)
	removal := s.leave(context.Background(), "")

	_, remaining := s.leftovers(context.Background(), removal)

	c.Assert(removal, qt.HasLen, 1)
	c.Assert(removal[0].Accepted, qt.IsFalse)
	c.Assert(remaining, qt.DeepEquals, []string{
		"the directory /local/ptah_capprobe_00, which the teardown did not remove: " +
			"the connection's schema writer *sqlite.Writer cannot remove a directory",
		"the tables under /local/ptah_capprobe_00, which the server would not count",
	})
}

// The control: an accepted removal names no directory, and the reads still
// run, so what they find is still reported.
func TestLeftovers_AnAcceptedRemovalIsNot_HappyPath(t *testing.T) {
	c := qt.New(t)
	s := ydbSessionOverSQLite(c)
	removal := []Attempt{{Statement: "remove the directory", Accepted: true}}

	reads, remaining := s.leftovers(context.Background(), removal)

	c.Assert(reads, qt.HasLen, 1)
	c.Assert(remaining, qt.DeepEquals, []string{
		"the tables under /local/ptah_capprobe_00, which the server would not count",
	})
}
