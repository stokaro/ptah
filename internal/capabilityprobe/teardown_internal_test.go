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

// A refused removal remains a failure even if a subsequent lookup could
// establish absence. Both failures are retained when the lookup also fails.
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
		"the directory /local/ptah_capprobe_00, which the scheme service would not look up",
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
		"the directory /local/ptah_capprobe_00, which the scheme service would not look up",
	})
}

// A resource pool and its classifier belong to the whole database, so the
// directory removal does not take them: the teardown drops each by name,
// the classifier before the pool it names, and confirms each gone in the
// system view that lists it. Over SQLite every statement and every read is
// refused, which is what makes each one visible here.
func TestTeardown_DropsAndLooksUpTheResourcePools(t *testing.T) {
	c := qt.New(t)
	s := ydbSessionOverSQLite(c)
	s.resourcePools = []string{"ptah_capprobe_00_rpk"}
	s.resourcePoolClassifiers = []string{"ptah_capprobe_00_rpc"}

	drops := s.dropResourcePools(context.Background())
	_, remaining := s.leftovers(context.Background(), []Attempt{{Statement: "remove the directory", Accepted: true}})

	statements := make([]string, 0, len(drops))
	for _, drop := range drops {
		statements = append(statements, drop.Statement)
	}
	c.Assert(statements, qt.DeepEquals, []string{
		"DROP RESOURCE POOL CLASSIFIER `ptah_capprobe_00_rpc`;",
		"DROP RESOURCE POOL `ptah_capprobe_00_rpk`;",
	})
	c.Assert(remaining, qt.DeepEquals, []string{
		"the directory /local/ptah_capprobe_00, which the scheme service would not look up",
		"resource pool ptah_capprobe_00_rpk, which the server would not look up",
		"resource pool classifier ptah_capprobe_00_rpc, which the server would not look up",
	})
}
