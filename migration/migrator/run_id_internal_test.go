package migrator

// White-box testing required: a run ID is unexported state of a Migrator. A
// black-box test reads it only from the rows a run writes to a database, and
// what has to be pinned here is that two IDs taken in one clock tick differ,
// which a fixed clock makes deterministic and a live run cannot arrange.

import (
	"strconv"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestRunIDSource_HappyPath(t *testing.T) {
	c := qt.New(t)
	instant := time.Unix(1_700_000_000, 0)
	source := &runIDSource{pid: 42, now: func() time.Time { return instant }}
	nanos := strconv.FormatInt(instant.UnixNano(), 36)

	first, second := source.next(), source.next()

	c.Assert(first, qt.Equals, "42-"+nanos+"-1")
	c.Assert(second, qt.Equals, "42-"+nanos+"-2")
}

func TestNewMigrator_RunID_HappyPath(t *testing.T) {
	c := qt.New(t)
	// Migrators built back to back in one process fall in one clock tick
	// often enough that a run of them without the count repeats an ID.
	const built = 1000
	seen := make(map[string]bool, built)
	for range built {
		seen[NewMigrator(nil, nil).migrationLogRunID] = true
	}

	c.Assert(seen, qt.HasLen, built)
}
