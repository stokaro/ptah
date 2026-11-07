package conformance_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/conformance"
)

// inProcessLocks is memoryConfig with the lock check taking the lock from two
// sessions of one process.
func inProcessLocks(target conformance.Target, name string) conformance.Config {
	config := memoryConfig(target, name)
	config.Live.InProcessLocks = true
	return config
}

// resultOf returns the result of one check of a report.
func resultOf(report *conformance.Report, id string) conformance.Result {
	for _, result := range report.Results {
		if result.ID == id {
			return result
		}
	}
	return conformance.Result{}
}

// Under Live.InProcessLocks the lock check takes the lock from two sessions of
// one process. The memory provider passes in process, where every connection
// is one server, and behind a wire service whose engine one process holds at
// a time, which a session of a second process cannot open.
func TestCheck_InProcessLocks_HappyPath(t *testing.T) {
	t.Run("in process", func(t *testing.T) {
		c := qt.New(t)
		target := conformance.InProcess(newMemoryProvider(memoryDefects{}).server())

		report, err := conformance.Check(c.Context(), inProcessLocks(target, "in-process-locks"))

		c.Assert(err, qt.IsNil)
		assertConforms(c, report)
	})
	t.Run("an engine one process holds, behind a wire service", func(t *testing.T) {
		c := qt.New(t)
		target := conformance.Wire(exclusiveWireTarget(c, newMemoryProvider(memoryDefects{})))

		report, err := conformance.Check(c.Context(), inProcessLocks(target, "in-process-locks"))

		c.Assert(err, qt.IsNil)
		assertConforms(c, report)
	})
}

// The mode still requires the lock to exclude the second session, and where
// each connection is a process it requires a second process to fail to open
// the database: a provider whose database another process opens contends
// across processes and gets the default check.
func TestCheck_InProcessLocks_FailurePath(t *testing.T) {
	t.Run("a lock two sessions of one process hold", func(t *testing.T) {
		c := qt.New(t)
		target := conformance.InProcess(newMemoryProvider(memoryDefects{sharedLocks: true}).server())

		report, err := conformance.Check(c.Context(), inProcessLocks(target, "shared-locks"))

		c.Assert(err, qt.IsNil)
		c.Assert(failed(report), qt.DeepEquals, []string{"live.locks"})
		c.Assert(resultOf(report, "live.locks").Detail, qt.Matches, `(?s).*another session of the same process asks for the held lock: TryLock answered ACQUIRED.*`)
	})
	t.Run("a database a second process opens", func(t *testing.T) {
		c := qt.New(t)
		target := conformance.Wire(wireTarget(c, newMemoryProvider(memoryDefects{}), wireDefects{}))

		report, err := conformance.Check(c.Context(), inProcessLocks(target, "shared-database"))

		c.Assert(err, qt.IsNil)
		c.Assert(failed(report), qt.DeepEquals, []string{"live.locks"})
		c.Assert(resultOf(report, "live.locks").Detail, qt.Equals,
			"a second process opened the database while a session of the first held it; Live.InProcessLocks is for an engine whose database one process holds at a time")
	})
}

// Without the mode, an engine one process holds at a time fails the lock
// check: a session of a second process cannot open the database to contend
// for the lock. That is what Live.InProcessLocks exists for.
func TestCheck_ExclusiveEngineAcrossProcesses_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := conformance.Wire(exclusiveWireTarget(c, newMemoryProvider(memoryDefects{})))

	report, err := conformance.Check(c.Context(), memoryConfig(target, "exclusive"))

	c.Assert(err, qt.IsNil)
	c.Assert(failed(report), qt.DeepEquals, []string{"live.locks"})
	c.Assert(resultOf(report, "live.locks").Detail, qt.Equals,
		"a session of another process: Open refused the address: FailedPrecondition: another process holds the database")
}
