//go:build integration

package dblock_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
)

// connectOracle opens a connection to the Oracle test account and closes it
// when the test ends.
func connectOracle(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.Oracle))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// freshLockName is a lock name no run has taken, so its first acquisition
// inserts its row. The lock table exists already: a lock taken and given back
// once creates it.
func freshLockName(c *qt.C, conn *dbschema.DatabaseConnection, purpose string) string {
	c.Helper()
	lock, err := dblock.Acquire(c.Context(), conn, "ptah_dblock_oracle_table", 5*time.Second)
	c.Assert(err, qt.IsNil)
	c.Assert(lock.Release(c.Context()), qt.IsNil)
	return fmt.Sprintf("ptah_dblock_oracle_%s_%d", purpose, time.Now().UnixNano())
}

// errorText is err's message, or "" for no error.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Two first acquisitions of one name under NoWait, started together, take the
// lock once: the second INSERT of the row waits for the first one's commit and
// finds the name taken, and whichever asks for the row's lock second is
// refused.
func TestConcurrentFirstNoWaitAcquisitionsOracleLive(t *testing.T) {
	c := qt.New(t)
	first := connectOracle(c)
	second := connectOracle(c)
	name := freshLockName(c, first, "pair")
	waitCtx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	c.Cleanup(cancel)
	errs := make([]error, 2)
	var start, wg sync.WaitGroup
	start.Add(1)
	for i, conn := range []*dbschema.DatabaseConnection{first, second} {
		wg.Go(func() {
			start.Wait()
			lock, err := dblock.Acquire(waitCtx, conn, name, dblock.NoWait)
			errs[i] = err
			c.Cleanup(func() { _ = lock.Release(context.Background()) })
		})
	}

	start.Done()
	wg.Wait()

	texts := []string{errorText(errs[0]), errorText(errs[1])}
	slices.Sort(texts)
	c.Assert(texts, qt.DeepEquals, []string{"", fmt.Sprintf(`advisory lock %q on oracle is held by another session`, name)})
}
