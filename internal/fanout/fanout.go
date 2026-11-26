// Package fanout runs independent calls over a set of workers, one goroutine
// for each, and answers them in the order they were asked, so a caller that
// spreads its reads over several database sessions gets the result a
// sequential loop would give.
package fanout

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrNoWorker is what [InOrder] returns when it has calls to make and no
// worker to make them on.
var ErrNoWorker = errors.New("fanout: no worker to run the calls on")

// InOrder calls do for every index from 0 to n-1 and returns the results by
// index. Each worker gets a goroutine of its own and is passed to one call at a
// time, so a worker that serves one caller at a time, such as a database
// session, is safe to pass. At most n workers are used; with none, InOrder
// returns [ErrNoWorker] unless n is 0.
//
// Indexes are handed out in ascending order, and none is handed out after a
// call has failed. Every call already running is let finish, so each index
// below a failed one has run, and the error returned is the one of the lowest
// failed index: the error a loop that stopped at its first failure returns.
// On error the results are nil. ctx is passed to each call unchanged; a
// caller that wants the running calls to stop cancels it.
func InOrder[W, T any](ctx context.Context, workers []W, n int, do func(ctx context.Context, worker W, index int) (T, error)) ([]T, error) {
	if n > 0 && len(workers) == 0 {
		return nil, ErrNoWorker
	}
	results := make([]T, n)
	errs := make([]error, n)
	var next atomic.Int64
	var failed atomic.Bool
	var group sync.WaitGroup
	for _, worker := range workers[:min(len(workers), n)] {
		group.Go(func() {
			for !failed.Load() {
				index := int(next.Add(1) - 1)
				if index >= n {
					return
				}
				results[index], errs[index] = do(ctx, worker, index)
				if errs[index] != nil {
					failed.Store(true)
				}
			}
		})
	}
	group.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}
