package fanout_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/fanout"
)

// jitter sleeps a short time that varies with index without growing with it,
// so calls finish out of order.
func jitter(index int) {
	time.Sleep(time.Duration(index*7919%200) * time.Microsecond)
}

// workers returns n distinct worker numbers.
func workers(n int) []int {
	numbers := make([]int, n)
	for index := range numbers {
		numbers[index] = index
	}
	return numbers
}

func TestInOrder_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		n       int
		workers int
	}{
		{name: "more calls than workers", n: 200, workers: 8},
		{name: "fewer calls than workers", n: 3, workers: 8},
		{name: "one worker", n: 20, workers: 1},
		{name: "no call", n: 0, workers: 8},
		{name: "no call and no worker", n: 0, workers: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			want := make([]string, test.n)
			for index := range want {
				want[index] = fmt.Sprintf("result %d", index)
			}

			got, err := fanout.InOrder(t.Context(), workers(test.workers), test.n, func(_ context.Context, _, index int) (string, error) {
				jitter(index)
				return fmt.Sprintf("result %d", index), nil
			})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, want)
		})
	}
}

// Each worker serves one call at a time, every worker serves some, and no
// more calls run at once than there are workers.
func TestInOrder_HandsAWorkerToOneCallAtATime(t *testing.T) {
	c := qt.New(t)
	var mu sync.Mutex
	busy := make([]int, 4)
	peak := make([]int, 4)
	served := make([]int, 4)
	running, mostRunning := 0, 0

	_, err := fanout.InOrder(t.Context(), workers(4), 100, func(_ context.Context, worker, index int) (int, error) {
		mu.Lock()
		busy[worker]++
		peak[worker] = max(peak[worker], busy[worker])
		served[worker]++
		running++
		mostRunning = max(mostRunning, running)
		mu.Unlock()
		jitter(index)
		mu.Lock()
		busy[worker]--
		running--
		mu.Unlock()
		return 0, nil
	})

	c.Assert(err, qt.IsNil)
	c.Assert(peak, qt.DeepEquals, []int{1, 1, 1, 1})
	c.Assert(mostRunning <= 4, qt.IsTrue, qt.Commentf("%d calls at once", mostRunning))
	c.Assert(served[0] > 0 && served[1] > 0 && served[2] > 0 && served[3] > 0, qt.IsTrue, qt.Commentf("served %v", served))
}

var (
	errLow  = errors.New("index 7 failed")
	errHigh = errors.New("index 9 failed")
)

// The error is the lowest failed index's, however the calls interleave, and
// no index is handed out after a failure.
func TestInOrder_FailurePath(t *testing.T) {
	failures := map[int]error{7: errLow, 9: errHigh}
	for attempt := range 50 {
		t.Run(fmt.Sprint(attempt), func(t *testing.T) {
			c := qt.New(t)
			var calls atomic.Int64

			got, err := fanout.InOrder(t.Context(), workers(8), 1000, func(_ context.Context, _, index int) (int, error) {
				calls.Add(1)
				jitter(index)
				return index, failures[index]
			})

			c.Assert(err, qt.ErrorIs, errLow)
			c.Assert(got, qt.IsNil)
			c.Assert(calls.Load() < 1000, qt.IsTrue, qt.Commentf("%d calls", calls.Load()))
		})
	}
}

// Calls with no worker to run them on make none.
func TestInOrder_FailurePathNoWorker(t *testing.T) {
	c := qt.New(t)
	var calls atomic.Int64

	got, err := fanout.InOrder(t.Context(), []int(nil), 3, func(context.Context, int, int) (int, error) {
		calls.Add(1)
		return 0, nil
	})

	c.Assert(err, qt.ErrorIs, fanout.ErrNoWorker)
	c.Assert(got, qt.IsNil)
	c.Assert(calls.Load(), qt.Equals, int64(0))
}
