package dbtest

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
)

// ErrParallelNeedsIsolation is returned when a case asks to run in parallel and
// the run cannot give it a database of its own.
//
// It is a load failure rather than a case failure: the alternative to refusing
// is running concurrent mutating cases against one database and reporting
// whatever they did to each other as test results.
var ErrParallelNeedsIsolation = errors.New("a parallel case needs its own database")

// runParallelCases runs the cases marked parallel concurrently and the rest in
// document order, and returns one result per case in document order.
//
// Execution order between the two groups is not observable. Every case in this
// mode gets a database of its own, created for it and removed afterwards, so no
// case can see what another did and nothing carries between them -- which is
// exactly the property that makes running some of them at once safe.
//
// The report is indexed by position rather than appended as results arrive, so
// a reader comparing two runs of the same file sees the same order whatever the
// machine's timing was.
func runParallelCases(
	ctx context.Context,
	kind string,
	cases []Case,
	limit int,
	provision provisionFunc,
	run caseRunner,
) (*Report, error) {
	results := make([]CaseResult, len(cases))
	filled := make([]bool, len(cases))
	errs := make([]error, len(cases))

	var wg sync.WaitGroup
	slots := make(chan struct{}, parallelSlots(limit))

	for i := range cases {
		if cases[i].Skip {
			results[i], filled[i] = skippedCaseResult(cases[i]), true
			continue
		}
		if !cases[i].Parallel {
			results[i], errs[i] = runEphemeralCase(ctx, cases[i], provision, run)
			filled[i] = errs[i] == nil
			continue
		}

		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()

			results[index], errs[index] = runEphemeralCase(ctx, cases[index], provision, run)
			filled[index] = errs[index] == nil
		}(i)
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	// A case that produced neither a result nor an error would leave a zero
	// CaseResult in the report: unnamed, not passed, not skipped. Reporting a
	// suite as complete when one of its cases never ran is the failure this
	// whole mode most easily hides, so it is checked rather than assumed.
	for i := range filled {
		if !filled[i] {
			return nil, fmt.Errorf("test case %q produced no result", cases[i].Name)
		}
	}
	return &Report{Cases: results, kind: kind}, nil
}

// parallelSlots bounds how many cases run at once.
//
// Each one provisions its own database, so the cost of a slot is a file and a
// connection rather than only a goroutine, and an unbounded fan-out over a
// large directory is how a test run exhausts file descriptors on the machine
// that was supposed to be running it.
//
// A caller may set the bound explicitly. The default reads the machine, which
// makes the concurrency a run actually achieves depend on the machine -- fine
// for a test suite, and not something an assertion about concurrency can rest
// on, which is why the option exists rather than only the default.
func parallelSlots(limit int) int {
	if limit > 0 {
		return limit
	}
	slots := runtime.GOMAXPROCS(0)
	if slots < 1 {
		return 1
	}
	return slots
}

// refuseParallelWithoutIsolation reports the first parallel case.
//
// It is called only for a run against a caller-owned database, where every case
// shares one connection. Running concurrent mutating cases there would let one
// case's DDL decide another's result, and a suite that passes because two cases
// happened not to collide is worse than one that refuses.
func refuseParallelWithoutIsolation(cases []Case) error {
	for i := range cases {
		if !cases[i].Parallel {
			continue
		}
		return fmt.Errorf(
			"%w: test case %q asks to run in parallel, but this run shares one database; "+
				"omit the database URL so each case gets its own",
			ErrParallelNeedsIsolation,
			cases[i].Name,
		)
	}
	return nil
}

// anyParallel reports whether any case asks to run in parallel.
func anyParallel(cases []Case) bool {
	for i := range cases {
		if cases[i].Parallel {
			return true
		}
	}
	return false
}
