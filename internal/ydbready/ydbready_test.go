package ydbready_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbready"
)

// errStorageNotReady is the refusal local-ydb 26.2.1.14 answers a CREATE TABLE
// with for about a second after it first answers a query.
var errStorageNotReady = errors.New("Status: GENERIC_ERROR Issues: <main>: Error: " +
	"database doesn't have storage pools at all to create tablet channels to storage pool kind")

// errTypeAnnotation is a refusal a ready server gives.
var errTypeAnnotation = errors.New("Status: SCHEME_ERROR Issues: <main>: Error: Type annotation")

// scripted answers the n-th try with answers[n], and every try after the last
// with the last answer, counting the tries.
type scripted struct {
	answers []error
	calls   int
}

func (s *scripted) try(context.Context) error {
	answer := s.answers[min(s.calls, len(s.answers)-1)]
	s.calls++
	return answer
}

// run waits through script with ctx ending after timeout, and returns the
// clock time Until spent and what it returned.
func run(script *scripted, timeout time.Duration) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	err := ydbready.Until(ctx, script.try)
	return time.Since(start), err
}

// A server that binds its storage during the wait takes the table on the try
// after its last refusal, one interval later per refusal.
func TestUntil_HappyPath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := qt.New(t)
		script := &scripted{answers: []error{errStorageNotReady, errStorageNotReady, nil}}

		elapsed, err := run(script, time.Hour)

		c.Assert(err, qt.IsNil)
		c.Assert(script.calls, qt.Equals, 3)
		c.Assert(elapsed, qt.Equals, 2*ydbready.Interval)
	})
}

// The wait is bounded, it ends with the caller's context, and it is spent on
// the one refusal a fresh server gives: any other refusal is the answer.
func TestUntil_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		answers     []error
		timeout     time.Duration
		want        error
		wantCalls   int
		wantElapsed time.Duration
	}{{
		name:        "a server that never binds its storage",
		answers:     []error{errStorageNotReady},
		timeout:     time.Hour,
		want:        errStorageNotReady,
		wantCalls:   ydbready.Retries + 1,
		wantElapsed: ydbready.Retries * ydbready.Interval,
	}, {
		name:        "a context that ends during the wait",
		answers:     []error{errStorageNotReady},
		timeout:     2*ydbready.Interval + ydbready.Interval/2,
		want:        errStorageNotReady,
		wantCalls:   3,
		wantElapsed: 2*ydbready.Interval + ydbready.Interval/2,
	}, {
		name:        "another refusal",
		answers:     []error{errTypeAnnotation},
		timeout:     time.Hour,
		want:        errTypeAnnotation,
		wantCalls:   1,
		wantElapsed: 0,
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := qt.New(t)
				script := &scripted{answers: test.answers}

				elapsed, err := run(script, test.timeout)

				c.Assert(err, qt.ErrorIs, test.want)
				c.Assert(script.calls, qt.Equals, test.wantCalls)
				c.Assert(elapsed, qt.Equals, test.wantElapsed)
			})
		})
	}
}

// The refusal is recognized by its words wherever the driver puts them.
func TestStorageNotReady_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Assert(ydbready.StorageNotReady(errStorageNotReady), qt.IsTrue)
	c.Assert(ydbready.StorageNotReady(errors.Join(errors.New("ydb: SQL execution failed"), errStorageNotReady)), qt.IsTrue)
}

// No error, and an error with other words, is not the refusal.
func TestStorageNotReady_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Assert(ydbready.StorageNotReady(nil), qt.IsFalse)
	c.Assert(ydbready.StorageNotReady(errTypeAnnotation), qt.IsFalse)
}
