// Package dbanswer finds, in the error of a statement that failed, the error
// the database answered with, below the context Ptah wrapped around it.
//
// The migrator and every built-in dialect writer wrap a driver's error with
// words of their own in front of its message and, after it, the statement that
// failed. The `error` column of an Atlas-shaped revision table and the
// per-statement answer of a replayed migration both record the error without
// that context, so they read it through [Of]. One walk serves both because
// they must stop at the same wrappers: a wrapper that rewrote the text it
// wraps, such as a writer that took a secret out of the driver's message,
// unwraps to the text it rewrote, and stepping past it records the secret.
package dbanswer

import (
	"errors"
	"strings"
)

// Of follows err's chain down through the wrappers that only put context
// around the error they wrap, and returns the error the walk stops at. It
// returns nil for a nil err.
//
// A wrapper is stepped through when its message is the wrapped error's
// message unchanged, or that message with words of its own before it, and
// possibly after it. The walk stops at any other shape, because the wrapper's
// own message is then the one to show:
//
//   - a wrapper whose message does not contain the wrapped error's, such as a
//     redaction that took a secret out of the driver's text, or a provider's
//     execution error, which unwraps to a sentinel that classifies it;
//   - a wrapper whose message starts with the wrapped error's, which is how a
//     sentinel is attached in front of the cause it classifies;
//   - a branch, such as a join, which has no single error below it.
//
// The walk reads only messages and the single-error Unwrap method, so the
// error it returns still answers errors.Is and errors.As for everything below
// it.
func Of(err error) error {
	for err != nil {
		next := errors.Unwrap(err)
		if next == nil {
			return err
		}
		outer, inner := err.Error(), next.Error()
		if outer != inner && (inner == "" || strings.Index(outer, inner) <= 0) {
			return err
		}
		err = next
	}
	return nil
}
