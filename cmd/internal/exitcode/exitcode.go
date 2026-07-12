// Package exitcode carries explicit process exit codes through cobra command
// execution without making every command know about os.Exit.
package exitcode

import "errors"

// Error wraps an error message with a process exit code.
type Error struct {
	code int
	err  error
}

// New returns an error that should make the CLI exit with code.
func New(code int, err error) error {
	return &Error{code: code, err: err}
}

// Error returns the wrapped error text.
func (e *Error) Error() string {
	return e.err.Error()
}

// Unwrap returns the wrapped error.
func (e *Error) Unwrap() error {
	return e.err
}

// Code returns the explicit process exit code carried by err, or fallback when
// err is not an exit-code error.
func Code(err error, fallback int) int {
	var exitErr *Error
	if errors.As(err, &exitErr) {
		return exitErr.code
	}
	return fallback
}
